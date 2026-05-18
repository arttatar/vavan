// sync-to-ghl Edge Function
//
// Outbound side of two-way sync: pushes Vavan Maps / Sales App edits into the
// underlying CRM (Vavan CRM = GHL) so reps see their changes there too.
//
// Called by the frontend (`_syncToCRM`, `_syncNoteToCRM`) right after a
// successful Supabase write. Fire-and-forget on the frontend side.
//
// POST body shapes:
//   { business_id: uuid, action: 'sync' }
//      → PATCH contact in GHL (or POST if no ghl_contact_id yet)
//   { business_id: uuid, action: 'note', note: text, type?: text, created_by?: text }
//      → POST a note on that contact in GHL
//
// Loop prevention: every successful push sets
//   businesses.last_sync_origin = 'supabase_push'
// so the inbound ghl-webhook can detect "this was our own change bouncing
// back" and skip re-writing. Conversely, if we read a row whose
// last_sync_origin is 'ghl_webhook' AND last_sync_at is within 5 seconds,
// we treat it as the same change we just received and SKIP pushing
// (otherwise the rep would never see their CRM-side change settle).

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SERVICE_KEY  = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const sb = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

const GHL_BASE = 'https://services.leadconnectorhq.com';

interface Business {
  id: string;
  name: string | null;
  street: string | null;
  locality: string | null;       // city
  state: string | null;
  zip: string | null;
  phone: string | null;
  website: string | null;
  branch_code: string | null;
  ghl_contact_id: string | null;
  ghl_location_id: string | null;
  account_number: string | null;
  customer_health: string | null;
  customer_tier: string | null;
  pipeline_stage: string | null;
  status: string | null;
  tags: string[] | null;
  last_sync_origin: string | null;
  last_sync_at: string | null;
  is_competitor: boolean | null;
}

Deno.serve(async (req: Request): Promise<Response> => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });
  if (req.method !== 'POST')   return json({ error: 'POST only' }, 405);

  const t0 = Date.now();

  // Auth: verify caller has a valid JWT (any authenticated user can trigger
  // this for businesses they have RLS access to)
  const authHeader = req.headers.get('Authorization');
  if (!authHeader) return json({ error: 'missing auth' }, 401);

  let body: { business_id?: string; action?: string; note?: string; type?: string; created_by?: string };
  try { body = await req.json(); } catch { return json({ error: 'invalid json' }, 400); }
  const businessId = body.business_id;
  const action = (body.action || 'sync').toLowerCase();
  if (!businessId) return json({ error: 'business_id required' }, 400);

  // 1. Load the business row
  const { data: biz, error: bizErr } = await sb.from('businesses')
    .select('id, name, street, locality, state, zip, phone, website, branch_code, ghl_contact_id, ghl_location_id, account_number, customer_health, customer_tier, pipeline_stage, status, tags, last_sync_origin, last_sync_at, is_competitor')
    .eq('id', businessId).single();
  if (bizErr || !biz) {
    await logEvent({ direction:'outbound', status:'error', error:'biz not found',
                     business_id:businessId, processed_ms: Date.now()-t0 });
    return json({ error: 'business not found' }, 404);
  }
  const b = biz as unknown as Business;

  // 2. Echo guard: if the last write was from the inbound webhook within the
  // last 5 seconds, we'd be pushing our own change back. Skip.
  if (b.last_sync_origin === 'ghl_webhook' && b.last_sync_at) {
    const age = Date.now() - new Date(b.last_sync_at).getTime();
    if (age < 5000) {
      await logEvent({ direction:'outbound', status:'ignored', error:'echo guard',
                       business_id:businessId, branch_code:b.branch_code,
                       ghl_contact_id:b.ghl_contact_id, processed_ms: Date.now()-t0 });
      return json({ ok: true, status: 'skip_echo' }, 200);
    }
  }

  // 3. Resolve credentials for the branch
  if (!b.branch_code) {
    await logEvent({ direction:'outbound', status:'error', error:'no branch_code',
                     business_id:businessId, processed_ms: Date.now()-t0 });
    return json({ error: 'business has no branch_code' }, 400);
  }
  const { data: cred } = await sb.from('ghl_webhook_secrets')
    .select('location_id, pit').eq('branch_code', b.branch_code).single();
  if (!cred?.pit) {
    await logEvent({ direction:'outbound', status:'error', error:'no PIT for branch',
                     business_id:businessId, branch_code:b.branch_code,
                     processed_ms: Date.now()-t0 });
    return json({ error: 'no credentials for branch' }, 500);
  }
  const pit = cred.pit;
  const locationId = b.ghl_location_id || cred.location_id;

  // 4. Branch on action
  try {
    if (action === 'note') {
      if (!b.ghl_contact_id) {
        return json({ ok: false, status: 'no_contact_id', error: 'business not yet pushed to CRM' }, 200);
      }
      const note = (body.note || '').trim();
      if (!note) return json({ error: 'note text required' }, 400);
      const payload = { body: note };
      const r = await ghlFetch(`${GHL_BASE}/contacts/${b.ghl_contact_id}/notes`, pit, 'POST', payload);
      const status = r.ok ? 'ok' : 'error';
      await logEvent({ direction:'outbound', event_type:'NoteCreate', status,
                       business_id:businessId, branch_code:b.branch_code,
                       ghl_contact_id:b.ghl_contact_id, location_id:locationId,
                       payload:{ note: note.slice(0, 500), type: body.type, created_by: body.created_by },
                       error: r.ok ? null : (r.errorBody?.slice(0,300) || `http_${r.status}`),
                       processed_ms: Date.now()-t0 });
      return json({ ok: r.ok, status: r.ok ? 'note_created' : 'note_failed' }, 200);
    }

    // action === 'sync' — push contact fields
    const payload = mapBusinessToContact(b, locationId);
    let ghlContactId = b.ghl_contact_id;
    let r;
    if (ghlContactId) {
      // PUT /contacts/:id does NOT accept locationId in body — strip it
      const { locationId: _strip, ...putPayload } = payload as Record<string, unknown> & { locationId?: unknown };
      r = await ghlFetch(`${GHL_BASE}/contacts/${ghlContactId}`, pit, 'PUT', putPayload);
    } else {
      r = await ghlFetch(`${GHL_BASE}/contacts/`, pit, 'POST', payload);
      if (r.ok && r.data) {
        ghlContactId = (r.data.contact?.id || r.data.id) as string | null;
      } else if (r.status === 400 && r.errorBody?.toLowerCase().includes('duplicat')) {
        // GHL dedupe by phone/email — extract existing id
        try {
          const errObj = JSON.parse(r.errorBody);
          ghlContactId = errObj?.meta?.contactId || null;
          if (ghlContactId) r = { ok: true, status: 200, data: { contact: { id: ghlContactId } } };
        } catch { /* fall through */ }
      }
    }

    if (!r.ok) {
      await logEvent({ direction:'outbound', event_type: b.ghl_contact_id ? 'ContactUpdate' : 'ContactCreate',
                       status:'error', business_id:businessId, branch_code:b.branch_code,
                       ghl_contact_id:b.ghl_contact_id, location_id:locationId,
                       error: r.errorBody?.slice(0,300) || `http_${r.status}`,
                       processed_ms: Date.now()-t0 });
      return json({ ok:false, status:'sync_failed', error: r.errorBody?.slice(0,200) }, 200);
    }

    // 5. Update Supabase: record new id (if just created) + mark origin so the
    // inbound webhook can detect and skip the echo.
    const update: Record<string, unknown> = {
      last_sync_at: new Date().toISOString(),
      last_sync_origin: 'supabase_push',
    };
    if (!b.ghl_contact_id && ghlContactId) update.ghl_contact_id = ghlContactId;
    await sb.from('businesses').update(update).eq('id', businessId);

    await logEvent({ direction:'outbound',
                     event_type: b.ghl_contact_id ? 'ContactUpdate' : 'ContactCreate',
                     status:'ok', business_id:businessId, branch_code:b.branch_code,
                     ghl_contact_id: ghlContactId, location_id:locationId,
                     processed_ms: Date.now()-t0 });
    return json({ ok: true, status: b.ghl_contact_id ? 'updated' : 'created',
                  ghl_contact_id: ghlContactId }, 200);
  } catch (e) {
    const msg = (e as Error).message?.slice(0, 500);
    await logEvent({ direction:'outbound', status:'error', error:msg,
                     business_id:businessId, branch_code:b.branch_code,
                     ghl_contact_id:b.ghl_contact_id, processed_ms: Date.now()-t0 });
    return json({ ok:false, status:'exception', error: msg }, 200);
  }
});

// ---------------------------------------------------------------------------
function mapBusinessToContact(b: Business, locationId: string) {
  const phone = normalizePhone(b.phone);
  const tags = Array.isArray(b.tags) ? b.tags.filter((t) => typeof t === 'string') : [];

  const payload: Record<string, unknown> = {
    locationId,
    type: b.is_competitor ? 'lead' : 'lead',  // GHL only allows 'lead' / 'customer'
    name: (b.name || '').slice(0, 200),
    companyName: (b.name || '').slice(0, 200),
    firstName: (b.name || '').slice(0, 50),
    address1: b.street || '',
    city: b.locality || '',
    state: b.state || '',
    postalCode: (b.zip || '').slice(0, 5),
    country: 'US',
    tags,
    website: b.website || '',
  };
  if (phone) payload.phone = phone;
  return payload;
}

function normalizePhone(p: string | null): string | null {
  if (!p) return null;
  const d = p.replace(/[^0-9]/g, '');
  if (d.length === 10) return '+1' + d;
  if (d.length === 11 && d.startsWith('1')) return '+' + d;
  return null;
}

interface GhlResp { ok: boolean; status: number; data?: { contact?: { id?: string }; id?: string }; errorBody?: string; }
async function ghlFetch(url: string, pit: string, method: string, body: unknown): Promise<GhlResp> {
  const r = await fetch(url, {
    method,
    headers: {
      'Authorization': `Bearer ${pit}`,
      'Version': '2021-07-28',
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'curl/8.7.1',  // GHL Cloudflare blocks default Deno UA
    },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  let data;
  try { data = text ? JSON.parse(text) : undefined; } catch { /* not json */ }
  return { ok: r.ok, status: r.status, data, errorBody: r.ok ? undefined : text };
}

async function logEvent(row: Record<string, unknown>) {
  try { await sb.from('ghl_sync_log').insert(row); } catch (_) { /* swallow */ }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}
