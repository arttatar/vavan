// ghl-webhook Edge Function — inbound side of two-way sync.
// Handles: Contact*, Note*, Opportunity* events from GHL.

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';

const SUPABASE_URL = Deno.env.get('SUPABASE_URL')!;
const SERVICE_KEY  = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'content-type, authorization, x-ghl-signature',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

const sb = createClient(SUPABASE_URL, SERVICE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

interface GhlContact {
  id?: string; locationId?: string; firstName?: string; lastName?: string;
  name?: string; companyName?: string; email?: string; phone?: string;
  address1?: string; city?: string; state?: string; postalCode?: string;
  country?: string; website?: string; type?: string; source?: string;
  tags?: string[]; customFields?: Array<{ id: string; value: unknown }>;
  dateAdded?: string; dateUpdated?: string;
}

interface GhlEvent {
  type?: string; event?: string; locationId?: string;
  contactId?: string; contact?: GhlContact;
  noteId?: string; note?: { id?: string; body?: string; userId?: string; contactId?: string };
  opportunityId?: string; opportunity?: {
    id?: string; name?: string; status?: string; monetaryValue?: number;
    pipelineId?: string; pipelineStageId?: string; contactId?: string;
    assignedTo?: string;
  };
  id?: string; [k: string]: unknown;
}

Deno.serve(async (req: Request): Promise<Response> => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });
  if (req.method !== 'POST')   return json({ error: 'POST only' }, 405);

  const t0 = Date.now();
  const url = new URL(req.url);
  const token = url.searchParams.get('token') || req.headers.get('x-ghl-token') || '';

  let raw: unknown;
  try { raw = await req.json(); } catch { return json({ error: 'invalid json' }, 400); }
  const event = raw as GhlEvent;

  const locationId = event.locationId ||
    event.contact?.locationId ||
    (event as { location?: { id?: string } }).location?.id || null;

  if (!locationId) {
    await logEvent({ direction:'inbound', status:'error', error:'missing locationId',
                     payload:event as Record<string,unknown>, processed_ms: Date.now()-t0 });
    return json({ error: 'missing locationId' }, 400);
  }

  const { data: secretRow } = await sb.from('ghl_webhook_secrets')
    .select('secret, branch_code').eq('location_id', locationId).single();
  if (!secretRow) {
    await logEvent({ direction:'inbound', location_id:locationId, status:'error',
                     error:'unknown locationId', payload:event as Record<string,unknown>,
                     processed_ms: Date.now()-t0 });
    return json({ error: 'unknown locationId' }, 403);
  }
  if (token !== secretRow.secret) {
    await logEvent({ direction:'inbound', location_id:locationId, branch_code:secretRow.branch_code,
                     status:'error', error:'bad token', processed_ms: Date.now()-t0 });
    return json({ error: 'forbidden' }, 403);
  }
  const branchCode = secretRow.branch_code;

  const eventType = (event.type || event.event || '').trim() || 'unknown';
  const lower = eventType.toLowerCase();

  // Route to handler based on event prefix
  let status = 'ignored';
  let errMsg: string | undefined;
  let businessId: string | null = null;
  let ghlContactId: string | null = null;

  try {
    if (lower.includes('note')) {
      ({ status, errMsg, businessId, ghlContactId } =
        await handleNote(event, branchCode, locationId, lower));
    } else if (lower.includes('opportunity') || lower.includes('opp')) {
      ({ status, errMsg, businessId, ghlContactId } =
        await handleOpportunity(event, branchCode, locationId, lower));
    } else if (lower.includes('contact')) {
      ({ status, errMsg, businessId, ghlContactId } =
        await handleContact(event, branchCode, locationId, lower));
    }
  } catch (e) {
    status = 'error';
    errMsg = (e as Error).message?.slice(0, 500);
  }

  await logEvent({
    direction:'inbound', event_type:eventType,
    location_id:locationId, branch_code:branchCode,
    ghl_contact_id:ghlContactId, business_id:businessId,
    status, payload: event as Record<string,unknown>,
    error: errMsg, processed_ms: Date.now() - t0,
  });

  return json({ ok: status === 'ok' || status === 'ignored', status, business_id: businessId }, 200);
});

// ─── Contact handler (unchanged) ────────────────────────────────────────────
async function handleContact(event: GhlEvent, branchCode: string, locationId: string, lower: string)
  : Promise<{ status:string; errMsg?:string; businessId:string|null; ghlContactId:string|null }> {
  const contact = event.contact || (event.id ? event as unknown as GhlContact : null);
  const ghlContactId = event.contactId || contact?.id || null;
  let businessId: string | null = null;

  if (lower.includes('delete') && ghlContactId) {
    const { data, error } = await sb.from('businesses')
      .update({
        is_abandoned: true, ghl_contact_id: null,
        last_sync_at: new Date().toISOString(), last_sync_origin: 'ghl_webhook',
        updated_at: new Date().toISOString(),
      })
      .eq('ghl_contact_id', ghlContactId).select('id').maybeSingle();
    if (error) throw error;
    return { status: 'ok', businessId: data?.id || null, ghlContactId };
  }

  if (contact && ghlContactId) {
    const update = mapContactToBusiness(contact, branchCode, locationId);
    const { data: existing } = await sb.from('businesses')
      .select('id').eq('ghl_contact_id', ghlContactId).maybeSingle();
    if (existing) {
      const { error } = await sb.from('businesses').update(update).eq('id', existing.id);
      if (error) throw error;
      businessId = existing.id;
    } else {
      const insertRow = { ...update, ghl_contact_id: ghlContactId };
      const { data, error } = await sb.from('businesses').insert(insertRow).select('id').single();
      if (error) throw error;
      businessId = data?.id || null;
    }
    return { status: 'ok', businessId, ghlContactId };
  }

  return { status: 'ignored', businessId: null, ghlContactId };
}

// ─── Note handler ──────────────────────────────────────────────────────────
async function handleNote(event: GhlEvent, branchCode: string, locationId: string, lower: string)
  : Promise<{ status:string; errMsg?:string; businessId:string|null; ghlContactId:string|null }> {
  const note = event.note || (event as { id?: string; body?: string; userId?: string; contactId?: string });
  const noteId = event.noteId || note?.id || null;
  const ghlContactId = note?.contactId || event.contactId || null;
  const body = note?.body || null;
  const createdBy = note?.userId || null;

  if (!noteId) return { status: 'ignored', businessId: null, ghlContactId };

  // Find business by ghl_contact_id
  let businessId: string | null = null;
  if (ghlContactId) {
    const { data: biz } = await sb.from('businesses').select('id, client_id')
      .eq('ghl_contact_id', ghlContactId).maybeSingle();
    if (biz) businessId = biz.id;
  }

  if (lower.includes('delete')) {
    await sb.from('activity').delete().eq('ghl_source_id', noteId);
    return { status: 'ok', businessId, ghlContactId };
  }

  // Upsert by ghl_source_id
  const row = {
    business_id: businessId,
    type: 'note',
    note: body || '',
    created_by: createdBy ? `ghl_user:${createdBy}` : 'ghl_webhook',
    ghl_source_id: noteId,
    ghl_location_id: locationId,
    branch_code: branchCode,
    ghl_synced_at: new Date().toISOString(),
    last_sync_at: new Date().toISOString(),
    last_sync_origin: 'ghl_webhook',
  };
  const { error } = await sb.from('activity').upsert(row, { onConflict: 'ghl_source_id' });
  if (error) throw error;
  return { status: 'ok', businessId, ghlContactId };
}

// ─── Opportunity handler ───────────────────────────────────────────────────
async function handleOpportunity(event: GhlEvent, branchCode: string, locationId: string, lower: string)
  : Promise<{ status:string; errMsg?:string; businessId:string|null; ghlContactId:string|null }> {
  const opp = event.opportunity || (event as unknown as { id?:string });
  const oppId = event.opportunityId || opp?.id || null;
  const ghlContactId = (opp as { contactId?: string })?.contactId || event.contactId || null;

  if (!oppId) return { status: 'ignored', businessId: null, ghlContactId };

  let businessId: string | null = null;
  let clientId: string | null = null;
  if (ghlContactId) {
    const { data: biz } = await sb.from('businesses').select('id, client_id')
      .eq('ghl_contact_id', ghlContactId).maybeSingle();
    if (biz) { businessId = biz.id; clientId = biz.client_id; }
  }

  if (lower.includes('delete')) {
    await sb.from('business_opportunities').delete().eq('ghl_opportunity_id', oppId);
    return { status: 'ok', businessId, ghlContactId };
  }

  const o = opp as { name?:string; status?:string; monetaryValue?:number; assignedTo?:string };
  const row = {
    ghl_opportunity_id: oppId,
    ghl_location_id: locationId,
    branch_code: branchCode,
    business_id: businessId,
    client_id: clientId,
    ghl_name: o.name || null,
    ghl_status: o.status || null,
    ghl_monetary_value: o.monetaryValue || null,
    assigned_to: o.assignedTo || null,
    ghl_synced_at: new Date().toISOString(),
    last_sync_at: new Date().toISOString(),
    last_sync_origin: 'ghl_webhook',
    updated_at: new Date().toISOString(),
  };
  const { error } = await sb.from('business_opportunities')
    .upsert(row, { onConflict: 'ghl_opportunity_id' });
  if (error) throw error;
  return { status: 'ok', businessId, ghlContactId };
}

// ─── Helpers ───────────────────────────────────────────────────────────────
function mapContactToBusiness(c: GhlContact, branchCode: string, locationId: string) {
  const name = c.companyName || c.name ||
               [c.firstName, c.lastName].filter(Boolean).join(' ').trim() ||
               '[Unknown]';
  return {
    name, street: c.address1 || null, locality: c.city || null,
    state: c.state || null, zip: c.postalCode || null,
    phone: c.phone || null, website: c.website || null,
    branch_code: branchCode, ghl_location_id: locationId, tags: c.tags || [],
    last_sync_at: new Date().toISOString(), last_sync_origin: 'ghl_webhook',
    updated_at: new Date().toISOString(),
  };
}

async function logEvent(row: Record<string, unknown>) {
  try { await sb.from('ghl_sync_log').insert(row); } catch (_) { /* swallow */ }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}
