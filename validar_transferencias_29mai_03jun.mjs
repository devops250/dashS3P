// Valida fidedignidade das 3 transferências reportadas
// + investiga as 7 conversas do inbox Handoff Vendedores no período

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE = "mhsizs8nybch0be";

const CW_BASE = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT = 1;

const PERIODO_INICIO = new Date("2026-05-29T00:00:00-03:00");
const PERIODO_FIM    = new Date("2026-06-03T23:59:59-03:00");

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}

async function nocoFind(telefone) {
  const url = `${NOCO_BASE}/api/v1/db/data/noco/${NOCO_PROJECT}/${NOCO_TABLE}?where=(telefone,eq,${telefone})&limit=5`;
  const r = await fetch(url, { headers: { "xc-token": NOCO_TOKEN } });
  const d = await r.json();
  return d.list || [];
}

async function cwGet(endpoint, params = {}) {
  const url = new URL(`/api/v1/accounts/${CW_ACCOUNT}/${endpoint}`, CW_BASE);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const r = await fetch(url.toString(), { headers: { api_access_token: CW_TOKEN } });
  if (!r.ok) throw new Error(`CW ${r.status} ${endpoint}`);
  return r.json();
}

async function fetchInboxConversations(inboxId) {
  const conversations = [];
  let page = 1;
  while (true) {
    const data = await cwGet("conversations", { page, inbox_id: inboxId, status: "all" });
    const payload = data?.data?.payload || [];
    if (!payload.length) break;
    conversations.push(...payload);
    const total = data?.data?.meta?.all_count || 0;
    if (conversations.length >= total) break;
    page++;
    if (page > 100) break;
  }
  return conversations;
}

async function fetchMessages(convId) {
  const d = await cwGet(`conversations/${convId}/messages`);
  return d?.payload || [];
}

const transferidos = [
  { tel: "5569992949915", nome: "MARIA MERCEDES GAVIOLI", esperado: "Douglas" },
  { tel: "5564992949506", nome: "ESLEI CASTILHO URBANO", esperado: "Ronaldo" },
  { tel: "556596928280",  nome: "Abitio",                 esperado: "Maria Luiza" },
];

console.log("=".repeat(70));
console.log("VALIDAÇÃO TRANSFERÊNCIAS - 29/05 a 03/06");
console.log("=".repeat(70));

console.log("\n[A] Inspeção NocoDB - data_transferencia e Hora Mensagem");
for (const t of transferidos) {
  const recs = await nocoFind(t.tel);
  if (!recs.length) { console.log(`  ${t.nome}: SEM REGISTRO`); continue; }
  for (const r of recs) {
    console.log(`\n  ${t.nome} (+${t.tel})`);
    console.log(`    vendedor_responsavel : ${r.vendedor_responsavel}`);
    console.log(`    data_transferencia   : ${r.data_transferencia}`);
    console.log(`    status               : ${r.status}`);
    console.log(`    Hora Mensagem        : ${r["Hora Mensagem"]}`);
    console.log(`    CreatedAt            : ${r.CreatedAt}`);
    console.log(`    Disparo              : ${r.Disparo}`);
    const dt = r.data_transferencia ? new Date(r.data_transferencia) : null;
    const noPer = dt && dt >= PERIODO_INICIO && dt <= PERIODO_FIM;
    console.log(`    >> transferida NO período (29mai-03jun)? ${noPer ? "SIM" : "NÃO"} ${dt ? `(${dt.toISOString()})` : ""}`);
  }
}

console.log("\n\n[B] Inbox Handoff Vendedores (id 16) - 7 conversas no período");
const handoff = await fetchInboxConversations(16);
const noPer = handoff.filter(c => {
  const last = new Date((c.last_activity_at || 0) * 1000);
  const created = new Date((c.created_at || 0) * 1000);
  return (created >= PERIODO_INICIO && created <= PERIODO_FIM) ||
         (last >= PERIODO_INICIO && last <= PERIODO_FIM);
});
console.log(`  Total no período: ${noPer.length}`);
for (const c of noPer) {
  const tel = normalizePhone(c.meta?.sender?.phone_number || "");
  const created = new Date((c.created_at || 0) * 1000);
  console.log(`\n  conv ${c.id} | ${c.meta?.sender?.name} | +${tel}`);
  console.log(`    criada: ${created.toISOString()}`);
  console.log(`    assignee: ${c.meta?.assignee?.name || "-"}`);
  console.log(`    status: ${c.status}`);
  // Buscar registro NocoDB
  const recs = await nocoFind(tel);
  if (recs.length) {
    const r = recs[0];
    console.log(`    NocoDB vendedor: ${r.vendedor_responsavel || "-"}`);
    console.log(`    NocoDB data_transf: ${r.data_transferencia || "-"}`);
    console.log(`    NocoDB Hora Mensagem: ${r["Hora Mensagem"] || "-"}`);
  } else {
    console.log(`    NocoDB: SEM REGISTRO (fora do Disparo mai25)`);
  }
}
