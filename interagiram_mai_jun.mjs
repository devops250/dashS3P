// Recomputa "interagiram" cruzando NocoDB (base disparada) com Chatwoot (resposta real)
// Janela: 01/05/2026 a 10/06/2026
// Tabelas: Disparo mai26 + Disparo Jun26

import { writeFileSync } from "fs";

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const TABLES = [
  { id: "mhsizs8nybch0be", title: "Disparo mai26" },
  { id: "mp1noylkq6er2jy", title: "Disparo Jun26" },
];

const CW_BASE = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT = 1;

const PERIODO_INICIO = new Date("2026-05-01T00:00:00-03:00");
const PERIODO_FIM    = new Date("2026-06-10T23:59:59-03:00");

const SKIP_INBOX = new Set([16]); // Handoff Vendedores - interno

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}

async function fetchNocoAll(tableId) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = `${NOCO_BASE}/api/v1/db/data/noco/${NOCO_PROJECT}/${tableId}?limit=200&offset=${offset}`;
    const r = await fetch(url, { headers: { "xc-token": NOCO_TOKEN } });
    const d = await r.json();
    const list = d.list || [];
    all.push(...list);
    if (list.length < 200) break;
    offset += 200;
    if (offset > 100000) break;
  }
  return all;
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
    if (page > 200) break;
  }
  return conversations;
}

async function fetchMessages(convId) {
  try {
    const d = await cwGet(`conversations/${convId}/messages`);
    return d?.payload || [];
  } catch {
    return [];
  }
}

async function listInboxes() {
  const d = await cwGet("inboxes");
  return d.payload || [];
}

console.log("=".repeat(70));
console.log("INTERAGIRAM MAI/JUN — cruzamento NocoDB x Chatwoot");
console.log("Período: 01/05/2026 a 10/06/2026");
console.log("=".repeat(70));

console.log("\n[1/4] Buscando bases disparadas (mai26 + Jun26)...");
const phonesBase = new Map(); // tel -> { nome, origem }
const totalPorTabela = {};
for (const t of TABLES) {
  const rows = await fetchNocoAll(t.id);
  let disparados = 0;
  for (const r of rows) {
    if (r.Disparo !== true) continue;
    const tel = normalizePhone(r.telefone);
    if (!tel) continue;
    disparados++;
    if (!phonesBase.has(tel)) {
      phonesBase.set(tel, { nome: r.nome || "", origem: t.title });
    }
  }
  totalPorTabela[t.title] = disparados;
  console.log(`  ${t.title}: ${disparados} disparados`);
}
console.log(`  Total telefones únicos disparados: ${phonesBase.size}`);

console.log("\n[2/4] Listando inboxes Chatwoot...");
const inboxes = (await listInboxes()).filter(i => !SKIP_INBOX.has(i.id));
console.log(`  Inboxes (sem Handoff interno): ${inboxes.length}`);

console.log("\n[3/4] Buscando conversas com atividade no período...");
const allConvs = [];
for (const ib of inboxes) {
  process.stdout.write(`  Inbox ${ib.id} ${ib.name}...`);
  try {
    const cs = await fetchInboxConversations(ib.id);
    const inPer = cs.filter(c => {
      const created = new Date((c.created_at || 0) * 1000);
      const last = new Date((c.last_activity_at || 0) * 1000);
      return (created >= PERIODO_INICIO && created <= PERIODO_FIM) ||
             (last >= PERIODO_INICIO && last <= PERIODO_FIM);
    });
    inPer.forEach(c => { c.__inbox_id = ib.id; c.__inbox_name = ib.name; });
    allConvs.push(...inPer);
    console.log(` ${cs.length}/${inPer.length}`);
  } catch (e) {
    console.log(` ERRO ${e.message}`);
  }
}
console.log(`  Total conversas no período: ${allConvs.length}`);

console.log("\n[4/4] Verificando incoming dos disparados...");
const respondentes = new Map(); // tel -> { primeira, qtd_msgs, conv_id, inbox, assignee }
let i = 0;
for (const c of allConvs) {
  i++;
  if (i % 100 === 0) process.stdout.write(`  ${i}/${allConvs.length}\n`);
  const tel = normalizePhone(c.meta?.sender?.phone_number || "");
  if (!phonesBase.has(tel)) continue;
  const msgs = await fetchMessages(c.id);
  const incoming = msgs.filter(m => {
    if (m.message_type !== 0) return false;
    const ts = new Date((m.created_at || 0) * 1000);
    return ts >= PERIODO_INICIO && ts <= PERIODO_FIM;
  });
  if (!incoming.length) continue;
  const existing = respondentes.get(tel);
  if (!existing || new Date(incoming[0].created_at * 1000) < new Date(existing.primeira)) {
    respondentes.set(tel, {
      primeira: new Date(incoming[0].created_at * 1000).toISOString(),
      qtd_msgs: incoming.length,
      conv_id: c.id,
      inbox: c.__inbox_name,
      assignee: c.meta?.assignee?.name || null,
      nome: c.meta?.sender?.name || phonesBase.get(tel).nome,
      origem: phonesBase.get(tel).origem,
    });
  }
}

const totalInteragiram = respondentes.size;

// Quebrar por origem
const porOrigem = { "Disparo mai26": 0, "Disparo Jun26": 0 };
for (const r of respondentes.values()) {
  porOrigem[r.origem] = (porOrigem[r.origem] || 0) + 1;
}

const out = {
  geradoEm: new Date().toISOString(),
  periodo: { inicio: "2026-05-01", fim: "2026-06-10" },
  fonte: "NocoDB (base disparada) x Chatwoot (incoming) - dedup por telefone",
  totais: {
    leads_disparados_unicos: phonesBase.size,
    interagiram_unicos: totalInteragiram,
    taxa_interacao_pct: phonesBase.size ? +(totalInteragiram/phonesBase.size*100).toFixed(2) : 0,
  },
  por_origem: porOrigem,
  por_tabela_disparados: totalPorTabela,
  detalhes_respondentes: Array.from(respondentes.entries())
    .map(([tel, r]) => ({ telefone: tel, ...r }))
    .sort((a, b) => a.primeira.localeCompare(b.primeira)),
};

writeFileSync("interagiram_mai_jun.json", JSON.stringify(out, null, 2));

console.log("\n" + "=".repeat(70));
console.log("RESULTADO");
console.log("=".repeat(70));
console.log(JSON.stringify(out.totais, null, 2));
console.log("\nPor origem:");
for (const [o, n] of Object.entries(porOrigem)) console.log(`  ${o.padEnd(20)} ${n}`);
console.log("\nArquivo: interagiram_mai_jun.json");
