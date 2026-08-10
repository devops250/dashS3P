// Análise últimos 30 dias: 11/jul → 10/ago/2026
// NocoDB: tabela "Disparo Jun26" (mp1noylkq6er2jy)
// Chatwoot: SOMENTE Inbox 18 — "Yara API Oficial"
import { writeFileSync } from "fs";

const NOCO_BASE    = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN   = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE   = "mp1noylkq6er2jy";
const CW_BASE      = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN     = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT   = 1;
const INBOX_ID     = 18;

const PERIODO_INICIO = new Date("2026-07-11T00:00:00-03:00");
const PERIODO_FIM    = new Date("2026-08-10T23:59:59-03:00");
const DIAS_PERIODO   = 31;

const MESES_PT = {
  janeiro:1, fevereiro:2, marco:3, "março":3, abril:4, maio:5, junho:6,
  julho:7, agosto:8, setembro:9, outubro:10, novembro:11, dezembro:12,
};

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}
function inPeriodo(d) {
  if (!d) return false;
  const t = new Date(d);
  return !isNaN(t) && t >= PERIODO_INICIO && t <= PERIODO_FIM;
}
function horaMensagemNoPeriodo(s) {
  if (!s) return false;
  const m = s.match(/(\d{1,2})\s+de\s+(\w+)\s+de\s+2026/i);
  if (!m) return false;
  const day = +m[1];
  const mes = MESES_PT[m[2].toLowerCase()];
  if (!mes) return false;
  const dt = new Date(2026, mes - 1, day, 12, 0, 0);
  return dt >= new Date(2026, 6, 11) && dt <= new Date(2026, 7, 10, 23, 59, 59);
}
function dayKey(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return null;
  const local = new Date(d.getTime() - 3 * 3600 * 1000);
  return `${local.getUTCDate().toString().padStart(2,"0")}/${(local.getUTCMonth()+1).toString().padStart(2,"0")}`;
}
function weekKey(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return null;
  const local = new Date(d.getTime() - 3 * 3600 * 1000);
  const dow = local.getUTCDay(); // 0=Sun..6=Sat
  const monday = new Date(local);
  const diff = (dow + 6) % 7;
  monday.setUTCDate(local.getUTCDate() - diff);
  return `sem-${monday.getUTCDate().toString().padStart(2,"0")}/${(monday.getUTCMonth()+1).toString().padStart(2,"0")}`;
}

async function cwGet(endpoint, params = {}) {
  const url = new URL(`/api/v1/accounts/${CW_ACCOUNT}/${endpoint}`, CW_BASE);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const r = await fetch(url.toString(), { headers: { api_access_token: CW_TOKEN } });
  if (!r.ok) throw new Error(`CW ${r.status} em ${endpoint}`);
  return r.json();
}
async function fetchNocoAll() {
  const all = []; let offset = 0;
  while (true) {
    const url = `${NOCO_BASE}/api/v1/db/data/noco/${NOCO_PROJECT}/${NOCO_TABLE}?limit=200&offset=${offset}`;
    const r = await fetch(url, { headers: { "xc-token": NOCO_TOKEN } });
    const d = await r.json();
    const list = d.list || [];
    all.push(...list);
    if (list.length < 200) break;
    offset += 200;
  }
  return all;
}
async function fetchInboxConvsPeriodo(inboxId) {
  const convs = []; let page = 1;
  while (true) {
    const d = await cwGet("conversations", { page, inbox_id: inboxId, status: "all" });
    const p = d?.data?.payload || [];
    if (!p.length) break;
    const inPer = p.filter(c => {
      const last = new Date((c.last_activity_at || 0) * 1000);
      return last >= PERIODO_INICIO && last <= PERIODO_FIM;
    });
    convs.push(...inPer);
    const ultima = p[p.length - 1];
    const ultimaTs = new Date((ultima.last_activity_at || 0) * 1000);
    if (ultimaTs < PERIODO_INICIO) break;
    const total = d?.data?.meta?.all_count || 0;
    if (page * 25 >= total) break;
    page++;
    if (page > 800) break;
  }
  return convs;
}
async function fetchMsgs(id) {
  try { const d = await cwGet(`conversations/${id}/messages`); return d?.payload || []; }
  catch { return []; }
}

console.log("=".repeat(60));
console.log(`Análise: 11/jul → 10/ago/2026 (últimos 30 dias)`);
console.log(`NocoDB: Disparo Jun26 | Chatwoot: Inbox 18 (Yara API Oficial)`);
console.log("=".repeat(60));

console.log("\n[1/3] Baixando NocoDB...");
const nocoAll = await fetchNocoAll();
console.log(`  ${nocoAll.length} registros totais`);

const contatados = nocoAll.filter(r => r.Disparo && (horaMensagemNoPeriodo(r["Hora Mensagem"]) || inPeriodo(r.CreatedAt)));
const transferidos = nocoAll.filter(r => inPeriodo(r.data_transferencia));
const telSet = new Set();
const transfUnicos = [];
for (const r of transferidos) {
  const tel = normalizePhone(r.telefone);
  if (!tel || telSet.has(tel)) continue;
  telSet.add(tel);
  transfUnicos.push(r);
}
console.log(`  Contatados: ${contatados.length}`);
console.log(`  Transferidos (total): ${transferidos.length}`);
console.log(`  Transferidos únicos: ${transfUnicos.length}`);

const dispDia = {};
for (const r of contatados) {
  const hm = r["Hora Mensagem"] || "";
  const m = hm.match(/(\d{1,2})\s+de\s+(\w+)\s+de\s+2026/i);
  const k = m ? `${m[1].padStart(2,"0")} ${m[2]}` : "(sem data)";
  dispDia[k] = (dispDia[k] || 0) + 1;
}
const tDia = {};
const tSem = {};
for (const r of transferidos) {
  const k = dayKey(r.data_transferencia); if (k) tDia[k] = (tDia[k] || 0) + 1;
  const w = weekKey(r.data_transferencia); if (w) tSem[w] = (tSem[w] || 0) + 1;
}
const tVend = {};
for (const r of transferidos) { const v = r.vendedor_responsavel || "Não informado"; tVend[v] = (tVend[v] || 0) + 1; }
const temp = {};
for (const r of transferidos) { const t = r.temperatura || "(vazio)"; temp[t] = (temp[t] || 0) + 1; }

console.log("\n[2/3] Buscando conversas no Inbox 18...");
const phonesContatados = new Set(contatados.map(r => normalizePhone(r.telefone)).filter(Boolean));
const responderamSet = new Set();
const respDetalhes = [];

const convs = await fetchInboxConvsPeriodo(INBOX_ID);
console.log(`  ${convs.length} conversas no período`);

console.log("\n[3/3] Analisando mensagens recebidas...");
let processadas = 0;
for (const c of convs) {
  const tel = normalizePhone(c.meta?.sender?.phone_number || "");
  if (!phonesContatados.has(tel)) { processadas++; continue; }
  const msgs = await fetchMsgs(c.id);
  const inc = msgs.filter(m => {
    if (m.message_type !== 0) return false;
    const ts = new Date((m.created_at || 0) * 1000);
    return ts >= PERIODO_INICIO && ts <= PERIODO_FIM;
  });
  if (inc.length > 0) {
    responderamSet.add(tel);
    respDetalhes.push({
      tel,
      nome: c.meta?.sender?.name,
      primeira: inc[0].content?.slice(0, 140),
      assignee: c.meta?.assignee?.name,
    });
  }
  processadas++;
  if (processadas % 50 === 0) process.stdout.write(`  ${processadas}/${convs.length}\r`);
}

const base = contatados.length;
const out = {
  periodo: { inicio: "2026-07-11", fim: "2026-08-10", dias: DIAS_PERIODO },
  fonte: { nocodb_tabela: "Disparo Jun26", chatwoot_inbox: "18 - Yara API Oficial" },
  contatados: base,
  responderam: responderamSet.size,
  taxa_resposta_pct: base ? +(responderamSet.size / base * 100).toFixed(2) : 0,
  transferidos: transfUnicos.length,
  taxa_conversao_pct: base ? +(transfUnicos.length / base * 100).toFixed(2) : 0,
  transferidos_por_dia_media: +(transfUnicos.length / DIAS_PERIODO).toFixed(2),
  disparos_por_dia: dispDia,
  transferencias_por_dia: tDia,
  transferencias_por_semana: tSem,
  por_vendedor: tVend,
  temperatura: temp,
  respostas_detalhe: respDetalhes,
  transferidos_lista: transfUnicos.map(r => ({
    nome: r.nome, telefone: r.telefone, cidade: r.cidade,
    vendedor: r.vendedor_responsavel, temperatura: r.temperatura,
    data_transferencia: r.data_transferencia,
  })),
};
writeFileSync("relatorio_ultimos30d.json", JSON.stringify(out, null, 2));

console.log("\n" + "=".repeat(60));
console.log(`RESULTADOS 11/jul → 10/ago/2026 (30 dias)`);
console.log("=".repeat(60));
console.log(`Contatados:              ${base}`);
console.log(`Responderam:             ${responderamSet.size} (${out.taxa_resposta_pct}%)`);
console.log(`Transferidos únicos:     ${transfUnicos.length} (${out.taxa_conversao_pct}%)`);
console.log(`Transferidos/dia:        ${out.transferidos_por_dia_media}`);
console.log(`\nDisparos/dia (top 10):`);
for (const [k,v] of Object.entries(dispDia).sort((a,b)=>b[1]-a[1]).slice(0,10)) console.log(`  ${k}: ${v}`);
console.log(`\nTransferências por semana:`);
for (const [k,v] of Object.entries(tSem).sort()) console.log(`  ${k}: ${v}`);
console.log(`\nPor vendedor:`);
for (const [v,n] of Object.entries(tVend).sort((a,b)=>b[1]-a[1])) console.log(`  ${v}: ${n}`);
console.log(`\nTemperatura:`);
for (const [k,v] of Object.entries(temp).sort((a,b)=>b[1]-a[1])) console.log(`  ${k}: ${v}`);
console.log(`\nRelatório salvo: relatorio_ultimos30d.json`);
