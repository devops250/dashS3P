// Análise inbox Chatwoot "Yara 4530" (id 18) - 28 a 29/05/2026
// Cruza com NocoDB Disparo mai25 para identificar transferências

import { writeFileSync } from "fs";

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE = "mhsizs8nybch0be";

const CW_BASE = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT = 1;
const INBOX_ID = 18; // Yara 4530

const PERIODO_INICIO = new Date("2026-05-28T00:00:00-03:00");
const PERIODO_FIM    = new Date("2026-05-29T23:59:59-03:00");

const VENDEDOR_IDS = new Set([4, 2, 5]);

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}

function inPeriodoTs(unixSec) {
  if (!unixSec) return false;
  const t = new Date(unixSec * 1000);
  return t >= PERIODO_INICIO && t <= PERIODO_FIM;
}
function inPeriodoDate(d) {
  if (!d) return false;
  const t = new Date(d);
  if (isNaN(t)) return false;
  return t >= PERIODO_INICIO && t <= PERIODO_FIM;
}

async function cwGet(endpoint, params = {}) {
  const url = new URL(`/api/v1/accounts/${CW_ACCOUNT}/${endpoint}`, CW_BASE);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const r = await fetch(url.toString(), { headers: { api_access_token: CW_TOKEN } });
  if (!r.ok) throw new Error(`CW ${r.status}`);
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
  } catch { return []; }
}

async function fetchNocoAll() {
  const all = [];
  let offset = 0;
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

async function main() {
  console.log("=".repeat(70));
  console.log("CHATWOOT Yara 4530 (inbox 18) - 28 a 29/05/2026");
  console.log("=".repeat(70));

  console.log("\n[1/3] Buscando conversas do inbox 18...");
  const allConvs = await fetchInboxConversations(INBOX_ID);
  console.log(`  Total no inbox: ${allConvs.length}`);

  // Filtrar por última atividade ou criação no período
  const convsPer = allConvs.filter(c => {
    const created = c.created_at || 0;
    const last = c.last_activity_at || 0;
    return inPeriodoTs(created) || inPeriodoTs(last);
  });
  console.log(`  Conversas com atividade no período: ${convsPer.length}`);

  console.log("\n[2/3] Carregando mensagens e classificando...");
  const contatados = []; // tem mensagem outgoing no período
  const responderam = []; // tem mensagem incoming no período
  let i = 0;
  for (const c of convsPer) {
    i++;
    if (i % 50 === 0) process.stdout.write(`  ${i}/${convsPer.length}\n`);
    const msgs = await fetchMessages(c.id);
    const out = msgs.filter(m => m.message_type === 1 && inPeriodoTs(m.created_at));
    const inc = msgs.filter(m => m.message_type === 0 && inPeriodoTs(m.created_at));

    const tel = normalizePhone(c.meta?.sender?.phone_number || "");
    const rec = {
      conv_id: c.id,
      nome: c.meta?.sender?.name || "",
      telefone: tel,
      status: c.status,
      assignee_id: c.meta?.assignee?.id || null,
      assignee_name: c.meta?.assignee?.name || null,
      qtd_out: out.length,
      qtd_in: inc.length,
      primeira_in: inc.length ? new Date(inc[0].created_at*1000).toISOString() : null,
      amostra_in: inc.length ? (inc[0].content||"").slice(0,200) : null,
    };
    if (out.length) contatados.push(rec);
    if (inc.length) responderam.push(rec);
  }
  console.log(`\n  Contatados (msg outgoing no período): ${contatados.length}`);
  console.log(`  Responderam (msg incoming no período): ${responderam.length}`);

  // Únicos por telefone
  const uniqTel = arr => {
    const m = new Map();
    for (const r of arr) if (r.telefone && !m.has(r.telefone)) m.set(r.telefone, r);
    return Array.from(m.values());
  };
  const contatadosUniq = uniqTel(contatados);
  const responderamUniq = uniqTel(responderam);
  console.log(`  Contatados únicos (por telefone): ${contatadosUniq.length}`);
  console.log(`  Responderam únicos (por telefone): ${responderamUniq.length}`);

  console.log("\n[3/3] Cruzando com NocoDB Disparo mai25 para detectar transferências...");
  const noco = await fetchNocoAll();
  const nocoByTel = new Map();
  for (const r of noco) {
    const t = normalizePhone(r.telefone);
    if (t) nocoByTel.set(t, r);
  }

  // Direcionados:
  //   (A) Chatwoot: conversa do respondente atribuída a agente vendedor (Maria Luiza/Ronaldo/Tharik)
  //   (B) NocoDB Disparo mai25 com vendedor_responsavel preenchido OU data_transferencia no período
  const transferidosMap = new Map();
  for (const r of responderamUniq) {
    const fromCw = VENDEDOR_IDS.has(r.assignee_id);
    const noc = nocoByTel.get(r.telefone);
    const fromNoco = noc && (
      (noc.vendedor_responsavel && String(noc.vendedor_responsavel).trim() !== "") ||
      (noc.status || "").toLowerCase().includes("transferid") ||
      inPeriodoDate(noc.data_transferencia)
    );
    if (fromCw || fromNoco) {
      transferidosMap.set(r.telefone, {
        nome: r.nome,
        telefone: r.telefone,
        vendedor_cw: fromCw ? r.assignee_name : null,
        vendedor_noco: noc?.vendedor_responsavel || null,
        data_transferencia: noc?.data_transferencia || null,
        status_noco: noc?.status || null,
        amostra: r.amostra_in,
      });
    }
  }
  const transferidos = Array.from(transferidosMap.values());

  // Agrupar por vendedor
  const porVendedor = {};
  for (const t of transferidos) {
    const v = t.vendedor_noco || t.vendedor_cw || "Não informado";
    porVendedor[v] = (porVendedor[v] || 0) + 1;
  }

  const respSemTransf = responderamUniq.filter(r => !transferidosMap.has(r.telefone));

  const relatorio = {
    fonte: { chatwoot_inbox: "Yara 4530 (18)", cruzamento_noco: "Disparo mai25" },
    periodo: { inicio: "2026-05-28", fim: "2026-05-29" },
    metricas: {
      contatados: contatadosUniq.length,
      responderam: responderamUniq.length,
      taxa_resposta_pct: contatadosUniq.length ? +(responderamUniq.length/contatadosUniq.length*100).toFixed(2) : 0,
      direcionados_vendedor: transferidos.length,
      taxa_conversao_pct: contatadosUniq.length ? +(transferidos.length/contatadosUniq.length*100).toFixed(2) : 0,
      responderam_sem_transferencia: respSemTransf.length,
    },
    direcionados_por_vendedor: porVendedor,
    detalhes: {
      transferidos,
      respostas_sem_transferencia: respSemTransf,
    },
  };

  writeFileSync("relatorio_yara4530_28_29mai.json", JSON.stringify(relatorio, null, 2));

  console.log("\n" + "=".repeat(70));
  console.log("RESULTADO - Yara 4530 - 28-29/05/2026");
  console.log("=".repeat(70));
  for (const [k,v] of Object.entries(relatorio.metricas)) {
    console.log(`  ${k.padEnd(40)} ${v}`);
  }
  console.log("\nDirecionados por vendedor:");
  for (const [v,n] of Object.entries(porVendedor)) console.log(`  ${v}: ${n}`);
  console.log("\nArquivo: relatorio_yara4530_28_29mai.json");
}

main().catch(e => { console.error(e); process.exit(1); });
