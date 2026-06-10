// Análise período 08/06 a 10/06/2026
// Fonte 1: NocoDB tabela "Disparo mai25" (mhsizs8nybch0be)
// Fonte 2: Chatwoot - todos inboxes (match por telefone)
// Critério endurecido: transferido SÓ se data_transferencia caiu no período

import { writeFileSync } from "fs";

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE = "mp1noylkq6er2jy"; // Disparo Jun26

const CW_BASE = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT = 1;

const PERIODO_INICIO = new Date("2026-06-08T00:00:00-03:00");
const PERIODO_FIM    = new Date("2026-06-10T23:59:59-03:00");

const VENDEDOR_IDS = new Set([4, 2, 5]);
const VENDEDOR_NAMES = { 4: "Maria Luiza Durães", 2: "Ronaldo Alves", 5: "Tharik Rafael" };

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}

function inPeriodo(d) {
  if (!d) return false;
  const t = new Date(d);
  if (isNaN(t)) return false;
  return t >= PERIODO_INICIO && t <= PERIODO_FIM;
}

// formato: "quinta-feira, 28 de maio de 2026, 14:30"
function horaMensagemNoPeriodo(s) {
  if (!s) return false;
  const m = s.match(/(\d{1,2})\s+de\s+(maio|junho)\s+de\s+2026/i);
  if (!m) return false;
  const day = +m[1];
  const mes = m[2].toLowerCase();
  if (mes === "junho") return day >= 8 && day <= 10;
  return false;
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

async function main() {
  console.log("=".repeat(70));
  console.log("ANÁLISE S3P - Disparo Jun26 - 08/06 a 10/06/2026");
  console.log("=".repeat(70));

  console.log("\n[1/4] Buscando NocoDB Disparo mai25...");
  const nocoAll = await fetchNocoAll();
  console.log(`  Total na tabela: ${nocoAll.length}`);

  const contatados = nocoAll.filter(r => {
    if (!r.Disparo) return false;
    return horaMensagemNoPeriodo(r["Hora Mensagem"]) || inPeriodo(r.CreatedAt);
  });
  console.log(`  Contatados 08-10/06 (Disparo=true): ${contatados.length}`);

  console.log("\n[2/4] Listando inboxes Chatwoot...");
  const inboxes = await listInboxes();
  console.log(`  Inboxes: ${inboxes.length}`);

  console.log("\n[3/4] Buscando conversas com atividade no período...");
  const phonesContatados = new Set(contatados.map(r => normalizePhone(r.telefone)).filter(Boolean));
  console.log(`  Telefones únicos no recorte NocoDB: ${phonesContatados.size}`);

  const allConvs = [];
  for (const ib of inboxes) {
    if (ib.id === 16) { console.log(`  Inbox 16 Handoff Vendedores... PULADO (canal interno)`); continue; }
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
      console.log(` ${cs.length} total / ${inPer.length} no período`);
    } catch (e) {
      console.log(` ERRO ${e.message}`);
    }
  }
  console.log(`  Total conversas no período: ${allConvs.length}`);

  console.log("\n[4/4] Verificando respostas dos contatados...");
  const respostasPorTelefone = new Map();

  let i = 0;
  for (const c of allConvs) {
    i++;
    if (i % 30 === 0) process.stdout.write(`  ${i}/${allConvs.length}\n`);
    const telConv = normalizePhone(c.meta?.sender?.phone_number || "");
    if (!phonesContatados.has(telConv)) continue;

    const msgs = await fetchMessages(c.id);
    const incomingPeriodo = msgs.filter(m => {
      if (m.message_type !== 0) return false;
      const ts = new Date((m.created_at || 0) * 1000);
      return ts >= PERIODO_INICIO && ts <= PERIODO_FIM;
    });
    if (!incomingPeriodo.length) continue;

    const existing = respostasPorTelefone.get(telConv) || [];
    existing.push({
      conv_id: c.id,
      inbox: c.__inbox_name,
      nome: c.meta?.sender?.name || "",
      telefone: telConv,
      status: c.status,
      assignee_id: c.meta?.assignee?.id || null,
      assignee_name: c.meta?.assignee?.name || null,
      qtd_mensagens_periodo: incomingPeriodo.length,
      primeira: new Date((incomingPeriodo[0].created_at || 0) * 1000).toISOString(),
      amostra: incomingPeriodo[0].content?.slice(0, 200) || "",
    });
    respostasPorTelefone.set(telConv, existing);
  }
  const responderam = Array.from(respostasPorTelefone.keys());
  console.log(`\n  Contatados que responderam (Chatwoot): ${responderam.length}`);

  // --- TRANSFERIDOS (critério endurecido: data_transferencia DENTRO do período) ---
  const transferidosMap = new Map();

  for (const r of contatados) {
    const tel = normalizePhone(r.telefone);
    const dataTransfNoPer = inPeriodo(r.data_transferencia);
    if (dataTransfNoPer) {
      transferidosMap.set(tel, {
        nome: r.nome,
        telefone: r.telefone,
        vendedor: r.vendedor_responsavel || null,
        status: r.status || null,
        data_transferencia: r.data_transferencia,
        cidade: r.cidade,
        temperatura: r.temperatura,
        fonte: "nocodb",
      });
    }
  }

  // Chatwoot fallback: respostas atribuídas a vendedor (mesmo se sem data_transferencia)
  for (const [tel, convs] of respostasPorTelefone.entries()) {
    const atribuidoVendedor = convs.find(c => VENDEDOR_IDS.has(c.assignee_id));
    if (atribuidoVendedor && !transferidosMap.has(tel)) {
      transferidosMap.set(tel, {
        nome: atribuidoVendedor.nome,
        telefone: tel,
        vendedor: atribuidoVendedor.assignee_name,
        fonte: "chatwoot_assign",
      });
    }
  }

  const transferidos = Array.from(transferidosMap.values());

  const porVendedor = {};
  for (const t of transferidos) {
    const v = t.vendedor || "Não informado";
    porVendedor[v] = (porVendedor[v] || 0) + 1;
  }

  const respSemTransf = responderam.filter(tel => !transferidosMap.has(tel));

  const relatorio = {
    periodo: { inicio: "2026-06-08", fim: "2026-06-10" },
    fonte: { tabela: "Disparo Jun26 (mp1noylkq6er2jy)", chatwoot: "todos inboxes exceto 16 (Handoff interno)" },
    criterio_transferencia: "data_transferencia DENTRO do período OU assignee Chatwoot é vendedor",
    metricas: {
      contatados: contatados.length,
      responderam_chatwoot: responderam.length,
      taxa_resposta_pct: contatados.length ? +(responderam.length/contatados.length*100).toFixed(2) : 0,
      direcionados_vendedor: transferidos.length,
      taxa_conversao_pct: contatados.length ? +(transferidos.length/contatados.length*100).toFixed(2) : 0,
      responderam_sem_transferencia: respSemTransf.length,
    },
    direcionados_por_vendedor: porVendedor,
    detalhes: {
      transferidos,
      respostas: Array.from(respostasPorTelefone.values()).flat(),
      respostas_sem_transferencia: respSemTransf.map(tel =>
        respostasPorTelefone.get(tel)
      ).flat(),
    },
  };

  writeFileSync("relatorio_08_10jun.json", JSON.stringify(relatorio, null, 2));

  console.log("\n" + "=".repeat(70));
  console.log("RESULTADO 08/06 a 10/06/2026");
  console.log("=".repeat(70));
  for (const [k, v] of Object.entries(relatorio.metricas)) {
    console.log(`  ${k.padEnd(40)} ${v}`);
  }
  console.log("\nDirecionados por vendedor:");
  for (const [v, n] of Object.entries(porVendedor)) {
    console.log(`  ${v}: ${n}`);
  }
  console.log("\nArquivo: relatorio_08_10jun.json");
}

main().catch(e => { console.error(e); process.exit(1); });
