// Análise período 20/04 a 24/04/2026
// Fonte 1: NocoDB tabela "DISPARO LIGAÇÃO" (m0ztcbw6trq2mqc)
// Fonte 2: Chatwoot inboxes "Leads 6125" (id 1) e "Leads Ligação" (id 11)

import { writeFileSync } from "fs";

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE = "m0ztcbw6trq2mqc";

const CW_BASE = "https://projetos-chatwoot.0ivxeq.easypanel.host";
const CW_TOKEN = "xmnGZd3JiwdKUAiCwxVVnnvj";
const CW_ACCOUNT = 1;
const TARGET_INBOXES = [1, 11]; // Leads 6125, Leads Ligação

const PERIODO_INICIO = new Date("2026-04-20T00:00:00-03:00");
const PERIODO_FIM = new Date("2026-04-24T23:59:59-03:00");

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

// === NOCODB ===
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

// === CHATWOOT ===
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

// === CLASSIFICAÇÃO DE RESPOSTAS ===
// Ordem de teste: errado > recusa > positivo > neutro
const ERRADO_RE = /(n[uú]mero\s*errado|n[aã]o\s+(sou|conhe[çc]o|trabalho\s+com\s+lavoura|somos\s+produtor)|esse\s+n[uú]mero\s+n[aã]o\s+[eé]|n[aã]o\s+[eé]\s+(o|a|de)|engano|outro\s+n[uú]mero|trocou\s+de\s+n[uú]mero)/i;
const RECUSA_RE = /\b(n[aã]o\s*(quero|tenho\s*interesse|preciso|me\s*interessa)|sem\s*interesse|n[aã]o\s*interessad[oa]|\bparar\b|\bsair\b|\bremover\b|\bcancelar\b|n[aã]o\s*manda|para\s*de\s*mandar|\bstop\b|descadastr)/i;
const POSITIVO_RE = /\b(quero|tenho\s*interesse|pre[çc]o|or[çc]amento|agenda|visita|manda|envia|cat[áa]logo|fechado|combinado|perfeito|hectares?|portfolio|info|gostaria|preciso\s+de|me\s+interessa)\b/i;

function classificarConversa(messages, leadName) {
  // Considera apenas mensagens incoming (do lead) no período
  const incomingsPeriodo = messages.filter(m => {
    if (m.message_type !== 0) return false; // 0 = incoming
    const ts = new Date((m.created_at || 0) * 1000);
    return ts >= PERIODO_INICIO && ts <= PERIODO_FIM;
  });
  if (!incomingsPeriodo.length) return { respondeu: false };

  const texto = incomingsPeriodo.map(m => m.content || "").join(" ").toLowerCase();

  let categoria = "neutro";
  if (ERRADO_RE.test(texto)) categoria = "contato_errado";
  else if (RECUSA_RE.test(texto)) categoria = "recusa";
  else if (POSITIVO_RE.test(texto)) categoria = "positivo";

  return {
    respondeu: true,
    categoria,
    primeiraResposta: new Date((incomingsPeriodo[0].created_at || 0) * 1000).toISOString(),
    qtdMensagens: incomingsPeriodo.length,
    amostra: incomingsPeriodo[0].content?.slice(0, 200) || "",
  };
}

// === MAIN ===
async function main() {
  console.log("=".repeat(70));
  console.log(`ANÁLISE S3P - DISPARO LIGAÇÃO + Chatwoot (20-24/04/2026)`);
  console.log("=".repeat(70));

  // 1) NocoDB
  console.log("\n[1/3] Buscando NocoDB DISPARO LIGAÇÃO...");
  const nocoAll = await fetchNocoAll();
  console.log(`  Total no NocoDB: ${nocoAll.length}`);

  // Lead "no período" = CreatedAt OU Hora_Ligacao OU data_transferencia OU Hora Mensagem dentro de 20-24/04
  const nocoPeriodo = nocoAll.filter(r => {
    const datas = [r.CreatedAt, r.UpdatedAt, r.data_transferencia];
    if (datas.some(d => inPeriodo(d))) return true;
    // Hora_Ligacao em formato "quarta-feira, 22 de abril de 2026, 12:48"
    if (r.Hora_Ligacao && /20|21|22|23|24/.test(r.Hora_Ligacao) && /abril/i.test(r.Hora_Ligacao)) {
      const m = r.Hora_Ligacao.match(/(\d{1,2})\s+de\s+abril/i);
      if (m && +m[1] >= 20 && +m[1] <= 24) return true;
    }
    return false;
  });
  console.log(`  Leads no período 20-24/04: ${nocoPeriodo.length}`);

  // 2) Chatwoot
  console.log("\n[2/3] Buscando Chatwoot inboxes-alvo...");
  const allConvs = [];
  for (const inboxId of TARGET_INBOXES) {
    process.stdout.write(`  Inbox ${inboxId}...`);
    const cs = await fetchInboxConversations(inboxId);
    console.log(` ${cs.length} conversas`);
    cs.forEach(c => c.__inbox = inboxId);
    allConvs.push(...cs);
  }
  console.log(`  Total conversas: ${allConvs.length}`);

  // Filtra conversas criadas no período OU com última atividade no período
  const convsPeriodo = allConvs.filter(c => {
    const created = new Date((c.created_at || 0) * 1000);
    const last = new Date((c.last_activity_at || 0) * 1000);
    return (created >= PERIODO_INICIO && created <= PERIODO_FIM) ||
           (last >= PERIODO_INICIO && last <= PERIODO_FIM);
  });
  console.log(`  Conversas com atividade no período: ${convsPeriodo.length}`);

  // 3) Buscar mensagens e classificar
  console.log("\n[3/3] Classificando respostas...");
  const respostas = [];
  let i = 0;
  for (const c of convsPeriodo) {
    i++;
    if (i % 20 === 0) process.stdout.write(`  ${i}/${convsPeriodo.length}\n`);
    const msgs = await fetchMessages(c.id);
    const phone = normalizePhone(c.meta?.sender?.phone_number || "");
    const cls = classificarConversa(msgs, c.meta?.sender?.name);
    if (cls.respondeu) {
      respostas.push({
        conv_id: c.id,
        inbox: c.__inbox,
        nome: c.meta?.sender?.name || "",
        telefone: phone,
        categoria: cls.categoria,
        primeira_resposta: cls.primeiraResposta,
        qtd_mensagens: cls.qtdMensagens,
        amostra: cls.amostra,
        status: c.status,
        assignee_id: c.meta?.assignee?.id || null,
        assignee_name: c.meta?.assignee?.name || null,
      });
    }
  }
  console.log(`  Conversas que responderam no período: ${respostas.length}`);

  // === Cruzamento ===
  // Leads do disparo ligação no período (telefones)
  const phonesNocoPeriodo = new Set(nocoPeriodo.map(r => normalizePhone(r.telefone)).filter(Boolean));

  const respostasDeDisparo = respostas.filter(r => phonesNocoPeriodo.has(r.telefone));

  // Transferidos:
  //   (a) NocoDB com vendedor_responsavel ou status "Transferido"
  //   (b) Conversa Chatwoot atribuída a um agente vendedor (Maria Luiza, Ronaldo, Tharik)
  const VENDEDOR_AGENT_IDS = new Set([4, 2, 5]); // Maria Luiza, Ronaldo, Tharik
  const transferidosNoco = nocoPeriodo.filter(r => {
    return (r.status || "").toLowerCase().includes("transferido") ||
           (r["status copy"] || "").toLowerCase().includes("transferido") ||
           (r.vendedor_responsavel && String(r.vendedor_responsavel).trim() !== "");
  });
  const transferidosCw = respostasDeDisparo.filter(r => VENDEDOR_AGENT_IDS.has(r.assignee_id));
  // Unir por telefone
  const transferidosMap = new Map();
  for (const r of transferidosNoco) {
    const tel = normalizePhone(r.telefone);
    transferidosMap.set(tel, {
      nome: r.nome, telefone: r.telefone, vendedor: r.vendedor_responsavel,
      cidade: r.cidade, hectares: r.hectares, data_transferencia: r.data_transferencia,
      temperatura: r.temperatura, fonte: "nocodb",
    });
  }
  for (const r of transferidosCw) {
    if (!transferidosMap.has(r.telefone)) {
      transferidosMap.set(r.telefone, {
        nome: r.nome, telefone: r.telefone, vendedor: r.assignee_name,
        fonte: "chatwoot_assign",
      });
    }
  }
  const transferidos = Array.from(transferidosMap.values());

  // Recusas / contatos errados a partir do Chatwoot cruzado
  const recusas = respostasDeDisparo.filter(r => r.categoria === "recusa");
  const errados = respostasDeDisparo.filter(r => r.categoria === "contato_errado");
  const positivos = respostasDeDisparo.filter(r => r.categoria === "positivo");
  const neutros = respostasDeDisparo.filter(r => r.categoria === "neutro");

  // Ligações realizadas no período (NocoDB Chamado=true e Hora_Ligacao no período)
  const ligacoes = nocoPeriodo.filter(r => r.Chamado === true || r.call_id);
  const ligacoesAtenderam = ligacoes.filter(r => r.Atendeu === true);

  // Group por vendedor (transferidos)
  const transfPorVendedor = {};
  for (const r of transferidos) {
    const v = r.vendedor || "Não informado";
    if (!transfPorVendedor[v]) transfPorVendedor[v] = [];
    transfPorVendedor[v].push(r);
  }

  const relatorio = {
    periodo: { inicio: "2026-04-20", fim: "2026-04-24" },
    fonte: {
      nocodb_tabela: "DISPARO LIGAÇÃO",
      chatwoot_inboxes: ["Leads 6125 (1)", "Leads Ligação (11)"],
    },
    metricas: {
      contatados: nocoPeriodo.length,
      ligacoes_efetuadas: ligacoes.length,
      ligacoes_atendidas: ligacoesAtenderam.length,
      whatsapp_enviado: nocoPeriodo.filter(r => r.WhatsappEnviado === true).length,
      responderam_no_chatwoot: respostasDeDisparo.length,
      taxa_resposta_pct: nocoPeriodo.length ? +(respostasDeDisparo.length / nocoPeriodo.length * 100).toFixed(2) : 0,
      transferidos_vendedor: transferidos.length,
      taxa_conversao_pct: nocoPeriodo.length ? +(transferidos.length / nocoPeriodo.length * 100).toFixed(2) : 0,
      recusas: recusas.length,
      contatos_errados: errados.length,
      respostas_positivas_nao_transferidas: positivos.length,
      respostas_neutras: neutros.length,
    },
    transferidos_por_vendedor: transfPorVendedor,
    detalhes: {
      transferidos,
      recusas, errados, positivos, neutros,
      ligacoes: ligacoes.map(r => ({
        nome: r.nome, telefone: r.telefone, hora_ligacao: r.Hora_Ligacao,
        atendeu: r.Atendeu, qualificado: r.Qualificado, end_reason: r.EndReason,
        resumo: r.Resumo, call_id: r.call_id, temperatura: r.temperatura,
        whatsapp_enviado: r.WhatsappEnviado,
      })),
    },
  };

  writeFileSync("relatorio_2024.json", JSON.stringify(relatorio, null, 2));

  console.log("\n" + "=".repeat(70));
  console.log("RESULTADO");
  console.log("=".repeat(70));
  for (const [k, v] of Object.entries(relatorio.metricas)) {
    console.log(`  ${k.padEnd(40)} ${v}`);
  }
  console.log("\nTransferidos por vendedor:");
  for (const [v, leads] of Object.entries(transfPorVendedor)) {
    console.log(`  ${v}: ${leads.length}`);
  }
  console.log("\nArquivo salvo: relatorio_2024.json");
}

main().catch(e => { console.error(e); process.exit(1); });
