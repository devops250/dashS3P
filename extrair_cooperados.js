/**
 * Extração de Dados - Yara Cooperados (Sementes Três Pinheiros)
 * Cruza NocoDB (Lista Cooperados Oficial) com Chatwoot (inbox Yara Cooperados)
 * Classifica leads e gera relatório + CSV para follow-up
 *
 * Executar: node extrair_cooperados.js
 */

const fs = require('fs');
const https = require('https');

// === CONFIG ===
const CW_BASE = 'https://projetos-chatwoot.0ivxeq.easypanel.host/api/v1/accounts/1';
const CW_TOKEN = 'xmnGZd3JiwdKUAiCwxVVnnvj';
const NOCO_BASE = 'https://projetos-nocodb.0ivxeq.easypanel.host/api/v2/tables';
const NOCO_TOKEN = 'mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R';
const NOCO_TABLE = 'm8jyvyvnz1i5vsm'; // Lista Cooperados Oficial
const CW_INBOX_ID = 9; // Yara Cooperados
const DIAS_PENDENTE = 3; // dias sem resposta = pendente

// === HTTP HELPER ===
function fetchJSON(url, headers, retries = 3) {
  return new Promise((resolve, reject) => {
    const doFetch = (attempt) => {
      const opts = { headers, timeout: 30000 };
      https.get(url, opts, (res) => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString()));
          } catch (e) {
            reject(new Error(`JSON parse error: ${e.message}`));
          }
        });
      }).on('error', (e) => {
        if (attempt < retries) {
          setTimeout(() => doFetch(attempt + 1), 1000 * attempt);
        } else reject(e);
      });
    };
    doFetch(1);
  });
}

// Rate limiter
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// === STEP 1: LOAD ALL NOCODB RECORDS ===
async function loadNocoDB() {
  process.stderr.write('=== Carregando NocoDB (Lista Cooperados Oficial) ===\n');
  const all = [];
  let offset = 0;
  while (true) {
    const url = `${NOCO_BASE}/${NOCO_TABLE}/records?limit=200&offset=${offset}`;
    const data = await fetchJSON(url, { 'xc-token': NOCO_TOKEN });
    const rows = data.list || [];
    all.push(...rows);
    process.stderr.write(`  Carregados ${all.length} / ${data.pageInfo?.totalRows || '?'} registros\n`);
    if (data.pageInfo?.isLastPage || rows.length === 0) break;
    offset += rows.length;
  }
  process.stderr.write(`Total NocoDB: ${all.length} cooperados\n\n`);
  return all;
}

// === STEP 2: LOAD ALL CHATWOOT CONVERSATIONS + MESSAGES ===
async function loadChatwoot() {
  process.stderr.write('=== Carregando Chatwoot (Yara Cooperados) ===\n');

  // Load all conversations
  const allConvs = [];
  let page = 1;
  while (true) {
    const url = `${CW_BASE}/conversations?inbox_id=${CW_INBOX_ID}&page=${page}`;
    const data = await fetchJSON(url, { 'api_access_token': CW_TOKEN });
    const convs = data.data?.payload || [];
    allConvs.push(...convs);
    const total = data.data?.meta?.all_count || '?';
    process.stderr.write(`  Pagina ${page}: ${convs.length} conversas (total: ${total})\n`);
    if (convs.length < 25) break; // last page
    page++;
    await sleep(300);
  }
  process.stderr.write(`Total conversas: ${allConvs.length}\n`);

  // Load messages for each conversation
  process.stderr.write('\n=== Carregando mensagens de cada conversa ===\n');
  for (let i = 0; i < allConvs.length; i++) {
    const conv = allConvs[i];
    try {
      const url = `${CW_BASE}/conversations/${conv.id}/messages`;
      const data = await fetchJSON(url, { 'api_access_token': CW_TOKEN });
      conv._messages = data.payload || [];
      process.stderr.write(`  [${i + 1}/${allConvs.length}] Conv #${conv.id}: ${conv._messages.length} msgs\n`);
    } catch (e) {
      process.stderr.write(`  [${i + 1}/${allConvs.length}] Conv #${conv.id}: ERRO - ${e.message}\n`);
      conv._messages = [];
    }
    if (i % 5 === 4) await sleep(500); // rate limit
  }

  process.stderr.write(`\nMensagens carregadas para ${allConvs.length} conversas\n\n`);
  return allConvs;
}

// === STEP 3: NORMALIZE PHONE ===
function normalizePhone(phone) {
  if (!phone) return '';
  return phone.replace(/\D/g, '').replace(/^0+/, '');
}

function phoneKeys(phone) {
  const clean = normalizePhone(phone);
  // Multiple key lengths for fuzzy match: 8, 9, 10, 11 digits
  return {
    k8: clean.slice(-8),
    k9: clean.slice(-9),
    k10: clean.slice(-10),
    k11: clean.slice(-11),
    full: clean
  };
}

// === STEP 4: CLASSIFY CONVERSATIONS ===
function classifyConversation(conv, nocoData) {
  const msgs = conv._messages || [];
  const now = Date.now();
  const lastActivity = (conv.last_activity_at || conv.timestamp || 0) * 1000;
  const diasParado = Math.floor((now - lastActivity) / (1000 * 60 * 60 * 24));

  // Get all message text combined
  const allText = msgs
    .filter(m => m.message_type !== 2) // exclude activity
    .map(m => (m.content || '').toLowerCase())
    .join(' ');

  // Incoming messages (from lead)
  const incomingMsgs = msgs.filter(m => m.message_type === 0); // incoming
  const outgoingMsgs = msgs.filter(m => m.message_type === 1); // outgoing

  const incomingText = incomingMsgs.map(m => (m.content || '').toLowerCase()).join(' ');
  const outgoingText = outgoingMsgs.map(m => (m.content || '').toLowerCase()).join(' ');

  // Last incoming message
  const lastIncoming = incomingMsgs.length > 0 ? incomingMsgs[incomingMsgs.length - 1] : null;
  const lastIncomingText = lastIncoming ? (lastIncoming.content || '').toLowerCase() : '';

  // NocoDB status override
  const nocoStatus = (nocoData?.status_conversa || '').toLowerCase();
  const nocoContrato = nocoData?.status_contrato || '';

  // Classification logic
  let status = 'PENDENTE';
  let motivo = '';

  // Detect if lead informed specific hectares/cultivars in conversation (strong interest signal)
  const hectaresPattern = /(\d+)\s*(ha|hectare|hect)/i;
  const informouArea = hectaresPattern.test(incomingText);
  const informouCultivar = /tormenta|olimpo|m[ií]tica|sparta|hera|c2795|c2810/i.test(incomingText);
  const confirmaPositivo = /pode gerar|pode sim|sim quero|quero manter|quero plantar|obrigad[oa]|beleza|combinado|perfeito|t[aá] bom|isso/i.test(incomingText);

  // 1. FECHAMENTO - contrato enviado/assinado or explicit closing language
  const fechamentoPatterns = /fechad|vamos fechar|pode gerar contrato|pode gerar|aceito|contrato|assinado|fechei|vou plantar|confirmo/;
  if (nocoData?.['contrato assinado'] === true) {
    status = 'FECHAMENTO';
    motivo = 'Contrato assinado no NocoDB';
  } else if (nocoData?.['contrato enviado'] === true) {
    status = 'FECHAMENTO';
    motivo = 'Contrato enviado';
  } else if (nocoContrato && nocoContrato.toLowerCase().includes('assinado')) {
    status = 'FECHAMENTO';
    motivo = `Status contrato: ${nocoContrato}`;
  } else if (fechamentoPatterns.test(incomingText)) {
    status = 'FECHAMENTO';
    motivo = 'Lead indicou fechamento na conversa';
  }

  // 1b. INTERESSE_FORTE - informou área + cultivar específicos (quase fechamento)
  else if (informouArea && informouCultivar && confirmaPositivo) {
    status = 'INTERESSE_FORTE';
    motivo = 'Informou área e cultivar com confirmação positiva';
  } else if (informouArea && informouCultivar) {
    status = 'INTERESSE_FORTE';
    motivo = 'Informou hectares e cultivar de interesse';
  }

  // 2. TRANSFERENCIA - pediu agrônomo/humano ou pediu ligação
  else if (/falar com|agr[oô]nomo|humano|consultor|atendente|vendedor|representante|pessoa real|me liga|quero ligar/i.test(incomingText) ||
           conv.status === 'pending' ||
           nocoStatus.includes('transfer')) {
    status = 'TRANSFERENCIA';
    motivo = 'Solicitou contato humano';
    if (/me liga|ligar|liga.*tarde|liga.*manhã/i.test(incomingText)) motivo = 'Pediu ligação';
    if (nocoStatus.includes('transfer')) motivo = `NocoDB: ${nocoData.status_conversa}`;
  }

  // 3. PENSANDO - disse que ia pensar/avaliar/consultar alguém
  else if (/vou pensar|preciso ver|depois decido|vou avaliar|conversar com|preciso avaliar|analisar|ver com|meu s[oó]cio|esposa|marido|verificar|trocar id[eé]ia|pesquisar|pesquisa um pouco|dar uma pesquisada|variedades novas pra mim/i.test(incomingText)) {
    status = 'PENSANDO';
    const match = incomingText.match(/vou pensar|preciso ver|depois decido|vou avaliar|conversar com|preciso avaliar|analisar|ver com|meu s[oó]cio|esposa|marido|verificar|trocar id[eé]ia|pesquisar|pesquisa um pouco|dar uma pesquisada|variedades novas/i);
    motivo = `Lead disse: "${match ? match[0] : 'vai pensar'}"`;
  }

  // 4. COM DUVIDAS - expressou dúvidas (perguntou sobre cultivar/opções/material)
  else if (/d[uú]vida|n[aã]o entendi|como funciona|qual o pre[cç]o|pre[cç]o|valor|quanto custa|prazo|entrega|quais.*op[cç][oõ]es|outras op[cç][oõ]es|tem.*material|ciclo.*d[ao]|multiplica[cç][aã]o|\?.*\?/i.test(incomingText)) {
    status = 'COM_DUVIDAS';
    const duvidasTecnicas = /cultivar|semente|plantio|solo|clima|adapta|resist[eê]ncia|ciclo|manejo|multiplica[cç][aã]o|op[cç][oõ]es|material/i.test(incomingText);
    const duvidasComerciais = /pre[cç]o|valor|custo|pagamento|prazo|entrega|frete|desconto/i.test(incomingText);
    if (duvidasTecnicas && duvidasComerciais) motivo = 'Dúvidas técnicas e comerciais';
    else if (duvidasTecnicas) motivo = 'Dúvida técnica (cultivares/opções)';
    else if (duvidasComerciais) motivo = 'Dúvida comercial';
    else motivo = 'Expressou dúvidas na conversa';

    // Check if answered
    const lastQ = incomingMsgs.findIndex(m => /\?|d[uú]vida|como|qual|opç/i.test(m.content || ''));
    if (lastQ >= 0) {
      const afterQ = outgoingMsgs.filter(m => m.created_at > incomingMsgs[lastQ].created_at);
      if (afterQ.length > 0) motivo += ' (respondida pela Yara)';
      else motivo += ' (SEM RESPOSTA)';
    }
  }

  // 4b. INTERESSE - lead respondeu positivamente mas sem detalhar hectares
  else if (informouCultivar || (confirmaPositivo && incomingMsgs.length >= 3)) {
    status = 'INTERESSE';
    motivo = informouCultivar ? 'Mencionou cultivar de interesse' : 'Respostas positivas na conversa';
  }

  // 5. DESINTERESSADO - recusa explicita (mas "não conheço" sobre cultivar = dúvida, não desinteresse)
  else if (/n[aã]o tenho interesse|n[aã]o quero|n[aã]o preciso|n[aã]o trabalh|n[aã]o plant|n[aã]o sou produtor|errado|engano|pare de|saia|bloque/i.test(incomingText)) {
    status = 'DESINTERESSADO';
    motivo = 'Lead demonstrou desinteresse';
  }

  // 6. PENDENTE (default) - refine
  else {
    if (incomingMsgs.length === 0) {
      status = 'PENDENTE';
      motivo = 'Sem resposta do lead';
    } else if (diasParado >= DIAS_PENDENTE) {
      status = 'PENDENTE';
      motivo = `Sem interação há ${diasParado} dias`;
    } else if (nocoStatus.includes('aguardando')) {
      status = 'PENDENTE';
      motivo = `NocoDB: ${nocoData.status_conversa}`;
    } else if (incomingMsgs.length > 0) {
      status = 'PENDENTE';
      motivo = 'Conversa aberta sem conclusão';
    }
  }

  // Override from NocoDB status_conversa if clear
  if (nocoStatus.includes('fechado') || nocoStatus.includes('fechamento')) {
    status = 'FECHAMENTO';
    motivo = `NocoDB: ${nocoData.status_conversa}`;
  }

  // Calculate area from cultivars
  let areaHa = 0;
  const cultivares = [];
  for (const cult of ['Tormenta', 'Olimpo', 'Mítica', 'Sparta', 'Hera', 'C2795', 'C2810']) {
    const val = parseFloat(nocoData?.[cult] || 0);
    if (val > 0) {
      areaHa += val;
      cultivares.push(`${cult}: ${val}ha`);
    }
  }

  // Extract last incoming message snippet
  const lastMsgSnippet = lastIncoming ?
    (lastIncoming.content || '').substring(0, 120) :
    'Sem resposta';

  const lastMsgDate = lastIncoming ?
    new Date(lastIncoming.created_at * 1000).toISOString().split('T')[0] :
    null;

  return {
    status,
    motivo,
    diasParado,
    areaHa,
    cultivares: cultivares.join(', '),
    totalMsgsIn: incomingMsgs.length,
    totalMsgsOut: outgoingMsgs.length,
    lastMsgSnippet,
    lastMsgDate,
    conversaAberta: conv.status === 'open',
    labels: (conv.labels || []).join(', ')
  };
}

// === STEP 5: SUGGEST NEXT ACTION ===
function sugerirAcao(classif) {
  switch (classif.status) {
    case 'FECHAMENTO': return 'Enviar contrato / confirmar dados';
    case 'INTERESSE_FORTE': return 'Prioridade: confirmar área e gerar contrato';
    case 'INTERESSE': return 'Follow-up consultivo para avançar ao fechamento';
    case 'TRANSFERENCIA': return 'Conectar com agrônomo/vendedor';
    case 'PENSANDO': return `Follow-up consultivo (parado ${classif.diasParado}d)`;
    case 'COM_DUVIDAS': return 'Responder dúvidas pendentes';
    case 'DESINTERESSADO': return 'Arquivar / remover da cadência';
    case 'PENDENTE':
      if (classif.totalMsgsIn === 0) return 'Reenviar abordagem (não respondeu)';
      if (classif.diasParado > 7) return 'Follow-up urgente (>7d parado)';
      return 'Follow-up em 2-3 dias';
    default: return 'Avaliar manualmente';
  }
}

// === STEP 6: GENERATE CSV ===
function generateCSV(results) {
  const header = 'Nome,Telefone,Município,Estado,Fazenda,Área (ha),Cultivares,Status,Última Interação,Dias Parado,Msgs Lead,Msgs Yara,Motivo/Observação,Próxima Ação Sugerida,Supervisor,Responsável,Link Chatwoot';

  const rows = results
    .filter(r => r.status !== 'DESINTERESSADO' && r.telefone && r.nome !== 'Giselli 3P')
    .sort((a, b) => {
      // Priority: area maior + mais tempo parado
      const scoreA = (a.areaHa || 0) * 0.3 + (a.diasParado || 0) * 0.7;
      const scoreB = (b.areaHa || 0) * 0.3 + (b.diasParado || 0) * 0.7;
      return scoreB - scoreA;
    })
    .map(r => {
      const fields = [
        r.nome, r.telefone, r.municipio, r.estado, r.fazenda,
        r.areaHa || 'N/D', r.cultivares || 'N/D', r.status,
        r.lastMsgDate || 'N/D', r.diasParado, r.totalMsgsIn, r.totalMsgsOut,
        r.motivo, r.acao, r.supervisor || 'N/D', r.responsavel || 'N/D',
        r.chatwootLink || 'N/D'
      ];
      return fields.map(f => `"${String(f || 'N/D').replace(/"/g, '""')}"`).join(',');
    });

  return header + '\n' + rows.join('\n');
}

// === STEP 7: GENERATE REPORT ===
function generateReport(results) {
  const now = new Date().toISOString();
  const total = results.length;

  const byStatus = {};
  for (const r of results) {
    if (!byStatus[r.status]) byStatus[r.status] = [];
    byStatus[r.status].push(r);
  }

  const fechamentos = byStatus['FECHAMENTO'] || [];
  const interesseForte = byStatus['INTERESSE_FORTE'] || [];
  const interesse = byStatus['INTERESSE'] || [];
  const pendentes = byStatus['PENDENTE'] || [];
  const duvidas = byStatus['COM_DUVIDAS'] || [];
  const pensando = byStatus['PENSANDO'] || [];
  const transferencias = byStatus['TRANSFERENCIA'] || [];
  const desinteressados = byStatus['DESINTERESSADO'] || [];

  const areaFechada = fechamentos.reduce((s, r) => s + (r.areaHa || 0), 0);
  const areaInteresseForte = interesseForte.reduce((s, r) => s + (r.areaHa || 0), 0);
  const areaTotal = results.reduce((s, r) => s + (r.areaHa || 0), 0);

  let report = `
================================================================================
   RELATORIO CONSOLIDADO - YARA COOPERADOS (Sementes Tres Pinheiros)
   Extracao: ${now}
================================================================================

RESUMO EXECUTIVO
────────────────
Total de cooperados trabalhados:  ${total}
NocoDB (Lista Cooperados Oficial): ${total} registros
Chatwoot (inbox Yara Cooperados):  conversas cruzadas

RESULTADOS POR CATEGORIA
─────────────────────────
  FECHAMENTOS:       ${String(fechamentos.length).padStart(3)}  (${(fechamentos.length/total*100).toFixed(1)}%)  |  Area: ${areaFechada} ha
  INTERESSE FORTE:   ${String(interesseForte.length).padStart(3)}  (${(interesseForte.length/total*100).toFixed(1)}%)  |  Area: ${areaInteresseForte} ha  [informou hectares + cultivar]
  INTERESSE:         ${String(interesse.length).padStart(3)}  (${(interesse.length/total*100).toFixed(1)}%)
  PENDENTES:         ${String(pendentes.length).padStart(3)}  (${(pendentes.length/total*100).toFixed(1)}%)
  COM DUVIDAS:       ${String(duvidas.length).padStart(3)}  (${(duvidas.length/total*100).toFixed(1)}%)
  PENSANDO:          ${String(pensando.length).padStart(3)}  (${(pensando.length/total*100).toFixed(1)}%)
  TRANSFERENCIAS:    ${String(transferencias.length).padStart(3)}  (${(transferencias.length/total*100).toFixed(1)}%)
  DESINTERESSADOS:   ${String(desinteressados.length).padStart(3)}  (${(desinteressados.length/total*100).toFixed(1)}%)

AREA TOTAL MAPEADA: ${areaTotal} ha
PIPELINE DE FECHAMENTO: ${areaFechada + areaInteresseForte} ha (fechados: ${areaFechada} + interesse forte: ${areaInteresseForte})
`;

  // Detail per category
  if (fechamentos.length > 0) {
    report += `\n\nFECHAMENTOS (${fechamentos.length})\n${'─'.repeat(60)}\n`;
    for (const r of fechamentos) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha | ${r.cultivares || 'N/D'}\n`;
      report += `    ${r.motivo}\n`;
    }
  }

  if (interesseForte.length > 0) {
    report += `\n\nINTERESSE FORTE - INFORMOU AREA + CULTIVAR (${interesseForte.length})\n${'─'.repeat(60)}\n`;
    for (const r of interesseForte) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha | ${r.cultivares || 'N/D'}\n`;
      report += `    ${r.motivo} | Ultima msg: ${r.lastMsgDate || 'N/D'} (${r.diasParado}d)\n`;
      report += `    Acao: ${r.acao}\n`;
    }
  }

  if (interesse.length > 0) {
    report += `\n\nINTERESSE - RESPOSTA POSITIVA (${interesse.length})\n${'─'.repeat(60)}\n`;
    for (const r of interesse) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha\n`;
      report += `    ${r.motivo} | Ultima msg: ${r.lastMsgDate || 'N/D'} (${r.diasParado}d)\n`;
    }
  }

  if (transferencias.length > 0) {
    report += `\n\nTRANSFERENCIAS PARA HUMANO (${transferencias.length})\n${'─'.repeat(60)}\n`;
    for (const r of transferencias) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha\n`;
      report += `    ${r.motivo} | Parado: ${r.diasParado}d\n`;
    }
  }

  if (pensando.length > 0) {
    report += `\n\nPENSANDO (${pensando.length})\n${'─'.repeat(60)}\n`;
    for (const r of pensando) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha\n`;
      report += `    ${r.motivo} | Ultima msg: ${r.lastMsgDate || 'N/D'} (${r.diasParado}d)\n`;
    }
  }

  if (duvidas.length > 0) {
    report += `\n\nCOM DUVIDAS (${duvidas.length})\n${'─'.repeat(60)}\n`;
    for (const r of duvidas) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha\n`;
      report += `    ${r.motivo} | Ultima msg: ${r.lastMsgDate || 'N/D'} (${r.diasParado}d)\n`;
    }
  }

  if (pendentes.length > 0) {
    report += `\n\nPENDENTES - SEM RESPOSTA/AGUARDANDO (${pendentes.length})\n${'─'.repeat(60)}\n`;
    const sorted = [...pendentes].sort((a, b) => (b.areaHa || 0) - (a.areaHa || 0));
    for (const r of sorted.slice(0, 30)) {
      report += `  ${r.nome} | ${r.municipio}/${r.estado} | ${r.areaHa}ha | Parado: ${r.diasParado}d\n`;
      report += `    ${r.motivo}\n`;
    }
    if (sorted.length > 30) report += `  ... e mais ${sorted.length - 30} pendentes\n`;
  }

  // Top priorities for follow-up
  // Priority: INTERESSE_FORTE > COM_DUVIDAS/PENSANDO/TRANSFERENCIA/INTERESSE > PENDENTE with area
  const statusWeight = { INTERESSE_FORTE: 1000, TRANSFERENCIA: 500, COM_DUVIDAS: 400, PENSANDO: 300, INTERESSE: 200, PENDENTE: 0 };
  const followUp = results
    .filter(r => !['FECHAMENTO', 'DESINTERESSADO'].includes(r.status) && r.telefone && r.nome !== 'Giselli 3P')
    .sort((a, b) => {
      const scoreA = (statusWeight[a.status] || 0) + (a.areaHa || 0) * 0.5 + Math.min(a.diasParado || 0, 30) * 0.3;
      const scoreB = (statusWeight[b.status] || 0) + (b.areaHa || 0) * 0.5 + Math.min(b.diasParado || 0, 30) * 0.3;
      return scoreB - scoreA;
    });

  report += `\n\nTOP 10 PRIORIDADES FOLLOW-UP (área + tempo parado)\n${'─'.repeat(50)}\n`;
  for (const r of followUp.slice(0, 10)) {
    report += `  ${r.status.padEnd(15)} ${r.nome.padEnd(30)} ${String(r.areaHa || 0).padStart(5)}ha  ${String(r.diasParado).padStart(3)}d  → ${r.acao}\n`;
  }

  // Main reasons for non-closing
  report += `\n\nPRINCIPAIS MOTIVOS DE NÃO-FECHAMENTO\n${'─'.repeat(50)}\n`;
  const motivos = {};
  for (const r of results.filter(r => r.status !== 'FECHAMENTO')) {
    const key = r.motivo.split(' (')[0]; // remove parenthetical
    motivos[key] = (motivos[key] || 0) + 1;
  }
  const sortedMotivos = Object.entries(motivos).sort((a, b) => b[1] - a[1]);
  for (const [m, count] of sortedMotivos.slice(0, 10)) {
    report += `  ${String(count).padStart(3)}x  ${m}\n`;
  }

  return report;
}

// === MAIN ===
async function main() {
  const startTime = Date.now();

  // Load data
  const nocoRecords = await loadNocoDB();
  const cwConvs = await loadChatwoot();

  // Build phone map from Chatwoot with multiple key lengths
  const cwByPhone = new Map();
  for (const conv of cwConvs) {
    const phone = conv.meta?.sender?.phone_number || '';
    if (phone) {
      const keys = phoneKeys(phone);
      for (const k of [keys.k8, keys.k9, keys.k10, keys.k11, keys.full]) {
        if (k && k.length >= 8) cwByPhone.set(k, conv);
      }
    }
  }
  process.stderr.write(`Mapa Chatwoot: ${cwByPhone.size} entradas por telefone\n\n`);

  // Cross-reference NocoDB with Chatwoot
  process.stderr.write('=== Cruzando dados NocoDB x Chatwoot ===\n');
  const results = [];
  let matched = 0;

  for (const noco of nocoRecords) {
    const phone = noco.telefone || '';
    const keys = phoneKeys(phone);
    const conv = cwByPhone.get(keys.k9) || cwByPhone.get(keys.k10) || cwByPhone.get(keys.k11) || cwByPhone.get(keys.k8) || cwByPhone.get(keys.full) || null;

    if (conv) matched++;

    const classif = conv ? classifyConversation(conv, noco) : {
      status: noco.status_conversa === 'Aguardando_Resposta' ? 'PENDENTE' :
              (noco.status_conversa || 'PENDENTE').toUpperCase().replace(/\s/g, '_'),
      motivo: conv ? '' : (noco.Disparo ? 'Disparo feito, sem conversa no Chatwoot' : 'Sem disparo realizado'),
      diasParado: noco.data_disparo ? Math.floor((Date.now() - new Date(noco.data_disparo).getTime()) / (1000*60*60*24)) : 999,
      areaHa: 0,
      cultivares: '',
      totalMsgsIn: 0,
      totalMsgsOut: 0,
      lastMsgSnippet: 'N/D',
      lastMsgDate: null,
      conversaAberta: false,
      labels: ''
    };

    // Calculate area from cultivars
    let areaHa = 0;
    const cultivares = [];
    for (const cult of ['Tormenta', 'Olimpo', 'Mítica', 'Sparta', 'Hera', 'C2795', 'C2810']) {
      const val = parseFloat(noco[cult] || 0);
      if (val > 0) {
        areaHa += val;
        cultivares.push(`${cult}:${val}`);
      }
    }
    if (areaHa > 0) {
      classif.areaHa = areaHa;
      classif.cultivares = cultivares.join(', ');
    }

    // Producao estimada
    const prodEstimada = parseFloat(noco['Produção Estimada (tons)'] || 0);

    const result = {
      nome: noco.Cooperado || 'N/D',
      telefone: phone,
      municipio: noco['Município'] || 'N/D',
      estado: noco.Estado || 'N/D',
      fazenda: noco.Fazenda || 'N/D',
      supervisor: noco.Supervisor || 'N/D',
      responsavel: noco['Responsável'] || 'N/D',
      unidade: noco.Unidade || 'N/D',
      ...classif,
      prodEstimada,
      nocoStatus: noco.status_conversa || 'N/D',
      contratoEnviado: noco['contrato enviado'] || false,
      contratoAssinado: noco['contrato assinado'] || false,
      nocoContrato: noco.status_contrato || '',
      chatwootLink: conv ? `https://projetos-chatwoot.0ivxeq.easypanel.host/app/accounts/1/conversations/${conv.id}` : 'N/D',
      chatwootId: conv?.id || null,
      temConversa: !!conv
    };

    result.acao = sugerirAcao(result);
    results.push(result);
  }

  process.stderr.write(`\nCruzamento: ${matched}/${nocoRecords.length} cooperados encontrados no Chatwoot\n`);

  // Generate outputs
  const report = generateReport(results);
  const csv = generateCSV(results);

  // Save files
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').split('T')[0];
  const reportFile = `relatorio_cooperados_${timestamp}.txt`;
  const csvFile = `pendencias_cooperados_${timestamp}.csv`;
  const jsonFile = `dados_cooperados_${timestamp}.json`;

  fs.writeFileSync(reportFile, report, 'utf8');
  fs.writeFileSync(csvFile, '\uFEFF' + csv, 'utf8'); // BOM for Excel/Sheets
  fs.writeFileSync(jsonFile, JSON.stringify(results, null, 2), 'utf8');

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  process.stderr.write(`\n=== CONCLUÍDO em ${elapsed}s ===\n`);
  process.stderr.write(`Relatório: ${reportFile}\n`);
  process.stderr.write(`CSV:       ${csvFile}\n`);
  process.stderr.write(`JSON:      ${jsonFile}\n`);

  // Print report to stdout too
  console.log(report);
}

main().catch(e => {
  console.error('ERRO FATAL:', e);
  process.exit(1);
});
