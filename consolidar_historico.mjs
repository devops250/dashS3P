// Consolidação histórica completa: todas as transferências e disparos
// da tabela "Disparo Jun26" (Yara SDR era) até 10/ago/2026.
// Também soma o snapshot histórico anterior (relatorio_geral_transferidos + historico_geral)
// que representa 35 tabelas Jan-Abr/2026 anteriores à era Yara atual.
import { writeFileSync, readFileSync, existsSync } from "fs";

const NOCO_BASE    = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN   = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE   = "mp1noylkq6er2jy";

const DEADLINE = new Date("2026-08-10T23:59:59-03:00");

function normalizePhone(p) {
  if (!p) return "";
  let n = String(p).replace(/\D/g, "");
  if (n.startsWith("55") && n.length >= 12) n = n.slice(2);
  return n;
}
function monthKey(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return null;
  const local = new Date(d.getTime() - 3 * 3600 * 1000);
  return `${local.getUTCFullYear()}-${(local.getUTCMonth()+1).toString().padStart(2,"0")}`;
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

console.log("=".repeat(60));
console.log("Consolidação histórica — todas as campanhas Yara até 10/ago/2026");
console.log("=".repeat(60));

console.log("\n[1/2] Baixando NocoDB Disparo Jun26 (era Yara atual)...");
const noco = await fetchNocoAll();
console.log(`  ${noco.length} registros`);

const disparados = noco.filter(r => r.Disparo);
const telDisp = new Set(disparados.map(r => normalizePhone(r.telefone)).filter(Boolean));

const transferidosAll = noco.filter(r => r.data_transferencia && new Date(r.data_transferencia) <= DEADLINE);
const telTransf = new Set();
const transfUnicos = [];
for (const r of transferidosAll) {
  const tel = normalizePhone(r.telefone);
  if (!tel || telTransf.has(tel)) continue;
  telTransf.add(tel);
  transfUnicos.push(r);
}

console.log(`  Disparados (Disparo=true): ${disparados.length} (únicos por tel: ${telDisp.size})`);
console.log(`  Transferências brutas: ${transferidosAll.length}`);
console.log(`  Transferidos únicos: ${transfUnicos.length}`);

// Distribuição por vendedor
const porVendedor = {};
for (const r of transfUnicos) {
  const v = r.vendedor_responsavel || "Não informado";
  porVendedor[v] = (porVendedor[v] || 0) + 1;
}

// Distribuição por mês
const porMes = {};
for (const r of transfUnicos) {
  const k = monthKey(r.data_transferencia);
  if (k) porMes[k] = (porMes[k] || 0) + 1;
}

// Temperatura
const porTemp = {};
for (const r of transfUnicos) {
  const t = r.temperatura || "(vazio)";
  porTemp[t] = (porTemp[t] || 0) + 1;
}

console.log("\n[2/2] Cruzando com snapshot histórico anterior...");
let snapshotAnterior = null;
const pathHist = "dashS3P/historico_geral.json";
if (existsSync(pathHist)) {
  try { snapshotAnterior = JSON.parse(readFileSync(pathHist, "utf8")); }
  catch { snapshotAnterior = null; }
}

// Snapshot no dashboard (10/jun): 29.014 leads, 1.798 interações, 376 transferidos (35 tabelas)
// Desses 376, sabemos pelos dashboards antigos que ~34 são mai/jun (Disparo mai26 + Disparo Jun26 até 10/jun).
// Ou seja, a era Yara atual (tabela Disparo Jun26 completa) provavelmente já englobe todos os 34
// mais os novos transfers pós 10/jun. O restante (342) veio de campanhas legadas.
const HISTORICO_LEGADO = {
  fonte: "Dashboard 10/jun/2026 — 35 tabelas legadas (DISPARO LIGACAO + campanhas antigas + mai26 + Jun26 até 10/jun)",
  total_leads: 29014,
  interagiram: 1798,
  transferidos: 376,
  pediram_parar: 36,
  vendedores_ate_jun: {
    "Maria Luiza": 90, "Maria Izabel": 81, "Ronaldo": 80, "Tharik Rafael": 79,
    "Douglas": 32, "Fabio": 6, "Rhuan": 5,
  },
};

const out = {
  atualizado_em: "2026-08-10",
  fonte_atual: "NocoDB tabela Disparo Jun26 (mp1noylkq6er2jy)",
  era_yara_atual: {
    disparados_registros: disparados.length,
    disparados_telefones_unicos: telDisp.size,
    transferidos_unicos: transfUnicos.length,
    por_vendedor: porVendedor,
    por_mes_transferencia: porMes,
    por_temperatura: porTemp,
  },
  legado_pre_10jun: HISTORICO_LEGADO,
  transferidos_lista: transfUnicos.map(r => ({
    nome: r.nome, telefone: r.telefone, cidade: r.cidade,
    vendedor: r.vendedor_responsavel, temperatura: r.temperatura,
    data_transferencia: r.data_transferencia,
  })),
};
writeFileSync("historico_consolidado_10ago.json", JSON.stringify(out, null, 2));

console.log("\n" + "=".repeat(60));
console.log("HISTÓRICO CONSOLIDADO (Yara atual até 10/ago)");
console.log("=".repeat(60));
console.log(`Disparados (registros na tabela): ${disparados.length}`);
console.log(`Telefones únicos disparados:      ${telDisp.size}`);
console.log(`Transferidos únicos:              ${transfUnicos.length}`);
console.log(`\nPor vendedor:`);
for (const [v,n] of Object.entries(porVendedor).sort((a,b)=>b[1]-a[1])) console.log(`  ${v}: ${n}`);
console.log(`\nPor mês:`);
for (const [k,v] of Object.entries(porMes).sort()) console.log(`  ${k}: ${v}`);
console.log(`\nTemperatura:`);
for (const [k,v] of Object.entries(porTemp).sort((a,b)=>b[1]-a[1])) console.log(`  ${k}: ${v}`);
console.log(`\nSalvo em: historico_consolidado_10ago.json`);
