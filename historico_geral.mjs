// Calcula totais do Historico Geral varrendo TODAS as tabelas do projeto S3P
import { writeFileSync } from "fs";

const BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const PROJECT = "picg8cag37aush6";
const HEADERS = { "xc-token": TOKEN };

async function listTables() {
  const r = await fetch(`${BASE}/api/v1/db/meta/projects/${PROJECT}/tables?limit=500`, { headers: HEADERS });
  const d = await r.json();
  return d.list || [];
}

async function fetchAll(tableId) {
  const rows = [];
  let offset = 0;
  while (true) {
    const url = `${BASE}/api/v1/db/data/noco/${PROJECT}/${tableId}?limit=200&offset=${offset}`;
    const r = await fetch(url, { headers: HEADERS });
    const d = await r.json();
    const list = d.list || [];
    rows.push(...list);
    if (list.length < 200) break;
    offset += 200;
  }
  return rows;
}

// Heurística: "interagiu" se tem qualquer marker de resposta/movimentação
function teveInteracao(r) {
  if (r.status && r.status.toLowerCase() !== "novo" && r.status !== "") return true;
  if (r.vendedor_responsavel && String(r.vendedor_responsavel).trim() !== "") return true;
  if (r.temperatura && r.temperatura.toLowerCase() !== "frio") return true;
  if (r.data_transferencia) return true;
  if (r.Atendeu === true) return true;
  if (r.WhatsappEnviado === true) return true;
  return false;
}

const TABELAS_EXCLUIR = /copy|teste|cooperados/i;

async function main() {
  const tables = await listTables();
  console.log(`Total tabelas: ${tables.length}`);

  let totalLeads = 0;
  let totalTransferidos = 0;
  let totalPediramParar = 0;
  let totalInteragiram = 0;
  const porVendedor = {};
  const porTabela = [];

  for (const t of tables) {
    if (TABELAS_EXCLUIR.test(t.title)) continue;
    process.stdout.write(`  ${t.title}...`);
    const rows = await fetchAll(t.id);
    let transf = 0, parar = 0, interagiu = 0;
    for (const r of rows) {
      const status = String(r.status || "").toLowerCase();
      if (status.includes("transferido")) {
        transf++;
        const v = r.vendedor_responsavel || "Nao informado";
        porVendedor[v] = (porVendedor[v] || 0) + 1;
      }
      if (status.includes("parar") || status.includes("descadastr") || status.includes("recusa") || status.includes("opt-out")) parar++;
      if (teveInteracao(r)) interagiu++;
    }
    totalLeads += rows.length;
    totalTransferidos += transf;
    totalPediramParar += parar;
    totalInteragiram += interagiu;
    porTabela.push({ tabela: t.title, leads: rows.length, transferidos: transf, pararam: parar, interagiram: interagiu });
    console.log(` ${rows.length} leads | ${transf} transf | ${parar} parou | ${interagiu} interagiu`);
  }

  const summary = {
    geradoEm: new Date().toISOString(),
    totais: {
      total_leads: totalLeads,
      interagiram: totalInteragiram,
      transferidos_vendedor: totalTransferidos,
      pediram_parar: totalPediramParar,
    },
    por_vendedor: porVendedor,
    por_tabela: porTabela.sort((a, b) => b.leads - a.leads),
  };

  console.log("\n" + "=".repeat(60));
  console.log("HISTORICO GERAL - TOTAIS");
  console.log("=".repeat(60));
  for (const [k, v] of Object.entries(summary.totais)) console.log(`  ${k}: ${v}`);
  console.log("\nPor vendedor:");
  for (const [v, n] of Object.entries(porVendedor)) console.log(`  ${v}: ${n}`);

  writeFileSync("historico_geral.json", JSON.stringify(summary, null, 2));
  console.log("\nSalvo em historico_geral.json");
}

main().catch(e => { console.error(e); process.exit(1); });
