// Computa dados agregados Maio/Junho 2026 - apenas Disparo mai26 + Disparo Jun26
import { writeFileSync } from "fs";

const BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const PROJECT = "picg8cag37aush6";
const HEADERS = { "xc-token": TOKEN };

const TABLES = [
  { id: "mhsizs8nybch0be", title: "Disparo mai26" },
  { id: "mp1noylkq6er2jy", title: "Disparo Jun26" },
];

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
    if (offset > 100000) break;
  }
  return rows;
}

function teveInteracao(r) {
  if (r.status && r.status.toLowerCase() !== "novo" && r.status !== "") return true;
  if (r.vendedor_responsavel && String(r.vendedor_responsavel).trim() !== "") return true;
  if (r.temperatura && r.temperatura.toLowerCase() !== "frio" && r.temperatura !== "") return true;
  if (r.data_transferencia) return true;
  if (r.Mensagem_Lead === true) return true;
  return false;
}

let totalLeads = 0, totalInter = 0, totalTransf = 0, totalParar = 0;
const porVendedor = {};
const porTabela = [];
const transferidosDetalhe = [];

for (const t of TABLES) {
  process.stdout.write(`Lendo ${t.title}...`);
  const rows = await fetchAll(t.id);
  let dispar = 0, transf = 0, parar = 0, inter = 0;
  for (const r of rows) {
    if (r.Disparo === true) dispar++;
    const status = String(r.status || "").toLowerCase();
    if (status.includes("transferid") || (r.vendedor_responsavel && r.data_transferencia)) {
      transf++;
      const v = r.vendedor_responsavel || "Nao informado";
      porVendedor[v] = (porVendedor[v] || 0) + 1;
      transferidosDetalhe.push({
        vendedor: r.vendedor_responsavel || "",
        nome: r.nome || "",
        telefone: String(r.telefone || "").replace(/^\+/, ""),
        cidade: r.cidade || "",
        uf: r.uf || "",
        temperatura: r.temperatura || "",
        hectares: r.hectares || "",
        cultivar: r.cultivar || "",
        porte: r.porte || "",
        dataTransf: r.data_transferencia || "",
        origem: t.title,
      });
    }
    if (status.includes("parar") || status.includes("descadastr") || status.includes("opt-out")) parar++;
    if (teveInteracao(r)) inter++;
  }
  totalLeads += dispar;  // contar só os disparados
  totalInter += inter;
  totalTransf += transf;
  totalParar += parar;
  porTabela.push({ tabela: t.title, leads: dispar, transferidos: transf, pararam: parar, interagiram: inter });
  console.log(` ${rows.length} regs | ${dispar} disparados | ${transf} transf | ${inter} interagiu | ${parar} parou`);
}

transferidosDetalhe.sort((a, b) => (a.dataTransf || "").localeCompare(b.dataTransf || ""));

const out = {
  geradoEm: new Date().toISOString(),
  escopo: "Maio + Junho 2026 (Disparo mai26 + Disparo Jun26)",
  totais: {
    total_leads_disparados: totalLeads,
    interagiram: totalInter,
    transferidos_vendedor: totalTransf,
    pediram_parar: totalParar,
  },
  por_vendedor: porVendedor,
  por_tabela: porTabela,
  transferidos_detalhe: transferidosDetalhe,
};

writeFileSync("dados_maio_junho.json", JSON.stringify(out, null, 2));

console.log("\n=== TOTAIS Maio/Junho ===");
console.log(JSON.stringify(out.totais, null, 2));
console.log("\n=== POR VENDEDOR ===");
for (const [v, n] of Object.entries(porVendedor).sort((a,b)=>b[1]-a[1])) console.log(`  ${v.padEnd(20)} ${n}`);
console.log("\n=== POR TABELA ===");
for (const t of porTabela) console.log(`  ${t.tabela.padEnd(20)} leads:${t.leads} transf:${t.transferidos} inter:${t.interagiram}`);
console.log(`\nTransferidos detalhe: ${transferidosDetalhe.length} | Salvo em dados_maio_junho.json`);
