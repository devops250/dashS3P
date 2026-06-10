// Relatório geral de transferidos - todas as datas, tabela Disparo Jun26
import { writeFileSync } from "fs";

const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";
const NOCO_TABLE = "mp1noylkq6er2jy"; // Disparo Jun26

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
    if (offset > 70000) break;
  }
  return all;
}

console.log("Buscando todos os registros Disparo Jun26...");
const all = await fetchNocoAll();
console.log(`Total: ${all.length}`);

const transferidos = all.filter(r =>
  r.data_transferencia ||
  (r.vendedor_responsavel && String(r.vendedor_responsavel).trim() !== "") ||
  (r.status && r.status.toLowerCase().includes("transferid"))
);
console.log(`Transferidos (algum critério): ${transferidos.length}`);

// Por vendedor
const porVendedor = {};
const porData = {};
const porTemperatura = {};

for (const t of transferidos) {
  const v = t.vendedor_responsavel || "Não informado";
  porVendedor[v] = (porVendedor[v] || 0) + 1;

  if (t.data_transferencia) {
    const d = t.data_transferencia.slice(0, 10); // YYYY-MM-DD
    porData[d] = (porData[d] || 0) + 1;
  }

  const temp = t.temperatura || "(vazio)";
  porTemperatura[temp] = (porTemperatura[temp] || 0) + 1;
}

const detalhes = transferidos
  .sort((a, b) => (a.data_transferencia || "").localeCompare(b.data_transferencia || ""))
  .map(t => ({
    nome: t.nome,
    telefone: t.telefone,
    vendedor: t.vendedor_responsavel,
    data_transferencia: t.data_transferencia,
    status: t.status,
    temperatura: t.temperatura,
    cidade: t.cidade,
    hectares: t.hectares,
    cultivar: t.cultivar,
    resumo_conversa: t.resumo_conversa ? t.resumo_conversa.slice(0, 300) : null,
  }));

const relatorio = {
  fonte: { tabela: "Disparo Jun26 (mp1noylkq6er2jy)", total_registros: all.length },
  totais: {
    transferidos_total: transferidos.length,
    com_data_transferencia: transferidos.filter(t => t.data_transferencia).length,
    sem_data_transferencia: transferidos.filter(t => !t.data_transferencia).length,
  },
  por_vendedor: porVendedor,
  por_temperatura: porTemperatura,
  por_data: Object.fromEntries(Object.entries(porData).sort()),
  detalhes,
};

writeFileSync("relatorio_geral_transferidos.json", JSON.stringify(relatorio, null, 2));

console.log("\n=== TOTAIS ===");
console.log(JSON.stringify(relatorio.totais, null, 2));
console.log("\n=== POR VENDEDOR ===");
const venSorted = Object.entries(porVendedor).sort((a,b)=>b[1]-a[1]);
for (const [v, n] of venSorted) console.log(`  ${v.padEnd(25)} ${n}`);
console.log("\n=== POR TEMPERATURA ===");
for (const [t, n] of Object.entries(porTemperatura).sort((a,b)=>b[1]-a[1])) console.log(`  ${t.padEnd(15)} ${n}`);
console.log("\n=== POR DATA ===");
for (const [d, n] of Object.entries(relatorio.por_data)) console.log(`  ${d}  ${n}`);
console.log("\nArquivo: relatorio_geral_transferidos.json");
