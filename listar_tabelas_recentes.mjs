const NOCO_BASE = "https://projetos-nocodb.0ivxeq.easypanel.host";
const NOCO_TOKEN = "mRUak5Md_uigXI8i9lVCutOymsfMT8q3t7mkBC6R";
const NOCO_PROJECT = "picg8cag37aush6";

const r = await fetch(`${NOCO_BASE}/api/v1/db/meta/projects/${NOCO_PROJECT}/tables`, {
  headers: { "xc-token": NOCO_TOKEN }
});
const d = await r.json();
const tabs = (d.list || []).map(t => ({
  id: t.id, title: t.title, updated: t.updated_at, created: t.created_at
}));
tabs.sort((a,b) => (b.updated || "").localeCompare(a.updated || ""));
console.log("Tabelas (top 15 por updated_at):");
for (const t of tabs.slice(0, 15)) {
  console.log(`  ${t.updated} | ${t.id} | ${t.title}`);
}
