// Gera array CALLS para ligacoes.html a partir de relatorio_2024.json
import { readFileSync, writeFileSync } from "fs";

const rel = JSON.parse(readFileSync("relatorio_2024.json", "utf8"));
const ligacoes = rel.detalhes.ligacoes;

const MONTHS = { janeiro:1, fevereiro:2, marco:3, "março":3, abril:4, maio:5, junho:6, julho:7, agosto:8, setembro:9, outubro:10, novembro:11, dezembro:12 };

function parseHoraLigacao(s) {
  if (!s) return null;
  // ex: "quarta-feira, 22 de abril de 2026, 12:48"
  const m = s.match(/(\d{1,2})\s+de\s+([a-zçã]+)\s+de\s+(\d{4}),?\s+(\d{1,2}):(\d{2})/i);
  if (!m) return null;
  const [, d, mn, y, hh, mm] = m;
  const month = MONTHS[mn.toLowerCase()] || 4;
  const iso = new Date(Date.UTC(+y, month - 1, +d, +hh + 3, +mm)).toISOString();
  return iso;
}

function deriveStatus(r) {
  if (r.atendeu === true) return "completed";
  const er = (r.end_reason || "").toLowerCase();
  if (er.includes("voicemail") || er.includes("silence")) return "voicemail";
  if (er.includes("no-answer") || er.includes("did-not-answer") || er.includes("failed-to-connect")) return "no-answer";
  if (er.includes("error") || er.includes("twilio-failed")) return "no-answer";
  return "no-answer";
}

const calls = ligacoes.map(r => {
  const status = deriveStatus(r);
  const phone = r.telefone?.startsWith("+") ? r.telefone : `+${r.telefone}`;
  return {
    id: (r.call_id || "").slice(0, 12) || `noid-${Math.random().toString(36).slice(2,8)}`,
    name: r.nome || "",
    phone,
    status,
    duration: 0, // NocoDB não tem duration
    createdAt: parseHoraLigacao(r.hora_ligacao) || "",
    endedReason: r.end_reason || "",
    cost: 0,
    summary: r.resumo || "",
    success: r.qualificado === "true" ? "true" : (r.qualificado === "false" ? "false" : ""),
    recording: "",
    transcript: [],
    temperatura: r.temperatura || "",
    whatsapp: r.whatsapp_enviado === true,
  };
});

// ordena por createdAt desc
calls.sort((a, b) => (b.createdAt || "").localeCompare(a.createdAt || ""));

// Stats
const total = calls.length;
const completed = calls.filter(c => c.status === "completed").length;
const voicemail = calls.filter(c => c.status === "voicemail").length;
const noAnswer = calls.filter(c => c.status === "no-answer").length;
const analyzed = calls.filter(c => c.summary && c.summary.length > 30).length;

const stats = { total, completed, voicemail, noAnswer, analyzed };
console.log("Stats:", stats);
writeFileSync("calls_period.json", JSON.stringify({ stats, calls }, null, 2));
console.log("Saved calls_period.json");
