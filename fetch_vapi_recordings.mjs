// Busca dados de cada call_id na VAPI e baixa gravações
import { readFileSync, writeFileSync, existsSync, mkdirSync, createWriteStream } from "fs";
import { Readable } from "stream";
import { pipeline } from "stream/promises";

const VAPI_TOKEN = "ea0c3cba-906a-497d-a0eb-d6780d600cec";
const HEADERS = { Authorization: `Bearer ${VAPI_TOKEN}` };

const { calls } = JSON.parse(readFileSync("calls_period.json", "utf8"));
if (!existsSync("audio")) mkdirSync("audio");

// O call_id no calls_period.json foi truncado em 12 chars. Preciso do call_id completo
// → recarrega do relatorio_2024.json
const rel = JSON.parse(readFileSync("relatorio_2024.json", "utf8"));
const fullIdByPhone = {};
for (const r of rel.detalhes.ligacoes) {
  fullIdByPhone[r.telefone] = r.call_id;
}

let fetched = 0, downloaded = 0, skipped = 0, failed = 0;
const out = [];

for (const c of calls) {
  // Skip se ja tem gravacao baixada
  if (c.recording && c.transcript && c.transcript.length) {
    out.push(c);
    skipped++;
    continue;
  }
  const phoneDigits = c.phone.replace(/\D/g, "");
  const fullId = fullIdByPhone[phoneDigits] || fullIdByPhone[phoneDigits.replace(/^55/, "")];
  if (!fullId) {
    console.log(`  SKIP ${c.name} - sem full call_id`);
    out.push(c);
    continue;
  }
  process.stdout.write(`  ${fullId} (${c.name})...`);
  await new Promise(r => setTimeout(r, 1500)); // throttle p/ evitar 429
  try {
    const r = await fetch(`https://api.vapi.ai/call/${fullId}`, { headers: HEADERS });
    if (!r.ok) {
      console.log(` HTTP ${r.status}`);
      failed++;
      out.push(c);
      continue;
    }
    const data = await r.json();
    fetched++;

    const recording = data.recordingUrl || data.artifact?.recordingUrl || data.stereoRecordingUrl;
    const duration = data.startedAt && data.endedAt
      ? Math.round((new Date(data.endedAt) - new Date(data.startedAt)) / 1000)
      : 0;
    const cost = data.cost || 0;
    const transcript = (data.artifact?.messages || data.messages || [])
      .filter(m => m.role === "user" || m.role === "bot" || m.role === "assistant")
      .map(m => ({
        role: m.role === "bot" ? "assistant" : m.role,
        content: m.message || m.content || "",
        time: m.secondsFromStart != null
          ? `${Math.floor(m.secondsFromStart / 60)}:${String(Math.floor(m.secondsFromStart % 60)).padStart(2, "0")}`
          : "",
      }))
      .filter(m => m.content);

    let recPath = "";
    if (recording) {
      const ext = recording.includes(".wav") ? "wav" : "m4a";
      const filename = `${fullId.slice(0, 12)}.${ext}`;
      const filepath = `audio/${filename}`;
      if (!existsSync(filepath)) {
        const ar = await fetch(recording);
        if (ar.ok) {
          await pipeline(Readable.fromWeb(ar.body), createWriteStream(filepath));
          downloaded++;
        }
      } else {
        skipped++;
      }
      recPath = `/audio/${filename}`;
    }

    out.push({
      ...c,
      duration,
      cost,
      recording: recPath,
      transcript,
    });
    console.log(` ok (rec:${recording?'sim':'nao'} dur:${duration}s)`);
  } catch (e) {
    console.log(` ERR ${e.message}`);
    failed++;
    out.push(c);
  }
}

console.log(`\nFetched ${fetched} | Downloaded ${downloaded} | Skipped ${skipped} | Failed ${failed}`);

// Recalcula stats
const stats = {
  total: out.length,
  completed: out.filter(c => c.status === "completed").length,
  voicemail: out.filter(c => c.status === "voicemail").length,
  noAnswer: out.filter(c => c.status === "no-answer").length,
  analyzed: out.filter(c => c.summary && c.summary.length > 30).length,
};

writeFileSync("calls_period.json", JSON.stringify({ stats, calls: out }, null, 2));
console.log("calls_period.json atualizado com gravacoes");
