import { readFileSync, writeFileSync } from "fs";

const { stats, calls } = JSON.parse(readFileSync("calls_period.json", "utf8"));
let html = readFileSync("ligacoes.html", "utf8");

// Atualiza CALLS array (linha que começa com "const CALLS=[")
const callsJson = JSON.stringify(calls);
html = html.replace(/const CALLS=\[[\s\S]*?\];/, `const CALLS=${callsJson};`);

// Atualiza cards do topo
html = html.replace(
  /<div class="card green"><div class="label">Total Ligacoes<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card green"><div class="label">Total Ligacoes</div><div class="value">${stats.total}</div><div class="detail">20-24/04 - DISPARO LIGAÇÃO</div></div>`
);
html = html.replace(
  /<div class="card blue"><div class="label">Completadas<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card blue"><div class="label">Completadas</div><div class="value">${stats.completed}</div><div class="detail">${(stats.completed/stats.total*100).toFixed(1)}% conectaram</div></div>`
);
html = html.replace(
  /<div class="card green"><div class="label">Com Analise IA<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card green"><div class="label">Com Analise IA</div><div class="value">${stats.analyzed}</div><div class="detail">conversas analisadas</div></div>`
);
html = html.replace(
  /<div class="card amber"><div class="label">Duracao Media<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card amber"><div class="label">Caixa Postal</div><div class="value">${stats.voicemail}</div><div class="detail">silencio/voicemail</div></div>`
);
html = html.replace(
  /<div class="card purple"><div class="label">Custo Total<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card purple"><div class="label">Periodo</div><div class="value" style="font-size:1.2rem">20-24/04</div><div class="detail">5 dias uteis</div></div>`
);
html = html.replace(
  /<div class="card red"><div class="label">Sem Resposta<\/div><div class="value">[^<]+<\/div><div class="detail">[^<]+<\/div><\/div>/,
  `<div class="card red"><div class="label">Sem Resposta</div><div class="value">${stats.noAnswer}</div><div class="detail">nao atendeu/erro</div></div>`
);

// Atualiza filtros
html = html.replace(/<button class="active" onclick="setFilter\('all'\)">Todas \(\d+\)<\/button>/, `<button class="active" onclick="setFilter('all')">Todas (${stats.total})</button>`);
html = html.replace(/<button onclick="setFilter\('completed'\)">Completadas \(\d+\)<\/button>/, `<button onclick="setFilter('completed')">Completadas (${stats.completed})</button>`);
html = html.replace(/<button onclick="setFilter\('analyzed'\)">Com Analise \(\d+\)<\/button>/, `<button onclick="setFilter('analyzed')">Com Analise (${stats.analyzed})</button>`);
html = html.replace(/<button onclick="setFilter\('voicemail'\)">Caixa Postal \(\d+\)<\/button>/, `<button onclick="setFilter('voicemail')">Caixa Postal (${stats.voicemail})</button>`);
html = html.replace(/<button onclick="setFilter\('no-answer'\)">Sem Resposta \(\d+\)<\/button>/, `<button onclick="setFilter('no-answer')">Sem Resposta (${stats.noAnswer})</button>`);

// Atualiza secao "Progresso da Campanha"
html = html.replace(
  /<div style="font-size:\.8rem;color:#888;margin-top:2px">[^<]*Cron ativo[^<]*<\/div>/,
  `<div style="font-size:.8rem;color:#888;margin-top:2px">Periodo 20-24/04/2026 · Cron ativo seg-sex, 8h-17h · ${stats.total} ligacoes via DISPARO LIGAÇÃO</div>`
);
html = html.replace(/<div style="font-family:'DM Serif Display',serif;font-size:2rem;font-weight:700;color:#1a5632">48%<\/div>/, `<div style="font-family:'DM Serif Display',serif;font-size:2rem;font-weight:700;color:#1a5632">${(stats.completed/stats.total*100).toFixed(0)}%</div>`);
html = html.replace(/width:48\.3%/, `width:${(stats.completed/stats.total*100).toFixed(1)}%`);
html = html.replace(
  /<div><span style="font-size:1\.4rem;font-weight:700;color:#1a5632">\d+<\/span><span style="font-size:\.8rem;color:#888;margin-left:6px">ligacoes realizadas<\/span><\/div>/,
  `<div><span style="font-size:1.4rem;font-weight:700;color:#1a5632">${stats.total}</span><span style="font-size:.8rem;color:#888;margin-left:6px">ligacoes realizadas</span></div>`
);
html = html.replace(
  /<div><span style="font-size:1\.4rem;font-weight:700;color:#d97706">\d+<\/span><span style="font-size:\.8rem;color:#888;margin-left:6px">leads restantes<\/span><\/div>/,
  `<div><span style="font-size:1.4rem;font-weight:700;color:#d97706">${stats.voicemail}</span><span style="font-size:.8rem;color:#888;margin-left:6px">caixa postal</span></div>`
);
html = html.replace(
  /<div><span style="font-size:1\.4rem;font-weight:700;color:#1a56db">\d+<\/span><span style="font-size:\.8rem;color:#888;margin-left:6px">falaram com a Yara<\/span><\/div>/,
  `<div><span style="font-size:1.4rem;font-weight:700;color:#1a56db">${stats.completed}</span><span style="font-size:.8rem;color:#888;margin-left:6px">falaram com a Yara</span></div>`
);
html = html.replace(
  /<div><span style="font-size:1\.4rem;font-weight:700;color:#dc2626">\d+<\/span><span style="font-size:\.8rem;color:#888;margin-left:6px">sem resposta<\/span><\/div>/,
  `<div><span style="font-size:1.4rem;font-weight:700;color:#dc2626">${stats.noAnswer}</span><span style="font-size:.8rem;color:#888;margin-left:6px">sem resposta</span></div>`
);
html = html.replace(/<div style="margin-left:auto"><span style="font-size:\.85rem;color:#888">Previsao:[^<]+<\/span><\/div>/, `<div style="margin-left:auto"><span style="font-size:.85rem;color:#888">Atualizado: 26/04/2026</span></div>`);

// Footer
html = html.replace(/Relatorio gerado automaticamente \| Cognita AI \| 10\/04\/2026/, "Relatorio gerado automaticamente | Cognita AI | 26/04/2026 | Periodo 20-24/04");

writeFileSync("ligacoes.html", html);
console.log("ligacoes.html atualizado");
