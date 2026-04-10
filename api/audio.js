export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const { url } = req.query;

  if (!url || !url.startsWith('https://storage.vapi.ai/')) {
    return res.status(400).json({ error: 'URL invalida' });
  }

  try {
    const headers = {};
    if (req.headers.range) {
      headers['Range'] = req.headers.range;
    }

    const response = await fetch(url, { headers });
    const buffer = await response.arrayBuffer();
    const buf = Buffer.from(buffer);

    res.setHeader('Content-Type', 'audio/wav');
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.setHeader('Content-Length', buf.length);

    if (response.headers.get('content-range')) {
      res.setHeader('Content-Range', response.headers.get('content-range'));
      return res.status(206).send(buf);
    }

    res.status(200).send(buf);
  } catch (e) {
    return res.status(500).json({ error: 'Erro interno' });
  }
}
