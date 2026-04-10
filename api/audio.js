export default async function handler(req, res) {
  const { url } = req.query;

  if (!url || !url.startsWith('https://storage.vapi.ai/')) {
    return res.status(400).json({ error: 'URL invalida' });
  }

  try {
    const response = await fetch(url);

    if (!response.ok) {
      return res.status(response.status).json({ error: 'Erro ao buscar audio' });
    }

    const buffer = await response.arrayBuffer();

    res.setHeader('Content-Type', 'audio/wav');
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.send(Buffer.from(buffer));
  } catch (e) {
    return res.status(500).json({ error: 'Erro interno' });
  }
}
