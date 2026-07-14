# dashS3P

Dashboard e ferramentas de análise para o projeto **Semente 3 Pinheiros (S3P)** — automação de prospecção e SDR via WhatsApp com a agente **Yara**.

O repositório reúne:

- Dashboards estáticos (HTML) publicados na Vercel.
- Scripts Node.js (`.mjs`) para extração, análise e validação de dados de campanhas, ligações e transferências de leads.
- Relatórios em JSON gerados por período, usados como fonte de dados dos dashboards.

## 🌐 Dashboards

| Página | Descrição |
| --- | --- |
| `index.html` | Dashboard principal — visão geral de campanhas, funil, transferências, cooperados e desempenho de vendedores. |
| `ligacoes.html` | Painel dedicado às ligações da Yara (VAPI) com métricas e amostras de gravações. |

Deploy configurado via `vercel.json`.

## 📁 Estrutura do projeto

```
dashS3P/
├── index.html                  # Dashboard principal
├── ligacoes.html               # Dashboard de ligações
├── vercel.json                 # Configuração de deploy
│
├── analise_*.mjs               # Análises por período (funil, transferências, respostas)
├── validar_*.mjs               # Scripts de validação e auditoria (duplicados, transferências)
├── build_calls_period.mjs      # Consolidação de ligações VAPI por período
├── fetch_vapi_recordings.mjs   # Download de gravações VAPI
├── extrair_cooperados.js       # Extração de base de cooperados
├── historico_geral.mjs         # Histórico geral consolidado
├── interagiram_mai_jun.mjs     # Contatos que interagiram por período
├── listar_tabelas_recentes.mjs # Diagnóstico de tabelas NocoDB
├── update_ligacoes_html.mjs    # Atualização automática do HTML de ligações
│
├── relatorio_*.json            # Relatórios processados por período
├── calls_period.json           # Dados agregados de ligações
├── dados_*.json                # Dados brutos de referência
└── audio/                      # Gravações de ligações
```

## 🔌 Fontes de dados

- **NocoDB** — base de leads, transferências, cooperados e campanhas WhatsApp.
- **Chatwoot** (Inbox 18 — *Yara API Oficial*) — conversas, mensagens recebidas e status de atendimento.
- **VAPI** — gravações e metadados das ligações da agente Yara.
- **n8n** — orquestração do workflow SDR Yara (prospecção, qualificação, transferência).

## ⚙️ Como rodar os scripts

Requisitos: Node.js 18+.

```bash
# Executar uma análise de período
node analise_29jun_01jul.mjs

# Validar transferências / duplicados
node validar_24_26jun.mjs
```

Os scripts esperam variáveis de ambiente para acesso às APIs (NocoDB, Chatwoot, VAPI). Não versionar tokens — usar `.env` local.

## 📊 Convenção dos relatórios

Os arquivos `relatorio_<periodo>.json` seguem uma estrutura padrão com:

- `periodo` — intervalo analisado.
- `disparos` — total de mensagens enviadas.
- `respostas` — leads que responderam.
- `transferencias` — leads transferidos para vendedores.
- `conversao` — taxa de conversão calculada.
- `distribuicao_vendedores` — carga por vendedor.

## 🧭 Contexto do projeto

- Cliente: **Semente 3 Pinheiros**.
- Objetivo: acompanhar performance da SDR virtual Yara em campanhas de WhatsApp Business.
- Métrica-chave: taxa de transferência qualificada por período e distribuição por vendedor.

## 📝 Licença

Uso interno — Cognita AI / Semente 3 Pinheiros.
