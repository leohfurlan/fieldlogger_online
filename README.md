# fieldlogger_online (app)

Aplicacao FastAPI para monitoramento online de um FieldLogger via Modbus TCP, persistencia no Oracle e analise de ciclos por batch.

## Funcionalidades

- Monitoramento online via WebSocket em `/`
- Historico com grafico + tabela em `/historico`
- Filtros por periodo, lote, composto e batch
- Deteccao automatica de ciclo/batch:
  - `botao_start` (borda 0->1) inicia batch
  - `tampa_descarga` (borda 1->0) encerra batch
  - Regra de tampa usada: `1 = fechada`, `0 = aberta`
- Buffer de leituras com `executemany` (menos carga no Oracle)
- Estado do detector de ciclo persistido em `ENGENHARIA.APP_STATE`
- Catalogo de compostos (`FIELDLOGGER_COMPOSTOS`) e vinculo por batch
- Relatorio automatico por batch (1 clique):
  - PDF (cabecalho, metricas, eventos, grafico)
  - Excel (`Resumo`, `Eventos`, `Leituras` opcional)
- Assinatura de ciclo + comparacao com batch padrao:
  - Reamostragem em tempo normalizado `0..100%` com 101 pontos
  - Hash SHA-256 da serie quantizada
  - Overlay baseline vs target no frontend
  - Metricas de desvio (RMSE, deltas de duracao/AUC/picos)

## Requisitos

- Python 3.11+ (recomendado 3.12/3.13)
- Oracle acessivel com credenciais validas
- Driver `oracledb` em modo auto (tenta thick quando `ORACLE_CLIENT_LIB_DIR` existir)

## Instalacao

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Variaveis de ambiente (`.env`)

Configure no arquivo `.env`:

```env
DB_DSN=SEU_DSN
DB_USER=SEU_USUARIO
DB_PASSWORD=SUA_SENHA
APP_LOG_FILE=app.log
ORACLE_CLIENT_LIB_DIR=C:\\oracle\\instantclient_23_9
ORACLE_MODE=auto

MODBUS_HOST=172.16.30.95
MODBUS_PORT=502
MODBUS_UNIT_ID=255
MODBUS_TIMEOUT_S=5
POLL_SECONDS=1

READINGS_BATCH_SIZE=100
READINGS_FLUSH_INTERVAL_S=2
```

Variaveis opcionais adicionais:

```env
CYCLE_STATE_PERSIST_INTERVAL_S=10
CYCLE_STALE_TIMEOUT_S=7200
CYCLE_SIGNATURE_SAMPLE_EVERY=10
DB_POOL_MIN=1
DB_POOL_MAX=4
DB_POOL_INCREMENT=1
```

## Migracao de banco (DDL)

Aplicar na ordem:

```sql
@sql/001_create_batch_tables.sql
@sql/002_create_state.sql
```

Esses scripts criam:

- `ENGENHARIA.FIELDLOGGER_BATCHES`
- FK de `FIELDLOGGER_MONITOR.BATCH_ID -> FIELDLOGGER_BATCHES.BATCH_ID`
- `ENGENHARIA.APP_STATE`

## Execucao

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Acesse:

- Monitoramento online: `http://localhost:8000/`
- Historico + comparacao: `http://localhost:8000/historico`

## Rotas novas (relatorio e assinatura)

- `GET /api/batches/{batch_id}/report?format=pdf|xlsx&include_raw=0|1`
- `GET /api/batches/{batch_id}/signature`
- `GET /api/batches/compare?baseline_batch_id={id}&target_batch_id={id}`

## Exemplos curl

```bash
# PDF 1-clique
curl -L "http://localhost:8000/api/batches/123/report?format=pdf" -o batch_123.pdf

# Excel 1-clique com leituras cruas
curl -L "http://localhost:8000/api/batches/123/report?format=xlsx&include_raw=1" -o batch_123.xlsx

# Assinatura + perfil normalizado
curl "http://localhost:8000/api/batches/123/signature"

# Comparacao baseline x target
curl "http://localhost:8000/api/batches/compare?baseline_batch_id=120&target_batch_id=123"
```

## Validacao da UI

1. Abra `/historico`.
2. Filtre um `Batch ID` e teste os botoes `Baixar PDF batch` e `Baixar Excel batch`.
3. Em `Comparar ciclos`, selecione `Batch padrao` e `Batch alvo`.
4. Clique `Comparar` e valide:
   - overlay de temperatura e corrente (0-100%)
   - cards de RMSE e deltas
   - linhas verticais de estagios (rampa/plato/descarga), quando inferidas.

## Testes

Testes unitarios minimos:

- detector de ciclo
- reamostragem `0..100%` com 101 pontos
- assinatura igual/diferente

```bash
pytest -q
```

## Consultas Oracle de verificacao

### 1) Assinaturas salvas em batches

```sql
SELECT batch_id, start_ts, end_ts, signature, duration_s
FROM engenharia.fieldlogger_batches
ORDER BY batch_id DESC;
```

### 2) Dados usados no relatorio (meta + metricas base)

```sql
SELECT
  b.batch_id,
  b.op,
  b.operador,
  b.start_ts,
  b.end_ts,
  b.duration_s,
  b.max_temp,
  b.max_corrente,
  b.avg_temp,
  b.avg_corrente
FROM engenharia.fieldlogger_batches b
WHERE b.batch_id = :batch_id;
```

### 3) Leituras do batch para conferir exportacoes

```sql
SELECT
  m.id,
  m.ts,
  m.temperatura_c,
  m.corrente,
  m.botao_start,
  m.tampa_descarga,
  m.batch_id
FROM engenharia.fieldlogger_monitor m
WHERE m.batch_id = :batch_id
ORDER BY m.ts, m.id;
```

## Screenshots (opcional)

Sugestao de capturas para documentacao interna:

- `historico` com botao de download por batch
- secao `Comparar ciclos` com overlay e cards de desvio
- exemplo de PDF exportado

## Scripts de apoio

- Backfill de batch em dados antigos:
  - `python backfill_batch_id.py`
- Importar compostos da Sankhya (grupo 18011100):
  - `python importar_compostos_sankhya.py`
- Vincular composto/lote/observacoes a um batch:
  - `python vincular_composto_batch.py <batch_id> --codprod <CODPROD> --lote <LOTE> --observacoes "<TEXTO>"`
  - ou `--descricao "<DESCRICAO>"`

## Rotas API principais

- `GET /health/db`
- `GET /health/modbus`
- `GET /api/history`
- `GET /api/history/options`
- `GET /api/compostos`
- `POST /api/compostos/import?grupo=18011100`
- `POST /api/batches/{batch_id}/composto?codprod=...&lote=...&observacoes=...`
- `GET /api/batches/{batch_id}/report?format=pdf|xlsx&include_raw=0|1`
- `GET /api/batches/{batch_id}/signature`
- `GET /api/batches/compare?baseline_batch_id=...&target_batch_id=...`

## Observacoes tecnicas

- Rotas antigas e painel atual foram mantidos.
- `energy_proxy` no relatorio/comparacao usa integral trapezoidal da corrente ao longo do tempo (AUC corrente).
- Insercoes de leitura usam `executemany` e commit por lote.
- Em falha Oracle, o buffer de leituras permanece em memoria e tenta novamente no proximo flush.
- O detector persiste estado em eventos de start/end, periodicamente durante ciclo ativo e no shutdown.
