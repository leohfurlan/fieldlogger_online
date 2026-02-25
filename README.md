# fieldlogger_online (app)

Aplicacao FastAPI para monitoramento online de um FieldLogger via Modbus TCP, persistencia no Oracle e consulta de historico com filtros e rastreio por batch.

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
- Tabela `ENGENHARIA.FIELDLOGGER_BATCHES` (1 linha por ciclo)
- Catalogo de compostos (`FIELDLOGGER_COMPOSTOS`) e vinculo ao batch
- Vinculo de `composto`, `lote` e `observacoes` para todas as linhas de um `batch_id`

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
- Historico: `http://localhost:8000/historico`

## Testes

Teste unitario minimo do detector de ciclo:

```bash
pytest -q
```

## Validacao rapida no Oracle

1. Execute a aplicacao e gere um ciclo real (start -> fim).
2. Verifique se batches foram criados e fechados:

```sql
SELECT batch_id, start_ts, end_ts, duration_s, max_temp, avg_temp, signature
FROM engenharia.fieldlogger_batches
ORDER BY batch_id DESC;
```

3. Verifique leituras vinculadas ao ciclo:

```sql
SELECT batch_id, COUNT(*) AS total
FROM engenharia.fieldlogger_monitor
GROUP BY batch_id
ORDER BY batch_id DESC;
```

4. Verifique estado persistido do detector:

```sql
SELECT key, DBMS_LOB.SUBSTR(value, 4000, 1) AS value_json, updated_at
FROM engenharia.app_state
WHERE key = 'cycle_detector_state';
```

## Scripts de apoio

- Backfill de batch em dados antigos:
  - `python backfill_batch_id.py`
- Importar compostos da Sankhya (grupo 18011100):
  - `python importar_compostos_sankhya.py`
- Vincular composto/lote/observacoes a um batch:
  - `python vincular_composto_batch.py <batch_id> --codprod <CODPROD> --lote <LOTE> --observacoes "<TEXTO>"`
  - ou `--descricao "<DESCRICAO>"`

## Principais rotas API

- `GET /health/db`
- `GET /health/modbus`
- `GET /api/history`
- `GET /api/history/options`
- `GET /api/compostos`
- `POST /api/compostos/import?grupo=18011100`
- `POST /api/batches/{batch_id}/composto?codprod=...&lote=...&observacoes=...`

## Observacoes tecnicas

- O frontend atual foi mantido (sem quebra de compatibilidade).
- Insercoes de leitura usam `executemany` e commit por lote.
- Em falha Oracle, o buffer de leituras permanece em memoria e tenta novamente no proximo flush.
- O detector persiste estado em eventos de start/end, periodicamente durante ciclo ativo e no shutdown.
