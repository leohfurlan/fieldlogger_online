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
- Catalogo de compostos (`FIELDLOGGER_COMPOSTOS`) e vinculo ao batch
- Vinculo de `composto`, `lote` e `observacoes` para todas as linhas de um `batch_id`

## Requisitos

- Python 3.11+ (recomendado 3.12/3.13)
- Oracle acessivel com credenciais validas
- (Opcional no Windows) Oracle Instant Client para modo thick

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
ORACLE_CLIENT_LIB_DIR=C:\\oracle\\instantclient_23_9
APP_LOG_FILE=app.log
```

Observacao:
- `ORACLE_CLIENT_LIB_DIR` e opcional. Se ausente, o driver tenta modo thin.

## Execucao

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Acesse:
- Monitoramento online: `http://localhost:8000/`
- Historico: `http://localhost:8000/historico`

## Estrutura de banco

Scripts uteis:

- Criar objetos: `python create_table.py`
- Resetar tabelas: `python reset_table.py`
- SQL base: `sql.sql`

Tabelas principais:
- `ENGENHARIA.FIELDLOGGER_MONITOR`
- `ENGENHARIA.FIELDLOGGER_COMPOSTOS`

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

- O host/porta/unit_id do Modbus estao definidos em `main.py`.
- O frontend usa CDN para Tabulator, Chart.js e XLSX.
- Para producao, considere travar versoes, usar proxy reverso e executar sem `--reload`.
