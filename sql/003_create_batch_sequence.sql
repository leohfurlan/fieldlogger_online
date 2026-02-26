-- sql/003_create_batch_sequence.sql
-- Sequence oficial para Batch ID (fonte unica de verdade).

CREATE SEQUENCE SEQ_BATCH_ID
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;
