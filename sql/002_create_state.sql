-- sql/002_create_state.sql
-- Estado persistente de aplicacao (detector de ciclo).

BEGIN
  EXECUTE IMMEDIATE q'[
    CREATE TABLE ENGENHARIA.APP_STATE (
      KEY         VARCHAR2(64) PRIMARY KEY,
      VALUE       CLOB,
      UPDATED_AT  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    )
  ]';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
