-- Runs once on first container start, as SYSDBA — BEFORE any
-- APP_USER machinery, so the seed creates its own user. 250
-- employees, enough to see paging, types, and the incremental
-- cursor do real work.
ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE USER rdlt IDENTIFIED BY rdlt QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE TO rdlt;

CREATE TABLE rdlt.employees (
    employee_id NUMBER(10)    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    salary      NUMBER(12,2)  NOT NULL,
    hired       DATE          NOT NULL,
    updated_at  TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

-- `updated_at` is SPREAD one minute apart: an incremental cursor
-- over a column where every row shares one value would re-read
-- everything each run, and a fixture should not manufacture that.
INSERT INTO rdlt.employees (name, salary, hired, updated_at)
SELECT 'employee-' || LEVEL,
       ROUND(DBMS_RANDOM.VALUE(30000, 90000), 2),
       DATE '2020-01-01' + MOD(LEVEL * 7, 2000),
       SYSTIMESTAMP - NUMTODSINTERVAL(250 - LEVEL, 'MINUTE')
FROM dual CONNECT BY LEVEL <= 250;
COMMIT;
