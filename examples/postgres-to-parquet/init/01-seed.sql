-- Seeded on FIRST container start (a persisted volume keeps state;
-- `compose down -v` resets). Two related tables so the example can
-- show multiple streams, a numeric cursor, and a join query stream.
CREATE TABLE customers (
    customer_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text        NOT NULL,
    region      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint      NOT NULL REFERENCES customers,
    amount      numeric(12,2) NOT NULL,
    status      text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO customers (name, region, created_at)
SELECT 'customer-' || i,
       (ARRAY['eu','us','apac'])[1 + i % 3],
       now() - make_interval(mins => 200 - i)
FROM generate_series(1, 200) AS i;

-- `updated_at` is SPREAD over time (one minute apart) rather than
-- stamped by one transaction: an incremental cursor over a column
-- where every row shares one value would re-read everything on each
-- run, and a fixture should not manufacture that corner.
INSERT INTO orders (customer_id, amount, status, updated_at)
SELECT 1 + (i % 200),
       round((random() * 900 + 100)::numeric, 2),
       (ARRAY['open','shipped','returned'])[1 + i % 3],
       now() - make_interval(mins => 5000 - i)
FROM generate_series(1, 5000) AS i;
