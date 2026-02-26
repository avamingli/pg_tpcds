SELECT tpcds.gen_query();

-- Verify 99 queries generated
SELECT count(*) FROM tpcds.query;

-- Spot check: queries exist and are non-empty
SELECT query_id, length(query_text) > 0 AS has_text
  FROM tpcds.query WHERE query_id IN (1, 50, 99) ORDER BY query_id;

-- show() works
SELECT length(tpcds.show(1)) > 0;
