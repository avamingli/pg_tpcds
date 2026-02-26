-- EXPLAIN (COSTS OFF) all 99 queries to verify they parse and plan.
-- Without data the plans hit empty tables — that's fine.
-- We just need every query to plan without error.
SET search_path = tpcds, public;

CREATE TEMP TABLE explain_results (query_id INT, status TEXT);

DO $$
DECLARE
  _qid INTEGER;
BEGIN
  FOR _qid IN 1..99 LOOP
    BEGIN
      PERFORM tpcds.explain(_qid, 'COSTS OFF');
      INSERT INTO explain_results VALUES (_qid, 'OK');
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO explain_results VALUES (_qid, 'ERROR: ' || SQLERRM);
    END;
  END LOOP;
END;
$$;

SELECT * FROM explain_results ORDER BY query_id;
