CREATE EXTENSION tpcds;

SELECT tpcds.gen_schema();

-- Verify 25 data tables exist (exclude extension metadata tables)
SELECT count(*) FROM pg_tables WHERE schemaname = 'tpcds'
  AND tablename NOT IN ('config', 'query', 'bench_results', 'bench_summary');

-- List all data tables
SELECT tablename FROM pg_tables WHERE schemaname = 'tpcds'
  AND tablename NOT IN ('config', 'query', 'bench_results', 'bench_summary')
  ORDER BY tablename;

-- Verify search_path is not clobbered
SHOW search_path;
