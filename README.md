# TPC-DS PostgreSQL Extension

The `tpcds` extension implements the data generator and queries for the
[TPC-DS benchmark](https://www.tpc.org/tpcds/) Version 4.0.0, and provides an easy way to run all 99 queries in order.

## Install

`make install` compiles the TPC-DS DSGen tools from source, so you need a C toolchain:

```bash
# Debian/Ubuntu
sudo apt-get install build-essential flex bison

# RHEL/Rocky/CentOS/Fedora
sudo dnf install gcc make flex bison
```

Then:

```bash
cd pg_tpcds
make install
```

## Quick Start

```sql
CREATE EXTENSION tpcds;       -- 1. install the extension
SELECT tpcds.gen_schema();    -- 2. create 25 TPC-DS tables
SELECT tpcds.gen_data(1);     -- 3. generate & load SF-1 (~1GB) data (auto-analyzes, cleans up .dat files)
SELECT tpcds.gen_query();     -- 4. generate 99 queries, saved to query_dir as .sql files
SELECT tpcds.bench();         -- 5. run all 99 queries, results + summary.csv in results_dir
```

That's it. Schema, data, queries, benchmark — done.

Check the latest results:

```sql
SELECT * FROM tpcds.bench_summary;
```

Built and tested on **PostgreSQL 19devel**. Older versions should also work. If not, please create an issue.

## Run the Benchmark

```sql
SELECT tpcds.bench();                          -- run all 99 queries
SELECT tpcds.bench('EXPLAIN');                  -- explain all 99 queries
SELECT tpcds.bench('EXPLAIN (ANALYZE, COSTS OFF)'); -- explain with options
```

Per-query output is written to `results_dir` (`queryXX.out` or `queryXX_explain.out`), plus a `summary.csv` with timing for all 99 queries. The `tpcds.bench_summary` table is updated after each run.

## Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `tpcds.info()` | TABLE | Show all resolved paths and scale factor |
| `tpcds.gen_schema()` | TEXT | Create 25 TPC-DS tables under `tpcds` schema |
| `tpcds.gen_data(scale)` | TEXT | Generate data, load, and analyze all tables |
| `tpcds.gen_query(seed)` | TEXT | Generate 99 queries, store in `tpcds.query` table and `query_dir` |
| `tpcds.show(qid)` | TEXT | Return query text |
| `tpcds.exec(qid)` | TEXT | Execute one query, save result to `tpcds.bench_results` |
| `tpcds.bench(mode)` | TEXT | Run or explain all 99 queries, update `bench_summary` |
| `tpcds.explain(qid, opts)` | SETOF TEXT | EXPLAIN a single query |

### show(qid)

Show the query1's text.
```sql
SELECT tpcds.show(1);
                              show
-----------------------------------------------------------------

 with customer_total_return as
 (select sr_customer_sk as ctr_customer_sk
 ,sr_store_sk as ctr_store_sk
 ,sum(SR_FEE) as ctr_total_return
 from store_returns
 ,date_dim
 where sr_returned_date_sk = d_date_sk
 and d_year =2000
 group by sr_customer_sk
 ,sr_store_sk)
  select  c_customer_id
 from customer_total_return ctr1
 ,store
 ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 from customer_total_return ctr2
 where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
 and s_store_sk = ctr1.ctr_store_sk
 and s_state = 'TN'
 and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id
 limit 100;
```

### explain(qid, opts)

See the plan of query1.
```sql
SELECT * FROM tpcds.explain(1, 'COSTS OFF');
                          explain
-------------------------------------------------------------------------------------------
 Limit
   CTE customer_total_return
     ->  GroupAggregate
           Group Key: store_returns.sr_customer_sk, store_returns.sr_store_sk
           ->  Sort
                 Sort Key: store_returns.sr_customer_sk, store_returns.sr_store_sk
                 ->  Hash Join
                       Hash Cond: (store_returns.sr_returned_date_sk = date_dim.d_date_sk)
                       ->  Seq Scan on store_returns
                       ->  Hash
                             ->  Seq Scan on date_dim
                                   Filter: (d_year = 2000)
   ->  Sort
         Sort Key: customer.c_customer_id
         ->  Nested Loop
               ->  Nested Loop
                     Join Filter: (store.s_store_sk = ctr1.ctr_store_sk)
                     ->  CTE Scan on customer_total_return ctr1
                           Filter: (ctr_total_return > (SubPlan expr_1))
                           SubPlan expr_1
                             ->  Aggregate
                                   ->  CTE Scan on customer_total_return ctr2
                                         Filter: (ctr1.ctr_store_sk = ctr_store_sk)
                     ->  Seq Scan on store
                           Filter: (s_state = 'TN'::bpchar)
               ->  Index Scan using customer_pkey on customer
                     Index Cond: (c_customer_sk = ctr1.ctr_customer_sk)
(27 rows)
```

### bench(mode)

```sql
SELECT tpcds.bench();                          -- execute
SELECT tpcds.bench('EXPLAIN');                  -- explain
SELECT tpcds.bench('EXPLAIN (ANALYZE, COSTS OFF)'); -- explain with options
```

Output saved to `results_dir`:
- Per-query: `query1.out` ... `query99.out` (or `query1_explain.out` ... `query99_explain.out`)
- Summary: `summary.csv` — query_id, status, duration_ms, rows_returned

## Where Things Are Stored

### Tables (all under `tpcds` schema)

| Table | Populated by | Description |
|-------|-------------|-------------|
| `tpcds.config` | `CREATE EXTENSION` | Configuration |
| `tpcds.query` | `gen_query()` | 99 generated query texts |
| `tpcds.bench_summary` | `bench()` | Latest run: query_id, status, duration_ms, rows_returned (updated each run) |
| `tpcds.bench_results` | `exec()` / `bench()` | All historical results (appended each run) |
| 25 data tables | `gen_schema()` + `gen_data()` | `store_sales`, `customer`, `item`, `date_dim`, etc. |

### Directories (auto-detected under extension install path)

| Directory | Contents |
|-----------|----------|
| `query_dir` | `query1.sql` ... `query99.sql` from `gen_query()` |
| `results_dir` | Per-query `.out` files and `summary.csv` from `bench()` |
| `data_dir` | Temporary `.dat` files from `gen_data()`, cleaned up after load (default: `/tmp/tpcds_data`) |

Check all resolved paths:

```sql
SELECT * FROM tpcds.info();
```

## Configuration

Everything works out of the box. All directories except `data_dir` are auto-detected under the extension install path. Optional overrides:

```sql
UPDATE tpcds.config SET value = '/data/tpcds' WHERE key = 'data_dir';
UPDATE tpcds.config SET value = '/data/results' WHERE key = 'results_dir';
UPDATE tpcds.config SET value = '/data/queries' WHERE key = 'query_dir';
```

## PostgreSQL Compatibility Fixes

`gen_query()` automatically patches raw `dsqgen` output:

1. **Date intervals** — `+ 14 days` &rarr; `+ interval '14 days'`
2. **Column name** (query 30) — `c_last_review_date_sk` &rarr; `c_last_review_date`
3. **GROUPING alias** (queries 36, 70, 86) — expands `lochierarchy` to full `grouping()` expression
4. **Division by zero** (query 90) — wraps denominator in `nullif(..., 0)`
