# TPC-DS PostgreSQL Extension

The `tpcds` extension implements the data generator and queries for the
[TPC-DS benchmark](https://www.tpc.org/tpcds/) Version 4.0.0, and provides an easy way to run all 99 queries in order.

## Install

`make install` compiles the TPC-DS DSGen tools from source, so you need a C toolchain:

```bash
# Debian/Ubuntu
sudo apt-get install build-essential flex bison

# RHEL/Rocky/CentOS/Fedora
sudo dnf install gcc make flex bison byacc
```

Then:

```bash
cd pg_tpcds
make install
```

## Configure PostgreSQL

Default PostgreSQL settings (`shared_buffers = 128MB`, `work_mem = 4MB`, etc.) are far too
conservative for analytical workloads. `gen_pg_conf.py` auto-detects your hardware (CPU,
RAM, disk type) and generates an optimized `tpcds_postgres.conf` with recommended settings
synthesized from [PostgreSQL Wiki](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server)
and [EDB OLAP tuning guide](https://www.enterprisedb.com/postgres-tutorials/trying-many-hats-how-improve-olap-workload-performance-postgresql),
combined with benchmark experience. The output is a starting point — adjust to your workload.

```bash
python3 gen_pg_conf.py              # writes tpcds_postgres.conf
python3 gen_pg_conf.py --dry-run    # preview without writing
```

Apply it and restart:

```bash
echo "include = '$(pwd)/tpcds_postgres.conf'" >> $(psql -tA -c "SHOW config_file")
pg_ctl restart -D $(psql -tA -c "SHOW data_directory")
```

Key parameters tuned: `shared_buffers` (25% RAM), `effective_cache_size` (75% RAM),
`work_mem`, `max_parallel_workers_per_gather`, `random_page_cost` (SSD vs HDD),
`max_wal_size`, `jit`, and more. See the generated file for details.

## Quick Start

### One-shot

```sql
CREATE EXTENSION tpcds;
CALL tpcds.run();  -- SF=1, single-threaded (default)
```

`run()` executes the full pipeline: schema → data generation → load → query generation → benchmark.

```sql
-- For larger scale factors:
SELECT tpcds.config('data_dir', '/data/tpcds_tmp');  -- optional: set data dir (default: /tmp/tpcds_data)
CALL tpcds.run(100, 32);  -- SF=100 (~100 GB), 32 parallel workers
```

### Step by step

```sql
CREATE EXTENSION tpcds;
SELECT tpcds.gen_schema();         -- 1. create 25 TPC-DS tables
SELECT tpcds.gen_data(1, 8);       -- 2. generate SF-1 (~1 GB) .dat files, 8 parallel workers
SELECT tpcds.load_data(8);         -- 3. load .dat files into tables, 8 parallel workers
SELECT tpcds.gen_query();          -- 4. generate 99 queries (scale auto-detected from gen_data)
SELECT tpcds.bench();              -- 5. run all 99 queries
SELECT tpcds.clean_data();         -- 6. (optional) delete .dat files to free disk space
```

Check the latest results:

```sql
SELECT * FROM tpcds.bench_summary;
```

Built and tested on **PostgreSQL 19devel**. Older versions should also work. If not, please create an issue.

## Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `tpcds.config(key)` | TEXT | Get config value |
| `tpcds.config(key, value)` | TEXT | Set config value |
| `tpcds.info()` | TABLE | Show all resolved paths and scale factor |
| `tpcds.run(scale, parallel=1)` | — | Full pipeline: gen_schema → gen_data → load_data → gen_query → bench |
| `tpcds.gen_schema()` | TEXT | Create 25 TPC-DS tables under `tpcds` schema |
| `tpcds.gen_data(scale, parallel=1)` | TEXT | Generate .dat files via dsdgen |
| `tpcds.load_data(workers=4)` | TEXT | Load .dat files into tables and analyze |
| `tpcds.clean_data()` | TEXT | Delete .dat files from data_dir to free disk space |
| `tpcds.gen_query(scale=auto)` | TEXT | Generate 99 queries; scale auto-detected from `gen_data`, default 1 |
| `tpcds.show(qid)` | TEXT | Return query text |
| `tpcds.exec(qid)` | TEXT | Execute one query, save result to `tpcds.bench_results` |
| `tpcds.bench(mode)` | TEXT | Run or explain all 99 queries, update `bench_summary` |
| `tpcds.explain(qid, opts)` | SETOF TEXT | EXPLAIN a single query |

### run(scale, parallel)

Runs the complete benchmark pipeline in one call.

```sql
CALL tpcds.run();       -- SF=1, single-threaded
CALL tpcds.run(100, 32);  -- SF=100, 32 dsdgen workers
```

`parallel` controls both data generation (dsdgen workers) and loading (table-COPY workers, capped
internally at `LEAST(parallel, 16)` since TPC-DS has only 25 tables).

Official certifiable scale factors: 1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000.
Other values work but dsdgen warns they are not valid for result publication.

### show(qid)

Show the query 1's text.
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

See the plan of query 1.
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

Run all 99 queries and record results.

```sql
SELECT tpcds.bench();                               -- execute all 99 queries
SELECT tpcds.bench('EXPLAIN');                      -- explain all 99 queries
SELECT tpcds.bench('EXPLAIN (ANALYZE, COSTS OFF)'); -- explain with options
```

Output saved to `results_dir`:
- Per-query: `query1.out` ... `query99.out` (or `query1_explain.out` ... `query99_explain.out`)
- Summary: `summary.csv` — query_id, status, duration_ms, rows_returned

`tpcds.bench_summary` is updated after each run with the latest results.

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
| `data_dir` | Temporary `.dat` files from `gen_data()` (default: `/tmp/tpcds_data`) |

Check all resolved paths:

```sql
SELECT * FROM tpcds.info();
```

## Configuration

Everything works out of the box. All directories except `data_dir` are auto-detected under the extension install path. Optional overrides:

```sql
SELECT tpcds.config('data_dir', '/data/tpcds');
SELECT tpcds.config('results_dir', '/data/results');
SELECT tpcds.config('query_dir', '/data/queries');
```

> **Disk space warning:** `gen_data()` writes raw `.dat` files to `data_dir` before loading them into
> PostgreSQL. The `.dat` files are roughly the same size as the loaded data (~1 GB per scale
> factor). Make sure `data_dir` has enough free space — at least **2× the scale factor in GB** to
> account for both the `.dat` files and the database storage. The default `data_dir` is
> `/tmp/tpcds_data`, which may be too small for large scale factors. Set it to a partition with
> sufficient space before running `gen_data()`:
>
> ```sql
> SELECT tpcds.config('data_dir', '/data/tpcds_tmp');
> SELECT tpcds.gen_data(100, 8);  -- SF=100 needs ~100 GB in data_dir
> ```

## PostgreSQL Compatibility Fixes

`gen_query()` automatically patches raw `dsqgen` output:

1. **Date intervals** — `+ 14 days` → `+ interval '14 days'`
2. **Column name** (query 30) — `c_last_review_date_sk` → `c_last_review_date`
3. **GROUPING alias** (queries 36, 70, 86) — expands `lochierarchy` to full `grouping()` expression
4. **Division by zero** (query 90) — wraps denominator in `nullif(..., 0)`
