-- TPC-DS PostgreSQL Extension v2.0
-- Config-driven, dynamic query generation, clean function names

-- =============================================================================
-- Config table
-- =============================================================================
CREATE TABLE tpcds.config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO tpcds.config (key, value) VALUES
    ('tpcds_dir', ''),
    ('data_dir', '/tmp/tpcds_data'),
    ('query_dir', ''),
    ('results_dir', '');

-- =============================================================================
-- Queries table — populated by gen_query()
-- =============================================================================
CREATE TABLE tpcds.query (
    query_id   INTEGER PRIMARY KEY,
    query_text TEXT NOT NULL
);

-- =============================================================================
-- Benchmark results table (historical, appended each run)
-- =============================================================================
CREATE TABLE tpcds.bench_results (
    id            SERIAL PRIMARY KEY,
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_id      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL
);

-- =============================================================================
-- Benchmark summary table (latest run only, updated each bench())
-- =============================================================================
CREATE TABLE tpcds.bench_summary (
    query_id      INTEGER PRIMARY KEY,
    status        TEXT NOT NULL,
    duration_ms   NUMERIC NOT NULL,
    rows_returned BIGINT NOT NULL,
    run_ts        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- _get_config(key) — read config value, raise if not set
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._get_config(cfg_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    IF _val IS NULL OR _val = '' THEN
        RAISE EXCEPTION 'tpcds.config key "%" is not set. Run: UPDATE tpcds.config SET value = ''...'' WHERE key = ''%''',
            cfg_key, cfg_key;
    END IF;
    RETURN _val;
END;
$func$;

-- =============================================================================
-- _resolve_dir(cfg_key, default_subdir) — config override or auto-detect
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._resolve_dir(cfg_key TEXT, default_subdir TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
    _sharedir TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    IF _val IS NOT NULL AND _val <> '' THEN
        RETURN _val;
    END IF;
    SELECT setting INTO _sharedir FROM pg_config() WHERE name = 'SHAREDIR';
    RETURN _sharedir || '/extension/' || default_subdir;
END;
$func$;

-- =============================================================================
-- config(key) — get config value
-- config(key, value) — set config value (upsert)
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.config(cfg_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _val TEXT;
BEGIN
    SELECT value INTO _val FROM tpcds.config WHERE key = cfg_key;
    RETURN _val;
END;
$func$;

CREATE OR REPLACE FUNCTION tpcds.config(cfg_key TEXT, cfg_value TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
BEGIN
    UPDATE tpcds.config SET value = cfg_value WHERE key = cfg_key;
    IF NOT FOUND THEN
        INSERT INTO tpcds.config (key, value) VALUES (cfg_key, cfg_value);
    END IF;
    RETURN cfg_value;
END;
$func$;

-- =============================================================================
-- info() — show resolved configuration
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.info()
RETURNS TABLE(key TEXT, value TEXT)
LANGUAGE plpgsql
AS $func$
BEGIN
    RETURN QUERY
    SELECT 'tpcds_dir'::TEXT,    tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen')
    UNION ALL
    SELECT 'data_dir',
           COALESCE(NULLIF((SELECT c.value FROM tpcds.config c WHERE c.key = 'data_dir'), ''), '/tmp/tpcds_data')
    UNION ALL
    SELECT 'query_dir', tpcds._resolve_dir('query_dir', 'tpcds_query')
    UNION ALL
    SELECT 'results_dir', tpcds._resolve_dir('results_dir', 'tpcds_results')
    UNION ALL
    SELECT 'scale_factor',
           COALESCE((SELECT c.value FROM tpcds.config c WHERE c.key = 'scale_factor'), '(not set)');
END;
$func$;

-- =============================================================================
-- _fix_query(qid, sql) — apply PostgreSQL compatibility fixes to dsqgen output
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds._fix_query(qid INTEGER, raw_sql TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT := raw_sql;
BEGIN
    -- Fix 1: Date intervals — "+ 14 days" → "+ interval '14 days'"
    _sql := regexp_replace(_sql,
        '([+-])\s*(\d+)\s+(days?|months?|years?)',
        E'\\1 interval ''\\2 \\3''', 'gi');

    -- Fix 2: c_last_review_date (query 30) — dsqgen uses wrong column name
    IF qid = 30 THEN
        _sql := replace(_sql, 'c_last_review_date_sk', 'c_last_review_date');
    END IF;

    -- Fix 3: GROUPING alias (queries 36, 70, 86) — expand lochierarchy references
    IF qid IN (36, 86) THEN
        _sql := replace(_sql, 'as lochierarchy', 'as __LOCHIER__');
        _sql := replace(_sql, 'lochierarchy', 'grouping(i_category)+grouping(i_class)');
        _sql := replace(_sql, '__LOCHIER__', 'lochierarchy');
    END IF;
    IF qid = 70 THEN
        _sql := replace(_sql, 'as lochierarchy', 'as __LOCHIER__');
        _sql := replace(_sql, 'lochierarchy', 'grouping(s_state)+grouping(s_county)');
        _sql := replace(_sql, '__LOCHIER__', 'lochierarchy');
    END IF;

    -- Fix 4: Div-by-zero (query 90) — wrap pmc cast in nullif
    IF qid = 90 THEN
        _sql := regexp_replace(_sql,
            '/(cast\s*\(\s*pmc\s+as\s+decimal\s*\([^)]+\)\s*\))',
            '/nullif(\1,0)', 'i');
    END IF;

    RETURN _sql;
END;
$func$;

-- =============================================================================
-- gen_schema() — create 25 TPC-DS tables (embedded DDL)
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_schema()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tbl TEXT;
    _tables TEXT[] := ARRAY[
        'dbgen_version','customer_address','customer_demographics','date_dim',
        'warehouse','ship_mode','time_dim','reason','income_band','item',
        'store','call_center','customer','web_site','store_returns',
        'household_demographics','web_page','promotion','catalog_page',
        'inventory','catalog_returns','web_returns','web_sales',
        'catalog_sales','store_sales'
    ];
BEGIN
    SET LOCAL client_min_messages = warning;
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('DROP TABLE IF EXISTS tpcds.%I CASCADE', _tbl);
    END LOOP;
    RESET client_min_messages;

    EXECUTE $ddl$
create table tpcds.dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            time                          ,
    dv_cmdline_args           varchar(200)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      ,
    primary key (ca_address_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                  not null,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       ,
    primary key (d_date_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      ,
    primary key (sm_ship_mode_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer               not null,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      ,
    primary key (t_time_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.reason
(
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     ,
    primary key (r_reason_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.item
(
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      ,
    primary key (i_item_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.store
(
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      ,
    primary key (c_customer_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.web_site
(
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       ,
    primary key (p_promo_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
);
    $ddl$;

    EXECUTE $ddl$
create table tpcds.store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);
    $ddl$;

    RETURN 'Created 25 TPC-DS tables in tpcds schema';
END;
$func$;

-- =============================================================================
-- gen_data(scale, parallel) — generate .dat files via dsdgen binary
-- Does NOT load into tables or delete files. Use load_data() afterwards.
-- parallel defaults to 1 (sequential). Set parallel > 1 to run that many
-- dsdgen workers simultaneously, which can dramatically cut wall-clock time.
-- Example:
--   SELECT tpcds.gen_data(10, 8);   -- generate SF=10 with 8 parallel workers
--   SELECT tpcds.load_data();       -- load into tables
--   SELECT tpcds.clean_data();      -- free disk space when done
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_data(scale_factor INTEGER, parallel INTEGER DEFAULT 1)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpcds_dir TEXT;
    _data_dir TEXT;
    _start_ts TIMESTAMPTZ;
    _gen_cmd TEXT;
BEGIN
    IF parallel < 1 THEN
        RAISE EXCEPTION 'parallel must be >= 1';
    END IF;

    _tpcds_dir := tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen');

    SELECT value INTO _data_dir FROM tpcds.config WHERE key = 'data_dir';
    IF _data_dir IS NULL OR _data_dir = '' THEN
        _data_dir := '/tmp/tpcds_data';
    END IF;

    -- Create data directory
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _data_dir);

    -- Generate data using dsdgen binary
    -- With parallel > 1: launch N workers via xargs -P, each handling one chunk.
    -- Output files are named <table>_<child>_<parallel>.dat per dsdgen convention.
    _start_ts := clock_timestamp();
    IF parallel = 1 THEN
        _gen_cmd := format('cd %s/tools && ./dsdgen -scale %s -dir %s -force',
                           _tpcds_dir, scale_factor, _data_dir);
    ELSE
        _gen_cmd := format(
            'cd %s/tools && seq 1 %s | xargs -P %s -I{} ./dsdgen -scale %s -dir %s -force -parallel %s -child {}',
            _tpcds_dir, parallel, parallel, scale_factor, _data_dir, parallel);
    END IF;
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', _gen_cmd);
    RAISE NOTICE 'Data generation completed in % seconds',
        extract(epoch from clock_timestamp() - _start_ts);

    -- Save scale_factor and parallel so load_data()/gen_query() can pick them up
    UPDATE tpcds.config SET value = scale_factor::TEXT WHERE key = 'scale_factor';
    IF NOT FOUND THEN
        INSERT INTO tpcds.config (key, value) VALUES ('scale_factor', scale_factor::TEXT);
    END IF;
    UPDATE tpcds.config SET value = parallel::TEXT WHERE key = 'parallel';
    IF NOT FOUND THEN
        INSERT INTO tpcds.config (key, value) VALUES ('parallel', parallel::TEXT);
    END IF;

    RETURN format('Generated .dat files at SF=%s (parallel=%s) in %s', scale_factor, parallel, _data_dir);
END;
$func$;

-- =============================================================================
-- load_data() — load .dat files from data_dir into tables, then analyze
-- Reads parallel from config (saved by gen_data) to determine file naming.
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.load_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _data_dir TEXT;
    _parallel INTEGER;
    _tbl TEXT;
    _tables TEXT[] := ARRAY[
        'dbgen_version','customer_address','customer_demographics','date_dim',
        'warehouse','ship_mode','time_dim','reason','income_band','item',
        'store','call_center','customer','web_site','store_returns',
        'household_demographics','web_page','promotion','catalog_page',
        'inventory','catalog_returns','web_returns','web_sales',
        'catalog_sales','store_sales'
    ];
    _start_ts TIMESTAMPTZ;
    _row_count BIGINT;
    _total_rows BIGINT := 0;
    _load_cmd TEXT;
BEGIN
    SELECT value INTO _data_dir FROM tpcds.config WHERE key = 'data_dir';
    IF _data_dir IS NULL OR _data_dir = '' THEN
        _data_dir := '/tmp/tpcds_data';
    END IF;

    SELECT value::INTEGER INTO _parallel FROM tpcds.config WHERE key = 'parallel';
    IF _parallel IS NULL THEN
        _parallel := 1;
    END IF;

    -- Truncate all tables before loading
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('TRUNCATE tpcds.%I CASCADE', _tbl);
    END LOOP;

    -- Load each table via COPY FROM PROGRAM (strip trailing pipe)
    FOREACH _tbl IN ARRAY _tables LOOP
        _start_ts := clock_timestamp();
        BEGIN
            IF _parallel = 1 THEN
                _load_cmd := format('sed ''s/|$//'' %s/%s.dat', _data_dir, _tbl);
            ELSE
                -- Use explicit loop instead of glob to avoid prefix collisions
                -- (e.g. store_*_96.dat would also match store_returns_*_96.dat)
                _load_cmd := format(
                    'for i in $(seq 1 %s); do cat %s/%s_${i}_%s.dat 2>/dev/null; done | sed ''s/|$//''',
                    _parallel, _data_dir, _tbl, _parallel);
            END IF;
            EXECUTE format(
                'COPY tpcds.%I FROM PROGRAM %L WITH (DELIMITER %L, NULL %L)',
                _tbl, _load_cmd, '|', ''
            );
            GET DIAGNOSTICS _row_count = ROW_COUNT;
            _total_rows := _total_rows + _row_count;
            RAISE NOTICE 'Loaded % rows into % (% sec)',
                _row_count, _tbl,
                round(extract(epoch from clock_timestamp() - _start_ts)::numeric, 2);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to load %: %', _tbl, SQLERRM;
        END;
    END LOOP;

    -- ANALYZE all tables
    FOREACH _tbl IN ARRAY _tables LOOP
        EXECUTE format('ANALYZE tpcds.%I', _tbl);
    END LOOP;

    RETURN format('Loaded %s total rows from %s', _total_rows, _data_dir);
END;
$func$;

-- =============================================================================
-- clean_data() — delete .dat files from data_dir to free disk space
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.clean_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _data_dir TEXT;
BEGIN
    SELECT value INTO _data_dir FROM tpcds.config WHERE key = 'data_dir';
    IF _data_dir IS NULL OR _data_dir = '' THEN
        _data_dir := '/tmp/tpcds_data';
    END IF;

    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L',
        format('rm -f %s/*.dat', _data_dir));

    RETURN format('Cleaned up .dat files from %s', _data_dir);
END;
$func$;

-- =============================================================================
-- gen_query(scale) — generate 99 queries via dsqgen, fix, store
--   scale defaults to the value saved by gen_data(), or 1 if not set.
--   Useful for regenerating queries at a different scale without re-running gen_data().
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.gen_query(scale INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _tpcds_dir TEXT;
    _query_dir TEXT;
    _scale INTEGER;
    _cmd TEXT;
    _raw TEXT;
    _fixed TEXT;
    _i INTEGER;
    _count INTEGER := 0;
BEGIN
    _tpcds_dir := tpcds._resolve_dir('tpcds_dir', 'tpcds_dsgen');
    _query_dir := tpcds._resolve_dir('query_dir', 'tpcds_query');

    -- Use explicit scale if provided, otherwise read from config
    IF scale IS NOT NULL THEN
        _scale := scale;
    ELSE
        SELECT value::INTEGER INTO _scale FROM tpcds.config WHERE key = 'scale_factor';
        IF _scale IS NULL THEN
            _scale := 1;
        END IF;
    END IF;

    DELETE FROM tpcds.query;

    -- Create queries output directory
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _query_dir);

    SET LOCAL client_min_messages = warning;
    DROP TABLE IF EXISTS _dsqgen_out;
    RESET client_min_messages;
    CREATE TEMP TABLE _dsqgen_out (line TEXT) ON COMMIT DROP;

    FOR _i IN 1..99 LOOP
        TRUNCATE _dsqgen_out;

        _cmd := _tpcds_dir || '/tools/dsqgen'
            || ' -TEMPLATE query' || _i || '.tpl'
            || ' -DIRECTORY ' || _tpcds_dir || '/query_templates'
            || ' -DIALECT postgres'
            || ' -SCALE ' || _scale
            || ' -FILTER Y -QUIET Y'
            || ' -DISTRIBUTIONS ' || _tpcds_dir || '/tools/tpcds.idx';

        BEGIN
            EXECUTE format('COPY _dsqgen_out FROM PROGRAM %L WITH (DELIMITER E''\x01'')', _cmd);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'dsqgen failed for query %: %', _i, SQLERRM;
            CONTINUE;
        END;

        SELECT string_agg(line, E'\n') INTO _raw FROM _dsqgen_out;

        IF _raw IS NULL OR btrim(_raw) = '' THEN
            RAISE WARNING 'dsqgen produced no output for query %', _i;
            CONTINUE;
        END IF;

        _fixed := tpcds._fix_query(_i, _raw);
        INSERT INTO tpcds.query (query_id, query_text) VALUES (_i, _fixed);
        _count := _count + 1;

        -- Write query to file
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _fixed,
                format('cat > %s/query%s.sql', _query_dir, _i));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write query%s.sql: %', _i, SQLERRM;
        END;
    END LOOP;

    SET LOCAL client_min_messages = warning;
    DROP TABLE IF EXISTS _dsqgen_out;
    RESET client_min_messages;

    RETURN format('Generated and stored %s queries (scale=%s)', _count, _scale);
END;
$func$;

-- =============================================================================
-- show(qid) — return query text
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.show(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;
    RETURN _sql;
END;
$func$;

-- =============================================================================
-- exec(qid) — execute a single query, record results
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.exec(qid INTEGER)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC := 0;
    _total_rows BIGINT := 0;
    _status TEXT := 'OK';
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query.query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;

        BEGIN
            _start_ts := clock_timestamp();
            EXECUTE _stmt;
            GET DIAGNOSTICS _rows = ROW_COUNT;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
            _total_rows := _total_rows + _rows;
        EXCEPTION WHEN OTHERS THEN
            _status := 'ERROR: ' || SQLERRM;
            _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
            _total_dur := _total_dur + _dur;
        END;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);

    INSERT INTO tpcds.bench_results (query_id, status, duration_ms, rows_returned)
    VALUES (qid, _status, round(_total_dur, 2), _total_rows);

    RETURN format('query %s: %s, %s ms, %s rows', qid, _status, round(_total_dur, 2), _total_rows);
END;
$func$;

-- =============================================================================
-- bench(mode) — run or explain all 99 queries, save output to results_dir
--   bench()                     — execute all 99, save queryXX.out
--   bench('EXPLAIN')            — explain all 99, save queryXX_explain.out
--   bench('EXPLAIN (COSTS OFF)')— explain with options, save queryXX_explain.out
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.bench(mode TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _qid INTEGER;
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _start_ts TIMESTAMPTZ;
    _dur NUMERIC;
    _rows BIGINT;
    _total_dur NUMERIC;
    _total_rows BIGINT;
    _status TEXT;
    _explain_sql TEXT;
    _line TEXT;
    _all_lines TEXT;
    _part INTEGER;
    _num_stmts INTEGER;
    _results_dir TEXT;
    _bench_start TIMESTAMPTZ;
    _is_explain BOOLEAN := false;
    _explain_opts TEXT := '';
    _filename TEXT;
    _ok_count INTEGER := 0;
    _err_count INTEGER := 0;
    _skip_count INTEGER := 0;
    _bench_dur NUMERIC;
    _saved_path TEXT;
BEGIN
    _bench_start := now();
    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);

    IF mode IS NOT NULL AND upper(btrim(mode)) LIKE 'EXPLAIN%' THEN
        _is_explain := true;
        _explain_opts := btrim(regexp_replace(btrim(mode), '^\s*EXPLAIN\s*', '', 'i'));
        IF _explain_opts LIKE '(%' THEN
            _explain_opts := btrim(_explain_opts, '()');
        END IF;
    END IF;

    _results_dir := tpcds._resolve_dir('results_dir', 'tpcds_results');
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'mkdir -p ' || _results_dir);


    FOR _qid IN 1..99 LOOP
        SELECT query_text INTO _sql FROM tpcds.query WHERE query.query_id = _qid;
        IF _sql IS NULL THEN
            _skip_count := _skip_count + 1;
            RAISE NOTICE 'query %: SKIP (not found)', _qid;
            CONTINUE;
        END IF;

        _sql := btrim(_sql, E' \t\n\r');
        _sql := rtrim(_sql, ';');
        _stmts := string_to_array(_sql, ';');
        _total_dur := 0;
        _total_rows := 0;
        _status := 'OK';
        _all_lines := '';
        _part := 0;

        SELECT count(*) INTO _num_stmts
        FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

        FOREACH _stmt IN ARRAY _stmts LOOP
            _stmt := btrim(_stmt, E' \t\n\r');
            IF _stmt = '' OR _stmt IS NULL THEN
                CONTINUE;
            END IF;
            _part := _part + 1;

            IF _is_explain THEN
                IF _num_stmts > 1 THEN
                    _all_lines := _all_lines
                        || format('-- Statement %s of %s', _part, _num_stmts) || E'\n';
                END IF;
                IF _explain_opts <> '' THEN
                    _explain_sql := format('EXPLAIN (%s) %s', _explain_opts, _stmt);
                ELSE
                    _explain_sql := 'EXPLAIN ' || _stmt;
                END IF;
                BEGIN
                    _start_ts := clock_timestamp();
                    FOR _line IN EXECUTE _explain_sql LOOP
                        _all_lines := _all_lines || _line || E'\n';
                    END LOOP;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                EXCEPTION WHEN OTHERS THEN
                    _status := 'ERROR: ' || SQLERRM;
                    _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                END;
            ELSE
                BEGIN
                    _start_ts := clock_timestamp();
                    EXECUTE _stmt;
                    GET DIAGNOSTICS _rows = ROW_COUNT;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _total_rows := _total_rows + _rows;
                    _all_lines := _all_lines
                        || format('Statement %s: %s rows, %s ms',
                                  _part, _rows, round(_dur, 2)) || E'\n';
                EXCEPTION WHEN OTHERS THEN
                    _status := 'ERROR: ' || SQLERRM;
                    _dur := extract(epoch from clock_timestamp() - _start_ts) * 1000;
                    _total_dur := _total_dur + _dur;
                    _all_lines := _all_lines || 'ERROR: ' || SQLERRM || E'\n';
                END;
            END IF;
        END LOOP;

        INSERT INTO tpcds.bench_results (query_id, status, duration_ms, rows_returned)
        VALUES (_qid, _status, round(_total_dur, 2), _total_rows);

        IF _is_explain THEN
            _filename := format('query%s_explain.out', _qid);
        ELSE
            _filename := format('query%s.out', _qid);
        END IF;
        BEGIN
            EXECUTE format('COPY (SELECT %L) TO PROGRAM %L',
                _all_lines,
                format('cat > %s/%s', _results_dir, _filename));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not write %: %', _filename, SQLERRM;
        END;

        IF _status = 'OK' THEN
            _ok_count := _ok_count + 1;
        ELSE
            _err_count := _err_count + 1;
        END IF;

        RAISE NOTICE 'query %: % (% ms)', _qid, _status, round(_total_dur);
    END LOOP;

    -- Update bench_summary table with latest run
    TRUNCATE tpcds.bench_summary;
    INSERT INTO tpcds.bench_summary (query_id, status, duration_ms, rows_returned, run_ts)
    SELECT query_id, status, duration_ms, rows_returned, run_ts
    FROM tpcds.bench_results
    WHERE run_ts >= _bench_start
    ORDER BY query_id;

    -- Write summary CSV
    BEGIN
        EXECUTE format(
            'COPY (SELECT query_id, status, duration_ms, rows_returned '
            'FROM tpcds.bench_summary ORDER BY query_id) '
            'TO PROGRAM %L WITH (FORMAT csv, HEADER)',
            format('cat > %s/summary.csv', _results_dir));
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Could not write summary.csv: %', SQLERRM;
    END;

    _bench_dur := round(extract(epoch from clock_timestamp() - _bench_start)::numeric, 1);

    PERFORM set_config('search_path', _saved_path, false);

    RETURN format('Completed: %s OK, %s errors, %s skipped in %s sec. Results: %s/summary.csv',
        _ok_count, _err_count, _skip_count, _bench_dur, _results_dir);
END;
$func$;

-- =============================================================================
-- explain(qid, opts) — EXPLAIN a single query, return plan to client
-- =============================================================================
CREATE OR REPLACE FUNCTION tpcds.explain(qid INTEGER, opts TEXT DEFAULT '')
RETURNS SETOF TEXT
LANGUAGE plpgsql
AS $func$
DECLARE
    _sql TEXT;
    _stmts TEXT[];
    _stmt TEXT;
    _explain_sql TEXT;
    _line TEXT;
    _part INTEGER := 0;
    _num_stmts INTEGER;
    _saved_path TEXT;
BEGIN
    SELECT query_text INTO _sql FROM tpcds.query WHERE query.query_id = qid;
    IF _sql IS NULL THEN
        RAISE EXCEPTION 'Query % not found (valid: 1-99)', qid;
    END IF;

    _saved_path := current_setting('search_path');
    PERFORM set_config('search_path', 'tpcds, public', false);

    _sql := btrim(_sql, E' \t\n\r');
    _sql := rtrim(_sql, ';');
    _stmts := string_to_array(_sql, ';');

    SELECT count(*) INTO _num_stmts
    FROM unnest(_stmts) s WHERE btrim(s, E' \t\n\r') <> '';

    FOREACH _stmt IN ARRAY _stmts LOOP
        _stmt := btrim(_stmt, E' \t\n\r');
        IF _stmt = '' OR _stmt IS NULL THEN
            CONTINUE;
        END IF;
        _part := _part + 1;

        IF _num_stmts > 1 THEN
            RETURN NEXT format('-- Statement %s of %s', _part, _num_stmts);
        END IF;

        IF opts <> '' THEN
            _explain_sql := format('EXPLAIN (%s) %s', opts, _stmt);
        ELSE
            _explain_sql := 'EXPLAIN ' || _stmt;
        END IF;

        FOR _line IN EXECUTE _explain_sql LOOP
            RETURN NEXT _line;
        END LOOP;
    END LOOP;

    PERFORM set_config('search_path', _saved_path, false);
END;
$func$;

-- =============================================================================
-- Extension loaded — remind user to configure data_dir
-- =============================================================================
DO $notice$
BEGIN
    RAISE WARNING E'\n'
        '  tpcds extension installed.\n'
        '  Default data_dir is /tmp/tpcds_data — this may be too small for large scale factors.\n'
        '  To change it:  SELECT tpcds.config(''data_dir'', ''/your/path'');\n'
        '  Quick start:   SELECT tpcds.gen_schema();\n'
        '                 SELECT tpcds.gen_data(1);\n'
        '                 SELECT tpcds.load_data();\n'
        '                 SELECT tpcds.gen_query();\n'
        '                 SELECT tpcds.bench();';
END;
$notice$;
