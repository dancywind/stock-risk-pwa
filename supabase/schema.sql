-- Supabase (Postgres) schema，第一階段。可重複執行。
-- 若你之前跑過舊版 schema，請先在 SQL Editor 執行：
--   drop table if exists scan_status, market_stats, qqq_proxy, twd_rates, private_holdings, macro_series cascade;

-- 每次檢查的帶時間紀錄（成功／無新資料／失敗／略過都寫入，不得靜默）
create table if not exists scan_status (
  id bigint generated always as identity primary key,
  checked_at timestamptz not null default now(),
  job text,
  source text not null,
  status text not null check (status in ('success','no_new_data','failure','skipped')),
  as_of date,
  detail text
);

-- FRED 總經日資料（日期是該筆資料自己的日期）
create table if not exists macro_series (
  id bigint generated always as identity primary key,
  series_id text not null,
  label text not null,
  as_of date not null,
  value numeric not null,
  source text not null default 'FRED',
  fetched_at timestamptz not null default now(),
  unique (series_id, as_of)
);

-- 台幣匯率（備用層與 FRED 收盤分開；只有 FRED 為 is_close_history = true）
-- App 畫趨勢圖時只取 is_close_history = true；顯示最新值時，FRED 有值就蓋過備用層。
create table if not exists twd_rates (
  id bigint generated always as identity primary key,
  as_of date not null,
  value numeric not null,
  kind text not null check (kind in ('official_close','official_statistic','board_rate','spot_aggregator')),
  source text not null,
  layer integer not null,
  is_close_history boolean not null default false,
  fetched_at timestamptz not null default now(),
  unique (layer, source, as_of)
);

-- 之後階段使用（先建好）
create table if not exists market_stats (
  id bigint generated always as identity primary key,
  market text not null,
  as_of date not null,
  close numeric, change_pct numeric, turnover numeric,
  advancers integer, decliners integer, unchanged integer,
  source text not null,
  unique (market, as_of, source)
);

create table if not exists qqq_proxy (
  id bigint generated always as identity primary key,
  as_of date not null unique,
  change_pct numeric not null,
  proxy boolean not null default true,
  index_name text not null default 'Nasdaq-100（非 Nasdaq 綜合指數）'
);

-- 私人持股（登入保護）
create table if not exists private_holdings (
  id bigint generated always as identity primary key,
  user_id uuid not null,
  symbol text not null,
  market text not null,
  shares numeric,
  avg_cost numeric,
  buy_date date,
  created_at timestamptz not null default now()
);

-- 全部開啟 Row Level Security。排程程式用 service key，會繞過 RLS 寫入；
-- 前端用登入使用者，只能依下列規則讀取。
alter table scan_status enable row level security;
alter table macro_series enable row level security;
alter table twd_rates enable row level security;
alter table market_stats enable row level security;
alter table qqq_proxy enable row level security;
alter table private_holdings enable row level security;

drop policy if exists "login read scan_status" on scan_status;
create policy "login read scan_status" on scan_status for select to authenticated using (true);
drop policy if exists "login read macro_series" on macro_series;
create policy "login read macro_series" on macro_series for select to authenticated using (true);
drop policy if exists "login read twd_rates" on twd_rates;
create policy "login read twd_rates" on twd_rates for select to authenticated using (true);
drop policy if exists "login read market_stats" on market_stats;
create policy "login read market_stats" on market_stats for select to authenticated using (true);
drop policy if exists "login read qqq_proxy" on qqq_proxy;
create policy "login read qqq_proxy" on qqq_proxy for select to authenticated using (true);

drop policy if exists "own holdings" on private_holdings;
create policy "own holdings" on private_holdings for all to authenticated
  using (auth.uid() = user_id) with check (auth.uid() = user_id);
