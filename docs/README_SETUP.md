# Automatic Threshold Setup Guide

This project already contains the modified PostgreSQL 16 source inside [postgresql-16.0]

## What This Setup Does

The scripts below:

- build your modified PostgreSQL into `automatic_threshold/pg-install`
- initialize a local database cluster in `automatic_threshold/pg-data`
- write logs to `automatic_threshold/pg-log/postgresql.log`
- run on port `55432` to avoid clashing with any system PostgreSQL
- load the full Brazilian e-commerce dataset from `automatic_threshold/brazilian-ecommerce`

## Project Layout

- Modified PostgreSQL source: [postgresql-16.0](/home/aditi_tapase/CS349/Project/automatic_threshold/postgresql-16.0)
- Dataset folder: [brazilian-ecommerce](/home/aditi_tapase/CS349/Project/automatic_threshold/brazilian-ecommerce)
- Main SQL test file: [test_auto_index.sql](/home/aditi_tapase/CS349/Project/automatic_threshold/test_auto_index.sql)
- Setup scripts: [scripts](/home/aditi_tapase/CS349/Project/automatic_threshold/scripts)

## Prerequisites

On Ubuntu or WSL, install the usual PostgreSQL build dependencies first:

```bash
sudo apt-get update
sudo apt-get install -y build-essential flex bison perl libreadline-dev zlib1g-dev
```

If `make` later complains about a missing library, install that package and rerun the same script.

## Clean From-Scratch Flow

Run everything from [automatic_threshold](/home/aditi_tapase/CS349/Project/automatic_threshold):

```bash
cd /home/aditi_tapase/CS349/Project/automatic_threshold
./scripts/setup_local_postgres.sh
./scripts/init_local_cluster.sh
./scripts/start_local_cluster.sh
./scripts/load_brazilian_ecommerce.sh auto_index_demo
```

At that point your project database is ready.

## Connect To The Local Server

```bash
cd /home/aditi_tapase/CS349/Project/automatic_threshold
./pg-install/bin/psql -h "$PWD/pg-run" -p 55432 -d auto_index_demo
```

## Run The Main Project Test SQL

```bash
cd /home/aditi_tapase/CS349/Project/automatic_threshold
./pg-install/bin/psql -h "$PWD/pg-run" -p 55432 -d auto_index_demo -f test_auto_index.sql
```

## Useful Runtime Commands

Start server:

```bash
./scripts/start_local_cluster.sh
```

Stop server:

```bash
./scripts/stop_local_cluster.sh
```

Reload config:

```bash
./pg-install/bin/pg_ctl -D ./pg-data reload
```

Tail logs:

```bash
tail -f ./pg-log/postgresql.log
```

Filter auto-index logs:

```bash
grep "auto_index" ./pg-log/postgresql.log | tail -n 50
```

## Repeatable Feature Demos

These helper scripts reset the needed index state, run the right queries, wait for the background worker, and print the relevant fresh log lines:

```bash
./scripts/run_feature6_demo.sh
./scripts/run_feature7_demo.sh
./scripts/run_feature8_demo.sh
./scripts/run_feature9_demo.sh
```

Feature 9 uses an optional runtime override table, `public.auto_index_settings`, so the stale-index demo can run in seconds instead of waiting an hour.

## Dataset Loading Details

The loader imports all CSVs currently present in [brazilian-ecommerce](automatic_threshold/brazilian-ecommerce):

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

It loads into public tables named exactly the way your existing project SQL expects, like `olist_customers`, `olist_orders`, and `olist_order_reviews`. The loader truncates tables on reruns, performs `\copy` from the repo-local CSV paths, and finishes with `ANALYZE` so your selectivity-based logic can use fresh statistics.

## Verify The Data Loaded

```bash
./pg-install/bin/psql -h "$PWD/pg-run" -p 55432 -d auto_index_demo -c "SELECT table_name, row_count FROM olist_load_summary ORDER BY table_name;"
```

