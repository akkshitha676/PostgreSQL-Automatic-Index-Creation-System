# Automatic Index Creation in PostgreSQL 16

An extension to **PostgreSQL 16** that automatically creates indexes based on observed query workloads using the **Ski Rental Algorithm**. The system monitors sequential scans, estimates whether index creation is worthwhile, and asynchronously creates indexes through a background worker.

---

## Overview

In PostgreSQL, indexes must normally be created manually by database administrators. If a frequently queried column is not indexed, PostgreSQL repeatedly performs sequential scans, which become increasingly expensive as tables grow.

This project extends the PostgreSQL executor to monitor sequential scans and automatically determine when creating an index becomes more cost-effective than continuing to scan the table. The decision is based on the **Ski Rental Problem**, where repeated scan costs are compared against the one-time cost of building an index.

---

## Features

- Automatic index creation for frequently queried columns
- Ski Rental based decision algorithm
- Detection of equality predicates (`=`)
- Detection of range predicates (`<`, `>`, `<=`, `>=`)
- Shared-memory statistics accessible across PostgreSQL backends
- Background worker for asynchronous index creation
- Dynamic thresholds computed from PostgreSQL planner cost parameters
- Selectivity-based filtering to avoid creating inefficient indexes
- Write-aware index creation policy

---

## Project Architecture

```
                 SQL Query
                     │
                     ▼
            Sequential Scan
                     │
                     ▼
          record_scan_stat()
                     │
                     ▼
      Shared Memory Statistics
                     │
                     ▼
        Ski Rental Decision Logic
                     │
          Threshold Crossed?
             /            \
           No              Yes
           │                │
           ▼                ▼
     Continue         Background Worker
       Tracking              │
                             ▼
                   CREATE INDEX
                             │
                             ▼
                Future Queries Use
                    Index Scan
```

---

## Repository Structure

```
.
├── README.md
├── docs
│   ├── Project_Report.pdf
│   ├── PROJECT_EXPLANATION.md
│   └── README_SETUP.md
├── src
│   ├── auto_index.h
│   ├── auto_index_bgworker.c
│   └── nodeSeqscan_patch.c
├── scripts
│   └── COMMANDS.txt
└── dataset
    └── README.md
```

---

## Implementation

The project modifies PostgreSQL 16 by introducing new components and extending the executor.

### New Components

- `auto_index.h`
- `auto_index_bgworker.c`

### Modified PostgreSQL Components

- `nodeSeqscan.c`
- `execModifyTable.c`
- `postmaster.c`
- `Makefile`

The executor records sequential scan statistics in shared memory, while a background worker periodically checks these statistics and creates indexes when the accumulated scan cost exceeds the estimated index creation cost.

---

## Decision Strategy

For every sequential scan:

1. Detect predicates used in the query.
2. Identify the filtered column.
3. Estimate query selectivity.
4. Track cumulative scan cost.
5. Compute the estimated cost of building an index.
6. Apply write-penalty and selectivity checks.
7. Trigger asynchronous index creation when beneficial.

---

## Dataset

The project uses the **Olist Brazilian E-Commerce Dataset**.

Download:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The dataset is **not included** in this repository.

---

## Building PostgreSQL

Example build commands:

```bash
./configure --prefix=/usr/local/pgsql

make -j4

sudo make install

initdb -D /usr/local/pgsql/data

pg_ctl start
```

Refer to `docs/README_SETUP.md` for complete setup instructions.

---

## Testing

Typical workflow:

```sql
EXPLAIN ANALYZE

SELECT *
FROM olist_customers
WHERE customer_state = 'SP';
```

Execute the same query multiple times until the accumulated scan cost exceeds the index creation threshold. The background worker then creates an index automatically, and subsequent executions use an **Index Scan** instead of a **Sequential Scan**.

---

## Results

The implementation successfully demonstrates:

- Automatic index creation for frequently scanned columns
- Detection of equality and range predicates
- Dynamic threshold computation using PostgreSQL cost parameters
- Shared-memory workload tracking
- Background-worker based index creation

Testing showed a reduction in execution time from approximately **18 ms** to **0.3 ms** after automatic index creation for representative queries.

---

## Current Limitations

- Duplicate index detection requires further refinement.
- Automatic removal of stale indexes is partially implemented.
- Recency decay mechanism requires additional debugging.
- Integer literal casts against `NUMERIC` columns are not detected in all cases.

These limitations are documented in the project report.

---

## Technologies

- C
- PostgreSQL 16
- SQL
- Linux
- Shared Memory
- Background Workers

---

## Authors

- Mahathi Nakka
- Gehna Chelvi Burri
- Aditi Tapase
- Akkshitha Rathod

---

## References

- PostgreSQL 16 Documentation
- PostgreSQL Source Code
- Olist Brazilian E-Commerce Dataset
- Ski Rental Problem
