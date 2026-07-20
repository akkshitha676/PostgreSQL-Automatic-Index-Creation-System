# Automatic Index Creation in PostgreSQL 16 — Complete Project Explanation

**Team:** Mahathi (23B0965), Gehna (23B1012), Aditi (23B1047), Akkshitha (23B1070)

---

## What This Project Does (Plain English)

PostgreSQL normally lets you create indexes manually. This project makes PostgreSQL do it automatically.

Every time PostgreSQL reads a table row by row (a "sequential scan"), our code runs. It keeps a running total of how much those repeated sequential scans cost. Once that running total crosses a threshold (the "ski rental point" — the moment it's cheaper to buy skis than keep renting them), it tells a background worker to create an index on the filtered column. From then on, queries use the index instead of reading the whole table.

---

## How the Project Connects to PostgreSQL Source Code

We modified **7 locations** in the PostgreSQL 16 source and added **2 new files**. Here is the complete map:

```
Added completely new:
  src/include/executor/auto_index.h          ← shared struct definitions
  src/backend/postmaster/auto_index_bgworker.c ← background worker

Modified existing files:
  src/backend/executor/nodeSeqscan.c         ← main tracking logic (hook into seq scans)
  src/backend/executor/nodeModifyTable.c     ← hook into INSERT/UPDATE/DELETE
  src/backend/postmaster/postmaster.c        ← register the background worker
  src/backend/storage/ipc/ipci.c             ← allocate shared memory at startup
  src/backend/postmaster/Makefile            ← compile the new bgworker file
```

---

## FILE 1 — `src/include/executor/auto_index.h` (NEW FILE)

This is the shared header file. Every C file that needs to talk to the auto-index system includes this header.

**Why it exists:** C files can't share code unless there's a header declaring the shared types and functions. This header defines the structs that live in shared memory, the constants, and the function declarations.

```c
/*
 * auto_index.h
 * Header shared between nodeSeqscan.c and auto_index_bgworker.c.
 * Place at: src/include/executor/auto_index.h
 */

#ifndef AUTO_INDEX_H   /* guard: prevents this file from being included twice */
#define AUTO_INDEX_H

#include "postgres.h"    /* PostgreSQL base types (Oid, bool, etc.) */
#include "storage/lwlock.h"  /* LWLock type for mutual exclusion */


/* -----------------------------------------------------------------------
 * CONSTANTS
 * These are fixed policy values that don't depend on hardware or table size.
 * ----------------------------------------------------------------------- */

/* Maximum number of (table, column) pairs tracked at one time.
 * We can only watch 64 different columns simultaneously. */
#define AUTO_INDEX_MAX_TRACKED      64

/* Cost we charge per row each time ExecSeqScan is called.
 * Matches PostgreSQL's cpu_tuple_cost default (0.01).
 * Each call to record_scan_stat adds N * 0.01 to total_scan_cost. */
#define AUTO_INDEX_COST_PER_ROW     0.01

/* How long (in seconds) an unused auto-created index survives before being dropped.
 * 3600 = 1 hour. If no query uses the index for 1 hour, we drop it. */
#define AUTO_INDEX_STALE_SECONDS    3600

/* When decaying old workload costs, multiply by this factor.
 * 0.5 = halve the accumulated cost each decay period. */
#define AUTO_INDEX_DECAY_FACTOR     0.5

/* How often (in seconds) we re-read pg_settings to refresh our thresholds.
 * 60 seconds is fine; pg_settings values rarely change. */
#define AUTO_INDEX_THRESHOLD_REFRESH_SECS  60


/* -----------------------------------------------------------------------
 * STRUCT: AutoIndexThresholds
 * All the decision thresholds, computed from pg_settings at runtime.
 * Stored in shared memory so all backends use the same values.
 *
 * WHY these exist: The original approach hardcoded values like 1000.0.
 * That's wrong because the correct threshold depends on how fast your disk
 * is (SSD vs HDD) and how big the table is. We now compute everything from
 * seq_page_cost and random_page_cost, which the DBA sets in postgresql.conf
 * to reflect their actual hardware.
 * ----------------------------------------------------------------------- */
typedef struct AutoIndexThresholds
{
    /* From pg_settings: random_page_cost.
     * Cost of one random disk read. Default = 4.0 (HDD), ~1.1 (SSD).
     * Used in: ICT = N * log2(N) * R */
    double  random_page_cost;

    /* From pg_settings: seq_page_cost.
     * Cost of reading one page sequentially. Default = 1.0.
     * Used in: max_selectivity = S / R */
    double  seq_page_cost;

    /* max_selectivity = seq_page_cost / random_page_cost.
     * The maximum fraction of rows that can match the WHERE clause
     * and still make an index useful. If more rows match, sequential
     * scan is cheaper than an index scan.
     * HDD: 1.0/4.0 = 0.25 (index helps only if <25% of rows match)
     * SSD: 1.0/1.1 = 0.91 (index almost always helps) */
    double  max_selectivity;

    /* max_write_ratio: computed per-table in record_scan_stat() because
     * it depends on N (table size). Placeholder here. */
    double  max_write_ratio;

    /* From pg_settings: checkpoint_timeout (in seconds).
     * We use this as the decay period. If no scan arrives in one
     * checkpoint interval, we halve the accumulated cost (Feature 8). */
    long    decay_seconds;

    /* When were these values last read from pg_settings? */
    TimestampTz computed_at;

    /* Has this struct been filled in yet? (false at startup) */
    bool    initialized;
} AutoIndexThresholds;


/* -----------------------------------------------------------------------
 * STRUCT: ScanStat
 * One tracking slot per (table OID, column number) pair.
 * 64 of these live in shared memory (scan_stats[64]).
 * Every backend process that runs a sequential scan reads and writes these.
 * ----------------------------------------------------------------------- */
typedef struct ScanStat
{
    Oid     table_oid;         /* which table (OID from pg_class) */
    int     col_attnum;        /* which column (attribute number 1..N) */

    double  total_scan_cost;   /* Δ1: running total of sequential scan cost */
    int     scan_count;        /* how many times ExecSeqScan was called for this column */
    int     write_count;       /* how many INSERT/UPDATE/DELETE happened on this table */

    bool    index_created;     /* true = bgworker has been told to create the index */
    bool    in_use;            /* true = this slot is being used (not empty) */
    bool    has_range_pred;    /* true = the predicate used >, <, >= or <= (not just =) */

    TimestampTz last_scan_time;   /* when was the last scan? (for Feature 8 decay) */
    Oid         auto_index_oid;   /* OID of the index we created (for Feature 9 monitoring) */
    TimestampTz index_last_used;  /* when was the index last used? (for Feature 9 stale check) */
} ScanStat;


/* -----------------------------------------------------------------------
 * GLOBAL VARIABLE DECLARATIONS
 * These live in shared memory (nodeSeqscan.c defines them;
 * all other files use extern to access them).
 *
 * IMPORTANT: LWLock * is the correct type in PostgreSQL 16.
 * The old type LWLockId is deprecated and must NOT be used.
 * ----------------------------------------------------------------------- */
extern ScanStat            *scan_stats;       /* the 64-slot tracking array */
extern AutoIndexThresholds *auto_thresholds;  /* threshold values from pg_settings */
extern LWLock              *auto_index_lock;  /* lock protecting both of the above */


/* -----------------------------------------------------------------------
 * FUNCTION DECLARATIONS
 * Functions defined in nodeSeqscan.c or auto_index_bgworker.c,
 * declared here so other files can call them.
 * ----------------------------------------------------------------------- */
extern void auto_index_shmem_init(void);                /* called from ipci.c at startup */
extern bool index_already_exists(Oid table_oid, int col_attnum); /* Feature 6 check */
extern void record_scan_stat(struct SeqScanState *node);  /* called from ExecSeqScan */
extern void record_write_stat(Oid table_oid);             /* called from ExecInsert/Update/Delete */
extern void auto_index_worker_main(Datum main_arg) pg_attribute_noreturn(); /* bgworker entry */

#endif /* AUTO_INDEX_H */
```

---

## FILE 2 — `src/backend/executor/nodeSeqscan.c` (HEAVILY MODIFIED)

This is the executor file for sequential scans. We added ALL our tracking logic here. The original file had `ExecSeqScan()` doing a plain scan; we added a call to `record_scan_stat()` inside it and defined all the supporting functions above it.

### 2a. New `#include` lines (added at the top, lines 31–52 of the actual file)

The original file only included basic executor headers. We added:

```c
/* LINE 31 */ #include "access/table.h"
/* WHY: needed for table_open() and table_close(), which we use in index_already_exists()
   to open the pg_index system catalog and scan it */

/* LINE 32 */ #include "catalog/pg_index.h"
/* WHY: defines the pg_index row structure (Form_pg_index) and column constants
   like Anum_pg_index_indrelid. We read pg_index to check if an index already exists. */

/* LINE 33 */ #include "utils/catcache.h"
/* WHY: provides systable_beginscan/systable_getnext for scanning system catalogs.
   Used in index_already_exists(). */

/* LINE 34 */ #include "access/genam.h"
/* WHY: defines IndexIndrelidIndexId (the name of the index on pg_index.indrelid).
   We use this index to quickly find all indexes on a given table. */

/* LINE 41 */ #include "catalog/pg_statistic.h"
/* WHY: defines Form_pg_statistic and the field stadistinct.
   We read this in estimate_selectivity() to find out how many distinct values
   a column has, which tells us what fraction of rows a WHERE clause will match. */

/* LINE 42 */ #include "executor/auto_index.h"
/* WHY: our own header — includes ScanStat, AutoIndexThresholds, and all constants */

/* LINE 43 */ #include "executor/spi.h"
/* WHY: SPI = Server Programming Interface. We use SPI_connect/SPI_execute/SPI_finish
   to run SQL queries (like SELECT FROM pg_settings) from inside C code.
   This is how we read seq_page_cost and random_page_cost. */

/* LINE 44 */ #include "access/htup_details.h"
/* WHY: defines GETSTRUCT() macro that extracts the data from a heap tuple.
   We use it to read the Form_pg_statistic struct from the tuple returned by SearchSysCache. */

/* LINE 45 */ #include "utils/fmgroids.h"
/* WHY: defines F_OIDEQ, the OID of the = operator for OID values.
   Used in ScanKeyInit() inside index_already_exists() to filter by table OID. */

/* LINE 46 */ #include "math.h"
/* WHY: provides log2() function. We compute log2(N) in the ICT formula:
   ICT = N * log2(N) * random_page_cost */

/* LINE 47 */ #include "storage/lwlock.h"
/* WHY: defines LWLockAcquire, LWLockRelease, LWLock type.
   We use a shared LWLock so that multiple backend processes can safely read/write
   the shared scan_stats[] array and auto_thresholds without corrupting them. */

/* LINE 48 */ #include "storage/shmem.h"
/* WHY: defines ShmemInitStruct(), the function that allocates named shared memory.
   We call it in auto_index_shmem_init() to allocate scan_stats[] and auto_thresholds. */

/* LINE 49 */ #include "utils/lsyscache.h"
/* WHY: defines get_rel_name() (OID → table name), get_attname() (OID + attnum → column name),
   and get_opname() (operator OID → operator string like "=", ">").
   All three are used in get_pred_col_attnum() and in the bgworker. */

/* LINE 50 */ #include "utils/snapmgr.h"
/* WHY: provides GetTransactionSnapshot() used in SPI transactions.
   Needed when running SPI queries to read pg_settings. */

/* LINE 51 */ #include "utils/syscache.h"
/* WHY: defines SearchSysCache3() and ReleaseSysCache().
   We use SearchSysCache3(STATRELATTINH, ...) to look up pg_statistic for a column. */

/* LINE 52 */ #include "utils/timestamp.h"
/* WHY: defines TimestampTz, GetCurrentTimestamp(), and TimestampDifference().
   Used in Feature 8 (decay) to measure how long ago the last scan was. */
```

### 2b. Global shared memory pointers (lines 55–64 of actual file)

```c
/* LINE 55 — comment: heading for this section */
/* ---------- GLOBAL SHARED MEMORY POINTERS ---------- */

/* LINE 58 */
ScanStat *scan_stats = NULL;
/* WHY: This pointer will point into PostgreSQL shared memory once
   auto_index_shmem_init() runs. NULL at process start — the init function
   sets it. Every backend process that runs a seq scan will read/write through this. */

/* LINE 61 */
AutoIndexThresholds *auto_thresholds = NULL;
/* WHY: Same pattern — pointer into shared memory for the threshold struct.
   Shared so all backends use the same thresholds and don't each re-read pg_settings. */

/* LINE 64 */
LWLock *auto_index_lock;
/* WHY: A lock that protects scan_stats[] and auto_thresholds from concurrent access.
   Without this, two backends could read and write the same slot simultaneously,
   causing data corruption.
   IMPORTANT: The type is LWLock * (pointer), NOT LWLockId.
   LWLockId was deprecated in PostgreSQL 9.x and removed. In PG16 we must use LWLock *.
   This lock is initialized in auto_index_shmem_init() and lives inside shared memory. */
```

### 2c. `auto_index_shmem_init()` function (lines 143–175 of actual file)

**Why this function exists:** PostgreSQL allocates shared memory before any backend process starts. We need to reserve our `scan_stats[]` array and `auto_thresholds` struct inside that shared memory. This function is called once from `ipci.c` at postmaster startup.

```c
/* LINE 143 */
void
auto_index_shmem_init(void)
{
    bool found;  /* ShmemInitStruct sets this to true if memory was already allocated */

    /* LINE 148-153: Allocate the scan_stats array + extra space for our LWLock.
     * We allocate (64 * sizeof(ScanStat)) + sizeof(LWLock) in one block.
     * The LWLock is stored at the very end of this block (after the 64 slots).
     * This avoids a separate ShmemInitStruct call for the lock. */
    scan_stats = (ScanStat *) ShmemInitStruct(
        "AutoIndexScanStats",
        sizeof(ScanStat) * AUTO_INDEX_MAX_TRACKED + sizeof(LWLock),
        &found);
    if (!found)
        /* First time: zero out all fields (marks all slots as not in_use) */
        memset(scan_stats, 0, sizeof(ScanStat) * AUTO_INDEX_MAX_TRACKED + sizeof(LWLock));

    /* LINE 155-160: Allocate the thresholds struct in shared memory */
    auto_thresholds = (AutoIndexThresholds *) ShmemInitStruct(
        "AutoIndexThresholds",
        sizeof(AutoIndexThresholds),
        &found);
    if (!found)
        memset(auto_thresholds, 0, sizeof(AutoIndexThresholds));
        /* initialized=false after memset, so compute_thresholds() will run on first use */

    /* LINE 168-174: Set up the LWLock.
     * LWLockNewTrancheId() asks PostgreSQL for a new lock "slot" (tranche ID).
     * LWLockRegisterTranche() gives it a name (for pg_locks diagnostics).
     * auto_index_lock points to the LWLock we stored at the end of the scan_stats block.
     * LWLockInitialize() fills in the lock structure with the tranche ID.
     * This is the correct PG16 pattern for custom dynamic locks. */
    {
        int tranche_id = LWLockNewTrancheId();
        LWLockRegisterTranche(tranche_id, "auto_index_lock");
        auto_index_lock = (LWLock *) (scan_stats + AUTO_INDEX_MAX_TRACKED);
        /* Cast: pointer arithmetic moves past the 64 ScanStat slots to the LWLock space */
        LWLockInitialize(auto_index_lock, tranche_id);
    }
}
```

### 2d. `read_guc_double()` and `read_guc_long()` helpers (lines 189–241)

**Why these exist:** We need to read `seq_page_cost`, `random_page_cost`, and `checkpoint_timeout` from PostgreSQL configuration. These GUC (Grand Unified Configuration) values are stored in `pg_settings`. Using SPI is the standard C-level way to query `pg_settings`.

```c
/* LINE 189-212: read_guc_double() — reads a floating-point config value */
static double
read_guc_double(const char *setting_name, double default_value)
{
    char    sql[256];
    int     ret;
    double  result = default_value;  /* if query fails, return the default */

    /* Build the query: SELECT setting::double precision FROM pg_settings WHERE name = '...' */
    snprintf(sql, sizeof(sql),
             "SELECT setting::double precision FROM pg_settings WHERE name = '%s'",
             setting_name);

    SPI_connect();              /* open a connection to the query engine */
    ret = SPI_execute(sql, true, 1);   /* true = read-only, 1 = max 1 row */
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        /* SPI_getbinval extracts the first column of the first row as a Datum */
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            result = DatumGetFloat8(d);  /* convert Datum to C double */
    }
    SPI_finish();  /* close the SPI connection */
    return result;
}

/* LINE 218-241: read_guc_long() — same pattern but for integer config values */
static long
read_guc_long(const char *setting_name, long default_value)
{
    char    sql[256];
    int     ret;
    long    result = default_value;

    snprintf(sql, sizeof(sql),
             "SELECT setting::bigint FROM pg_settings WHERE name = '%s'",
             setting_name);

    SPI_connect();
    ret = SPI_execute(sql, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            result = (long) DatumGetInt64(d);
    }
    SPI_finish();
    return result;
}
```

### 2e. `compute_thresholds()` function (lines 288–334)

**Why this exists:** Rather than using fixed constants like `0.2` or `1000`, we compute all thresholds from the actual disk cost settings. This makes the system work correctly on any hardware.

```c
/* LINE 288-334 */
static void
compute_thresholds(void)
{
    double S, R;
    long   checkpoint_timeout;

    /* Step 1: Read the three values we need from pg_settings.
     * S = seq_page_cost    (how costly a sequential page read is, default 1.0)
     * R = random_page_cost (how costly a random page read is, default 4.0 HDD, 1.1 SSD)
     * checkpoint_timeout   (seconds between checkpoints, used as decay period) */
    S = read_guc_double("seq_page_cost",    1.0);
    R = read_guc_double("random_page_cost", 4.0);
    checkpoint_timeout = read_guc_long("checkpoint_timeout", 300);

    /* Step 2: Write values into shared memory under an exclusive lock.
     * Must hold lock because multiple backends might call this simultaneously. */
    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

    auto_thresholds->seq_page_cost    = S;  /* save for per-table computations */
    auto_thresholds->random_page_cost = R;  /* save for ICT = N * log2(N) * R */

    /* max_selectivity = S / R
     * DERIVATION: An index scan costs selectivity * N * R (random I/O per matching row).
     * A seq scan costs N * S (read all rows sequentially).
     * Index wins when: selectivity * R < S → selectivity < S/R.
     * So if selectivity > S/R, seq scan is cheaper. We skip indexing those columns. */
    auto_thresholds->max_selectivity = S / R;
    /* On HDD (S=1, R=4): max_selectivity = 0.25 → index only if <25% rows match */
    /* On SSD (S=1, R=1.1): max_selectivity = 0.91 → index almost always helps */

    auto_thresholds->max_write_ratio  = 0.0;  /* computed per-table in record_scan_stat, not here */

    /* decay_seconds = checkpoint_timeout.
     * REASONING: checkpoint_timeout is how often the DBA decided the DB "resets" its
     * heavy I/O cycle. If no scans arrive within one checkpoint interval, the query
     * pattern is probably gone. We halve the accumulated cost at that point. */
    auto_thresholds->decay_seconds    = checkpoint_timeout;

    auto_thresholds->computed_at  = GetCurrentTimestamp();  /* record when we refreshed */
    auto_thresholds->initialized  = true;  /* mark as ready */

    LWLockRelease(auto_index_lock);

    /* Log the computed values at DEBUG level for demo visibility */
    elog(DEBUG1,
         "auto_index: thresholds computed: seq_page_cost=%.2f "
         "random_page_cost=%.2f max_selectivity=%.3f decay_seconds=%ld",
         S, R, S / R, checkpoint_timeout);
}
```

### 2f. `ensure_thresholds_fresh()` function (lines 341–365)

**Why this exists:** We don't want to re-read `pg_settings` on every single tuple read (too slow). Instead, we check if more than 60 seconds have passed since the last read, and refresh only then.

```c
/* LINE 341-365 */
static void
ensure_thresholds_fresh(void)
{
    bool needs_refresh = false;
    TimestampTz now = GetCurrentTimestamp();
    long secs;
    int  usecs;

    /* Read under shared lock — doesn't need to be exclusive since we're just checking */
    LWLockAcquire(auto_index_lock, LW_SHARED);
    if (!auto_thresholds->initialized)
    {
        needs_refresh = true;  /* first time ever: must compute */
    }
    else
    {
        /* Check how many seconds since last refresh */
        TimestampDifference(auto_thresholds->computed_at, now, &secs, &usecs);
        if (secs > AUTO_INDEX_THRESHOLD_REFRESH_SECS)  /* > 60 seconds */
            needs_refresh = true;
    }
    LWLockRelease(auto_index_lock);

    if (needs_refresh)
        compute_thresholds();  /* go read pg_settings and update thresholds */
}
```

### 2g. `get_var_from_node()` — THE KEY BUG FIX (lines 381–404)

**Why this exists:** When PostgreSQL compiles a query like `WHERE order_status = 'delivered'`, it produces an expression tree. For a `VARCHAR` column, the planner silently wraps it in a `RelabelType` node (a no-op "cast" from varchar to text). Without this function, our code would see `RelabelType(Var)` and fail the `IsA(node, Var)` check, meaning we would never detect predicates on VARCHAR or TEXT columns. Only CHAR columns worked before this fix.

```c
/* LINE 381-404 */
static Var *
get_var_from_node(Node *node)
{
    /* Loop until we either find a Var or hit something we can't unwrap */
    for (;;)
    {
        if (node == NULL)
            return NULL;  /* nothing here */

        if (IsA(node, Var))
            return (Var *) node;  /* found the column reference */

        if (IsA(node, RelabelType))
        {
            /* RelabelType is a silent type cast, e.g. varchar → text.
             * The actual column reference is inside ->arg. Unwrap it and loop. */
            node = (Node *) ((RelabelType *) node)->arg;
        }
        else if (IsA(node, CoerceToDomain))
        {
            /* CoerceToDomain wraps domain types (e.g. a type defined as DOMAIN over text).
             * Same pattern — unwrap and loop. */
            node = (Node *) ((CoerceToDomain *) node)->arg;
        }
        else
        {
            /* Some other node type (FuncExpr, etc.) — we can't handle it */
            return NULL;
        }
    }
}
```

### 2h. `get_pred_col_attnum()` — detects which column the WHERE clause filters on (lines 417–489)

**Why this exists:** When a seq scan runs, we look at its WHERE clause (called "quals" in PostgreSQL internals). This function walks through those quals to find if there's a simple `column = constant` or `column > constant` predicate. If yes, we return the column's attribute number so we can track it.

```c
/* LINE 417-489 */
static int
get_pred_col_attnum(List *qual, bool *is_range)
{
    ListCell *lc;
    *is_range = false;  /* assume equality until we see >, <, >= or <= */

    /* Walk every clause in the WHERE list */
    foreach(lc, qual)
    {
        Node *clause = (Node *) lfirst(lc);

        /* We only understand OpExpr nodes — these are binary operators like =, >, < */
        if (IsA(clause, OpExpr))
        {
            OpExpr *op   = (OpExpr *) clause;
            List   *args = op->args;

            /* An OpExpr has exactly 2 arguments: left operand and right operand */
            if (list_length(args) == 2)
            {
                Node *left  = (Node *) linitial(args);   /* left side of the operator */
                Node *right = (Node *) lsecond(args);    /* right side of the operator */

                /* Use get_var_from_node to unwrap any RelabelType/CoerceToDomain wrappers.
                 * This is the fix: previously we used IsA(left, Var) directly,
                 * which missed VARCHAR columns. */
                Var  *lvar  = get_var_from_node(left);   /* try to get column ref from left */
                Var  *rvar  = get_var_from_node(right);  /* try to get column ref from right */

                /* Pattern: column OP constant, e.g. customer_state = 'SP' */
                if (lvar != NULL && IsA(right, Const))
                {
                    char *opname = get_opname(op->opno);  /* get the operator name string */
                    if (opname == NULL) continue;

                    if (strcmp(opname, "=") == 0)
                    {
                        /* Feature 1: equality predicate */
                        *is_range = false;
                        return lvar->varattno;  /* return column number */
                    }
                    else if (strcmp(opname, ">")  == 0 ||
                             strcmp(opname, "<")  == 0 ||
                             strcmp(opname, ">=") == 0 ||
                             strcmp(opname, "<=") == 0)
                    {
                        /* Feature 4: range predicate */
                        *is_range = true;
                        return lvar->varattno;
                    }
                }

                /* Pattern: constant OP column (reversed), e.g. 100 < price */
                if (rvar != NULL && IsA(left, Const))
                {
                    char *opname = get_opname(op->opno);
                    if (opname == NULL) continue;

                    if (strcmp(opname, "=") == 0)
                    {
                        *is_range = false;
                        return rvar->varattno;
                    }
                    else if (strcmp(opname, ">")  == 0 ||
                             strcmp(opname, "<")  == 0 ||
                             strcmp(opname, ">=") == 0 ||
                             strcmp(opname, "<=") == 0)
                    {
                        *is_range = true;
                        return rvar->varattno;
                    }
                }
            }
        }
    }

    return -1;  /* no trackable predicate found in this query */
}
```

### 2i. `find_or_create_slot()` — manage the 64-slot tracking array (lines 500–530)

**Why this exists:** We only have 64 slots. This function finds the existing slot for a (table, column) pair, or creates a new one if it's the first time we've seen this pair. Must be called with the lock held.

```c
/* LINE 500-530 */
static int
find_or_create_slot(Oid table_oid, int col_attnum)
{
    int i;
    int free_slot = -1;  /* remember the first empty slot we find */

    for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
    {
        /* If this slot tracks exactly our (table, column) pair, return it */
        if (scan_stats[i].in_use &&
            scan_stats[i].table_oid  == table_oid &&
            scan_stats[i].col_attnum == col_attnum)
            return i;

        /* Remember the first unused slot in case we need to create a new entry */
        if (!scan_stats[i].in_use && free_slot == -1)
            free_slot = i;
    }

    /* Not found — create a new slot if we have room */
    if (free_slot != -1)
    {
        memset(&scan_stats[free_slot], 0, sizeof(ScanStat));  /* zero all fields */
        scan_stats[free_slot].table_oid       = table_oid;
        scan_stats[free_slot].col_attnum      = col_attnum;
        scan_stats[free_slot].in_use          = true;  /* mark as occupied */
        scan_stats[free_slot].last_scan_time  = GetCurrentTimestamp();
        scan_stats[free_slot].auto_index_oid  = InvalidOid;  /* no index yet */
        scan_stats[free_slot].index_last_used = GetCurrentTimestamp();
        return free_slot;
    }

    return -1;  /* array full, can't track this column */
}
```

### 2j. `index_already_exists()` — Feature 6 redundancy check (lines 540–577)

**Why this exists:** Before creating an index, we check if one already exists on this column (either manually created or auto-created before). This prevents duplicate indexes that waste disk space.

```c
/* LINE 540-577 */
bool
index_already_exists(Oid table_oid, int col_attnum)
{
    Relation    indrel;    /* handle for the pg_index catalog table */
    SysScanDesc indscan;   /* a scan over pg_index */
    ScanKeyData skey;      /* filter: only look at indexes for our table */
    HeapTuple   htup;      /* one row from pg_index */
    bool        found = false;

    /* Open pg_index (the catalog that stores all index metadata) in read mode */
    indrel = table_open(IndexRelationId, AccessShareLock);

    /* Set up filter: indrelid = our table_oid */
    ScanKeyInit(&skey,
                Anum_pg_index_indrelid,   /* filter on this column of pg_index */
                BTEqualStrategyNumber,    /* use = comparison */
                F_OIDEQ,                  /* OID equality operator */
                ObjectIdGetDatum(table_oid));

    /* Start scanning pg_index for all indexes on our table */
    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

    /* For each index on this table, check if col_attnum is in its key columns */
    while (HeapTupleIsValid(htup = systable_getnext(indscan)))
    {
        Form_pg_index idxform = (Form_pg_index) GETSTRUCT(htup);
        int k;
        /* indnatts = number of key columns in this index */
        for (k = 0; k < idxform->indnatts; k++)
        {
            if (idxform->indkey.values[k] == col_attnum)
            {
                found = true;  /* this index already covers our column */
                break;
            }
        }
        if (found) break;
    }

    systable_endscan(indscan);
    table_close(indrel, AccessShareLock);
    return found;
}
```

### 2k. `estimate_selectivity()` — Feature 5 selectivity guard (lines 597–647)

**Why this exists:** An index only helps if the query returns a small fraction of rows. If `customer_state = 'SP'` would return 40% of rows, reading them all via random disk seeks (index scan) is actually slower than just reading the table sequentially. We estimate the match fraction from `pg_statistic`, which PostgreSQL updates when `ANALYZE` runs.

```c
/* LINE 597-647 */
static double
estimate_selectivity(Oid table_oid, int col_attnum)
{
    HeapTuple  statup;
    double     selectivity = 1.0;  /* default = worst case: 100% rows match */

    /* SearchSysCache3: look up pg_statistic for (table, column, no inheritance) */
    statup = SearchSysCache3(STATRELATTINH,
                             ObjectIdGetDatum(table_oid),
                             Int16GetDatum((int16) col_attnum),
                             BoolGetDatum(false));

    if (!HeapTupleIsValid(statup))
        return 1.0;  /* ANALYZE hasn't been run — assume worst case (all rows match) */

    {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(statup);
        double stadistinct = stats->stadistinct;  /* how many distinct values?
           > 0: absolute count (e.g. 27 states)
           < 0: fraction of rows (e.g. -0.5 = half the rows are distinct)
           = 0: unknown */
        float4 stanullfrac = stats->stanullfrac;  /* fraction of NULLs */

        if (stadistinct == 0)
        {
            selectivity = 1.0;  /* unknown — be conservative */
        }
        else if (stadistinct > 0)
        {
            /* 27 distinct states → each state matches 1/27 = 0.037 of rows */
            selectivity = 1.0 / stadistinct;
        }
        else
        {
            /* stadistinct = -0.5 means 50% of rows have distinct values.
             * Simplified: treat as if 1/fraction of the rows match a single value. */
            selectivity = 1.0 / (-stadistinct);
        }

        /* Rows with NULLs never match a WHERE clause predicate, reduce selectivity */
        selectivity *= (1.0 - stanullfrac);
    }

    ReleaseSysCache(statup);  /* release the cache entry */

    /* Clamp to [0, 1] in case of rounding errors */
    if (selectivity < 0.0) selectivity = 0.0;
    if (selectivity > 1.0) selectivity = 1.0;
    return selectivity;
}
```

### 2l. `record_scan_stat()` — the main hook function (lines 672–896)

**Why this exists:** This is the heart of the whole system. It is called from inside `ExecSeqScan()` every single time PostgreSQL returns a tuple from a sequential scan. It does all the math and decides whether to signal index creation.

```c
/* LINE 672-896 */
void
record_scan_stat(SeqScanState *node)
{
    /* Local variable declarations for this function */
    SeqScan     *plan;
    Oid          table_oid;
    List        *qual;
    int          col_attnum;
    bool         is_range;
    int          slot;
    double       this_scan_cost;
    double       selectivity;
    double       max_selectivity;
    double       index_build_cost;   /* ICT = N * log2(N) * R */
    double       max_write_ratio;
    int64        nrows;              /* row count from pg_class (reltuples) */
    double       log2_n;
    TimestampTz  now;
    long         secs;
    int          usecs;
    double       S, R;

    /* Safety checks: if shared memory not set up yet, skip */
    if (scan_stats == NULL || auto_thresholds == NULL)
        return;

    /* If the relation isn't open yet (scan not started), skip */
    if (node->ss.ss_currentRelation == NULL)
        return;

    /* Get the OID of the table being scanned */
    table_oid = RelationGetRelid(node->ss.ss_currentRelation);

    /* Skip system catalog tables (OID < 16384).
     * We only track user tables, not internal PostgreSQL tables. */
    if (table_oid < FirstNormalObjectId)
        return;

    /* Get the WHERE clause (quals) from the plan node */
    plan = (SeqScan *) node->ss.ps.plan;
    qual = plan->scan.plan.qual;
    /* NOTE: it's plan->scan.plan.qual (through SeqScan→Scan→Plan chain), NOT plan->plan.qual */

    /* Feature 1 + 4: check if there's an equality or range predicate.
     * Returns the column attnum if found, or -1 if no trackable predicate. */
    col_attnum = get_pred_col_attnum(qual, &is_range);
    if (col_attnum < 0)
        return;  /* no WHERE clause we understand — ignore this scan */

    /* Refresh thresholds from pg_settings if they're older than 60 seconds */
    ensure_thresholds_fresh();

    /* Get the table's estimated row count from the relation descriptor.
     * rd_rel->reltuples is updated by ANALYZE and VACUUM. */
    nrows = (int64) node->ss.ss_currentRelation->rd_rel->reltuples;
    if (nrows < 1)
        nrows = 1;  /* avoid log2(0) and division-by-zero for empty tables */

    /* log2(N): height of a B-tree on N rows.
     * Used in ICT (sort + write cost) and write ratio (leaf update cost). */
    log2_n = log2((double) nrows);
    if (log2_n < 1.0)
        log2_n = 1.0;  /* minimum tree height = 1 */

    /* Read S and R from shared memory under shared lock */
    LWLockAcquire(auto_index_lock, LW_SHARED);
    S = auto_thresholds->seq_page_cost;
    R = auto_thresholds->random_page_cost;
    LWLockRelease(auto_index_lock);

    /* Feature 5: Selectivity guard.
     * max_selectivity = S/R (computed from pg_settings, not hardcoded).
     * If the column has too high selectivity (too many rows match), skip it.
     * e.g., a boolean column → selectivity=0.5 → blocked on HDD where max=0.25. */
    max_selectivity = S / R;
    selectivity     = estimate_selectivity(table_oid, col_attnum);
    if (selectivity > max_selectivity)
    {
        elog(DEBUG1,
             "auto_index: table OID %u col %d: skipping (selectivity %.3f > threshold %.3f)",
             table_oid, col_attnum, selectivity, max_selectivity);
        return;
    }

    /* Feature 2: Compute the Index Creation Threshold (ICT).
     * ICT = N * log2(N) * R
     * This is the estimated one-time cost to build a B-tree index.
     * Derivation: Sort N rows (N * log2(N) comparisons, each costs R random I/O). */
    index_build_cost = (double) nrows * log2_n * R;

    /* Feature 3: Write-penalty threshold.
     * max_write_ratio = S / (S + R * log2(N) / N)
     * If more than this fraction of operations are writes, the index maintenance
     * cost (R * log2(N) per write) outweighs the read benefit. */
    max_write_ratio = S / (S + R * log2_n / (double) nrows);

    /* Cost of this particular ExecSeqScan call: N rows * 0.01 per row.
     * ExecSeqScan is called once per tuple returned, so one full query
     * on a table of N rows with K matching rows contributes K * (N * 0.01) cost. */
    this_scan_cost = (double) nrows * AUTO_INDEX_COST_PER_ROW;

    now = GetCurrentTimestamp();  /* current time, used for Feature 8 decay */

    /* Acquire exclusive lock to modify the scan_stats[] slot */
    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

    /* Find (or create) the slot for this (table, column) pair */
    slot = find_or_create_slot(table_oid, col_attnum);
    if (slot < 0)
    {
        LWLockRelease(auto_index_lock);
        return;  /* array is full, can't track this column */
    }

    /* If the index was already flagged for creation, handle two sub-cases:
     * (a) index was manually dropped during a demo → reset so we re-track it
     * (b) index is still live → skip (nothing more to do) */
    if (scan_stats[slot].index_created)
    {
        if (scan_stats[slot].auto_index_oid != InvalidOid &&
            get_rel_name(scan_stats[slot].auto_index_oid) == NULL)
        {
            /* Case (a): auto_index_oid is set but the index no longer exists.
             * This happens if someone manually dropped the auto index (e.g. demo reset).
             * Clear the slot so we re-evaluate from scratch. */
            scan_stats[slot].index_created   = false;
            scan_stats[slot].auto_index_oid  = InvalidOid;
            scan_stats[slot].total_scan_cost = 0.0;
            scan_stats[slot].scan_count      = 0;
            scan_stats[slot].write_count     = 0;
            scan_stats[slot].has_range_pred  = false;
        }
        else
        {
            /* Case (b): index exists and is fine — stop tracking this column */
            LWLockRelease(auto_index_lock);
            return;
        }
    }

    /* Feature 8: Workload recency weighting (time decay).
     * If no scan arrived in the last decay_seconds (= checkpoint_timeout),
     * halve the accumulated cost. This way, old stale query patterns don't
     * keep accumulating toward the ICT threshold after the workload changed.
     * We temporarily release the exclusive lock to read thresholds (avoids deadlock). */
    {
        long decay_seconds;

        LWLockRelease(auto_index_lock);      /* release exclusive to read thresholds */
        LWLockAcquire(auto_index_lock, LW_SHARED);
        decay_seconds = auto_thresholds->decay_seconds;
        LWLockRelease(auto_index_lock);

        LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);  /* reacquire exclusive */

        /* How many seconds since the last scan on this column? */
        TimestampDifference(scan_stats[slot].last_scan_time, now, &secs, &usecs);
        if (secs > decay_seconds && scan_stats[slot].total_scan_cost > 0)
        {
            /* Apply decay: halve cost for each full decay_period that passed */
            int decay_periods = (int)(secs / decay_seconds);
            int d;
            for (d = 0; d < decay_periods; d++)
                scan_stats[slot].total_scan_cost *= AUTO_INDEX_DECAY_FACTOR;  /* *= 0.5 */

            elog(DEBUG1,
                 "auto_index: table OID %u col %d: applied %d decay(s), cost now %.2f "
                 "(decay_seconds=%ld from checkpoint_timeout)",
                 table_oid, col_attnum, decay_periods,
                 scan_stats[slot].total_scan_cost, decay_seconds);
        }
    }

    /* Update tracking counters */
    scan_stats[slot].last_scan_time   = now;
    scan_stats[slot].total_scan_cost += this_scan_cost;  /* accumulate Δ1 */
    scan_stats[slot].scan_count      += 1;
    if (is_range)
        scan_stats[slot].has_range_pred = true;  /* remember if any scan used a range */

    /* Log every scan for demo visibility (visible with log_min_messages=debug1) */
    elog(DEBUG1,
         "auto_index: tracked %s scan on table OID %u col %d: "
         "scan_count=%d total_scan_cost=%.2f threshold=%.2f",
         is_range ? "range" : "equality",
         table_oid, col_attnum,
         scan_stats[slot].scan_count,
         scan_stats[slot].total_scan_cost,
         index_build_cost);

    /* THE SKI RENTAL DECISION (Feature 2):
     * Has Δ1 (total accumulated scan cost) crossed ICT (index build cost)?
     * If yes, it's now cheaper to "buy" (create the index) than to keep "renting"
     * (paying sequential scan cost every time). */
    if (scan_stats[slot].total_scan_cost >= index_build_cost)
    {
        /* Feature 3: Also check write ratio before committing.
         * If the table has too many writes, index maintenance cost is too high. */
        int    total_ops   = scan_stats[slot].scan_count + scan_stats[slot].write_count;
        double write_ratio = (total_ops > 0) ?
                             ((double) scan_stats[slot].write_count / total_ops) : 0.0;

        if (write_ratio <= max_write_ratio)
        {
            /* All checks passed — signal the background worker to create the index */
            elog(LOG,
                 "auto_index: table OID %u col %d (%s predicate): "
                 "scan cost %.2f >= index build cost %.2f "
                 "(N=%lld, log2N=%.1f, R=%.2f), "
                 "write_ratio=%.2f <= max %.2f, selectivity=%.3f. "
                 "Signaling index creation.",
                 table_oid, col_attnum,
                 is_range ? "range" : "equality",
                 scan_stats[slot].total_scan_cost, index_build_cost,
                 (long long) nrows, log2_n, R,
                 write_ratio, max_write_ratio, selectivity);

            scan_stats[slot].index_created = true;  /* bgworker will see this flag */
        }
        else
        {
            /* Feature 3 blocked creation: too many writes */
            elog(LOG,
                 "auto_index: table OID %u col %d: "
                 "cost threshold crossed but skipping (write_ratio=%.2f > max %.2f, N=%lld)",
                 table_oid, col_attnum, write_ratio, max_write_ratio, (long long) nrows);
        }
    }

    LWLockRelease(auto_index_lock);
}
```

### 2m. `record_write_stat()` — Feature 3 write counting (lines 904–919)

**Why this exists:** The write-penalty check (Feature 3) needs to know how many inserts, updates, and deletes happened on a tracked table. This function is called from `nodeModifyTable.c` whenever any row is modified.

```c
/* LINE 904-919 */
void
record_write_stat(Oid table_oid)
{
    int i;

    if (scan_stats == NULL)
        return;  /* shared memory not ready yet (can happen at startup) */

    /* Increment write_count for every tracked slot that belongs to this table.
     * One INSERT increments write_count for all columns we track for that table,
     * because the index maintenance cost applies to all indexes on the table. */
    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
    for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
    {
        if (scan_stats[i].in_use && scan_stats[i].table_oid == table_oid)
            scan_stats[i].write_count++;
    }
    LWLockRelease(auto_index_lock);
}
```

### 2n. Hook call in `ExecSeqScan()` (lines 941–949)

**Why this change:** `ExecSeqScan` is called by the executor every time a row needs to be fetched from a sequential scan. We added exactly ONE line here — `record_scan_stat(node)` — so our tracking code runs for every sequential scan.

```c
/* ORIGINAL ExecSeqScan (before our change): */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
    SeqScanState *node = castNode(SeqScanState, pstate);
    return ExecScan(&node->ss,
                    (ExecScanAccessMtd) SeqNext,
                    (ExecScanRecheckMtd) SeqRecheck);
}

/* MODIFIED ExecSeqScan (after our change — line 945): */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
    SeqScanState *node = castNode(SeqScanState, pstate);
    record_scan_stat(node);   /* ← THIS IS THE ONLY LINE WE ADDED */
    /* This one line is all it takes to plug our tracking into every seq scan. */
    return ExecScan(&node->ss,
                    (ExecScanAccessMtd) SeqNext,
                    (ExecScanRecheckMtd) SeqRecheck);
}
```

---

## FILE 3 — `src/backend/executor/nodeModifyTable.c` (3 lines added)

This file handles `INSERT`, `UPDATE`, and `DELETE` operations. We added one `#include` and one function call in each of the three write functions.

### 3a. New include (line 65)

```c
/* LINE 65 */
#include "executor/auto_index.h"
/* WHY: We need to call record_write_stat() from this file.
   That function is declared in auto_index.h. Without this include, the compiler
   would not know what record_write_stat is. */
```

### 3b. Call in `ExecInsert()` (line 763)

```c
/* LINE 763 — inside the ExecInsert() function, right at the start of the function body */
record_write_stat(RelationGetRelid(resultRelInfo->ri_RelationDesc));  // ✅ ADD HERE
/* WHY: Every time a row is INSERTed, we count it as a write for that table.
   RelationGetRelid() extracts the table OID from the relation descriptor.
   This feeds the write_count used in Feature 3's write-penalty check. */
```

### 3c. Call in `ExecDeleteAct()` (line 1436)

```c
/* LINE 1436 — inside ExecDeleteAct(), right at the start of the function body */
record_write_stat(RelationGetRelid(resultRelInfo->ri_RelationDesc));  // ✅ ADD HERE
/* WHY: Same as INSERT — every DELETE is a write that costs index maintenance. */
```

### 3d. Call in `ExecUpdate()` (line 2268)

```c
/* LINE 2268 — inside ExecUpdate(), right at the start of the function body */
record_write_stat(RelationGetRelid(resultRelInfo->ri_RelationDesc));  // ✅ ADD HERE
/* WHY: Same as INSERT and DELETE — every UPDATE modifies a row and costs
   one B-tree leaf update per index on the table. */
```

---

## FILE 4 — `src/backend/postmaster/postmaster.c` (1 block added)

The postmaster is the main PostgreSQL process that starts all other processes. We register our background worker here so it starts automatically when PostgreSQL starts.

### 4a. Background worker registration (lines 1009–1021)

```c
/* LINES 1009-1021 — inserted right after process_shared_preload_libraries() call */
{
    BackgroundWorker worker;
    memset(&worker, 0, sizeof(worker));  /* zero all fields to start clean */

    /* These flags tell PostgreSQL what capabilities our worker needs:
     * BGWORKER_SHMEM_ACCESS: worker can read/write shared memory (needs scan_stats[])
     * BGWORKER_BACKEND_DATABASE_CONNECTION: worker can connect to a database (needs SPI) */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                       BGWORKER_BACKEND_DATABASE_CONNECTION;

    /* Start after recovery finishes (i.e., database is ready for normal use) */
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

    /* If worker crashes, restart it after 10 seconds */
    worker.bgw_restart_time = 10;

    /* "postgres" = our worker is built into the main postgres binary (not a shared library) */
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");

    /* Name of the C function that is the worker's main() */
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_worker_main");

    /* Human-readable names (visible in pg_stat_activity) */
    snprintf(worker.bgw_name, BGW_MAXLEN, "auto index worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "auto index worker");

    /* Register: PostgreSQL will start this worker at startup */
    RegisterBackgroundWorker(&worker);
}
```

---

## FILE 5 — `src/backend/storage/ipc/ipci.c` (2 lines added)

`ipci.c` initializes all shared memory at startup. We added our initialization call here.

### 5a. New include (line 28)

```c
/* LINE 28 */
#include "executor/auto_index.h"
/* WHY: We need to call auto_index_shmem_init() from this file.
   That function is declared in auto_index.h. */
```

### 5b. Call to `auto_index_shmem_init()` (line 301)

```c
/* LINE 301 — inside CreateSharedMemoryAndSemaphores(), after other shmem inits */
auto_index_shmem_init();
/* WHY: This is where PostgreSQL sets up all shared memory for every subsystem.
   We call our function here so our scan_stats[] array and auto_thresholds struct
   are allocated in shared memory before any backend process starts.
   Without this call, scan_stats would remain NULL and our entire system would be a no-op. */
```

---

## FILE 6 — `src/backend/postmaster/Makefile` (1 line added)

The Makefile controls what gets compiled. We added our new background worker file to the list.

```makefile
# LINE 28 — added to the OBJS list
auto_index_bgworker.o
# WHY: Without this line, the Makefile won't compile auto_index_bgworker.c,
# and the background worker function auto_index_worker_main won't exist in the binary.
# The Makefile automatically turns auto_index_bgworker.o into compiling
# src/backend/postmaster/auto_index_bgworker.c with the right flags.
```

---

## FILE 7 — `src/backend/postmaster/auto_index_bgworker.c` (NEW FILE)

This is the background worker. It wakes up every 10 seconds, checks the shared `scan_stats[]` array, and creates or drops indexes as needed. It never runs in response to a user query — it runs independently in the background.

### 7a. Includes and setup

```c
#include "postgres.h"           /* PostgreSQL base definitions */
#include "access/genam.h"       /* for index scan functions */
#include "access/heapam.h"      /* for heap table access */
#include "catalog/namespace.h"  /* get_namespace_oid() — to find the "public" schema OID */
#include "catalog/pg_index.h"   /* pg_index catalog access */
#include "executor/auto_index.h" /* our shared header */
#include "executor/spi.h"       /* SPI_connect/execute for running SQL in C */
#include "access/table.h"       /* table_open, table_close */
#include "utils/fmgroids.h"     /* F_OIDEQ */
#include "miscadmin.h"          /* CHECK_FOR_INTERRUPTS */
#include "pgstat.h"             /* pg_stat functions */
#include "postmaster/bgworker.h" /* BackgroundWorker, BackgroundWorkerInitializeConnection */
#include "storage/ipc.h"        /* pqsignal */
#include "storage/latch.h"      /* WaitLatch, ResetLatch — for sleeping */
#include "storage/lwlock.h"     /* LWLock */
#include "storage/proc.h"       /* MyLatch */
#include "storage/shmem.h"      /* shared memory access */
#include "tcop/utility.h"       /* command utilities */
#include "utils/builtins.h"     /* various utility functions */
#include "utils/lsyscache.h"    /* get_rel_name, get_attname */
#include "utils/memutils.h"     /* memory context utilities */
#include "utils/ps_status.h"    /* process status display */
#include "utils/snapmgr.h"      /* GetTransactionSnapshot */
#include "utils/timestamp.h"    /* TimestampTz, TimestampDifference */

/* Worker wakes up every 10 seconds to check for work */
#define AUTO_INDEX_WORKER_NAPTIME  10
```

### 7b. `get_index_scan_count()` — Feature 9 usage monitoring

```c
/* WHY THIS FUNCTION: Feature 9 requires us to know if the auto-created index
 * is actually being used by queries. pg_stat_user_indexes tracks how many times
 * each index has been used (idx_scan counter). We query it via SPI. */
static int64
get_index_scan_count(Oid index_oid)
{
    int     ret;
    int64   scan_count = -1;   /* -1 means "not found" */
    char    sql[256];

    /* Query pg_stat_user_indexes for the usage count of our specific index */
    snprintf(sql, sizeof(sql),
             "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelid = %u",
             index_oid);

    SPI_connect();
    ret = SPI_execute(sql, true, 1);   /* read-only, max 1 row */

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            scan_count = DatumGetInt64(d);  /* convert to C int64 */
    }

    SPI_finish();
    return scan_count;
}
```

### 7c. `drop_stale_index()` — Feature 9 dropping

```c
/* WHY THIS FUNCTION: When an auto-created index hasn't been used for 1 hour,
 * we drop it to free disk space. We use DROP INDEX IF EXISTS (plain, not CONCURRENTLY)
 * because this runs inside a transaction (StartTransactionCommand was called above),
 * and PostgreSQL forbids DROP INDEX CONCURRENTLY inside a transaction block. */
static void
drop_stale_index(const char *index_name)
{
    char sql[NAMEDATALEN + 64];
    int  ret;

    /* Build the DROP INDEX statement */
    snprintf(sql, sizeof(sql), "DROP INDEX IF EXISTS %s", index_name);
    /* NOTE: NOT "DROP INDEX CONCURRENTLY IF EXISTS" — CONCURRENTLY is forbidden
     * inside a transaction, and our bgworker is always inside a transaction here. */

    elog(LOG, "auto_index: dropping stale index: %s", sql);

    SPI_connect();
    ret = SPI_execute(sql, false, 0);   /* false = read-write, 0 = no row limit */
    SPI_finish();

    if (ret == SPI_OK_UTILITY)
        elog(LOG, "auto_index: successfully dropped stale index %s", index_name);
    else
        elog(WARNING, "auto_index: failed to drop stale index %s (SPI ret=%d)",
             index_name, ret);
}
```

### 7d. `auto_index_worker_main()` — the main worker loop

```c
/* WHY THIS FUNCTION: This is the entry point for the background worker.
 * It runs in a loop forever, waking every 10 seconds.
 * PASS 1: Create indexes where the ski rental threshold was crossed.
 * PASS 2: Drop indexes that haven't been used for 1 hour. */
void
auto_index_worker_main(Datum main_arg)
{
    /* Set up signal handling: SIGTERM kills the worker cleanly */
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* Connect to the demo database where our tracked tables live.
     * Without this, we can't look up table/column names or create indexes. */
    BackgroundWorkerInitializeConnection("auto_index_demo", NULL, 0);

    elog(LOG, "auto_index background worker started");

    for (;;)  /* infinite loop — worker runs until PostgreSQL shuts down */
    {
        int i;

        /* Sleep for 10 seconds (or until signaled).
         * WL_TIMEOUT: wake after 10s; WL_EXIT_ON_PM_DEATH: exit if postmaster dies. */
        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  AUTO_INDEX_WORKER_NAPTIME * 1000L,  /* 10000 milliseconds */
                  WAIT_EVENT_PG_SLEEP);
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();   /* allow clean shutdown if SIGTERM received */

        /* Start a transaction so we can run SQL via SPI */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        /* ================================================================
         * PASS 1: Create indexes where ski rental threshold was crossed
         * (Feature 7: background index creation)
         * ================================================================ */
        LWLockAcquire(auto_index_lock, LW_SHARED);  /* read-only scan of slots */

        for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
        {
            Oid    table_oid;
            int    col_attnum;
            char  *table_name;
            char  *col_name;
            char   index_name[NAMEDATALEN * 2 + 16];
            char   sql[1024];
            bool   is_range;

            if (!scan_stats[i].in_use)        continue;  /* empty slot, skip */
            if (!scan_stats[i].index_created) continue;  /* not ready for indexing */
            if (scan_stats[i].auto_index_oid != InvalidOid) continue; /* already created */

            table_oid  = scan_stats[i].table_oid;
            col_attnum = scan_stats[i].col_attnum;
            is_range   = scan_stats[i].has_range_pred;

            /* Look up the table name and column name by OID */
            table_name = get_rel_name(table_oid);
            col_name   = get_attname(table_oid, col_attnum, true);

            if (table_name == NULL || col_name == NULL)
                continue;  /* table/column was dropped — skip */

            /* Feature 6: Redundancy check.
             * If an index already exists on this column (manually or auto-created),
             * skip — we don't want duplicates. */
            if (index_already_exists(table_oid, col_attnum))
            {
                elog(LOG, "auto_index: index already exists on %s(%s), skipping",
                     table_name, col_name);
                continue;
            }

            LWLockRelease(auto_index_lock);  /* release lock before running SQL */

            /* Build the index name: auto_idx_<tablename>_<colname> */
            snprintf(index_name, sizeof(index_name),
                     "auto_idx_%s_%s", table_name, col_name);

            /* Feature 7: CREATE INDEX.
             * IF NOT EXISTS: safe if a concurrent process already created it.
             * B-tree is the default: handles both equality (=) and range (>, <) predicates.
             * NOT CONCURRENTLY: CONCURRENTLY cannot run inside a transaction block,
             * and our bgworker is always inside StartTransactionCommand(). */
            snprintf(sql, sizeof(sql),
                     "CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
                     index_name, table_name, col_name);

            elog(LOG, "auto_index: creating index (predicate: %s): %s",
                 is_range ? "range" : "equality", sql);

            {
                int ret;
                Oid new_oid = InvalidOid;

                SPI_connect();
                ret = SPI_execute(sql, false, 0);  /* false = read-write */
                SPI_finish();

                if (ret == SPI_OK_UTILITY)
                {
                    elog(LOG, "auto_index: successfully created %s", index_name);

                    /* Feature 9: store the new index OID so we can monitor usage */
                    new_oid = get_relname_relid(index_name,
                                  get_namespace_oid("public", true));
                    /* get_namespace_oid("public", true): find the OID of the "public" schema */

                    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                    scan_stats[i].auto_index_oid  = new_oid;
                    scan_stats[i].index_last_used = GetCurrentTimestamp();
                    LWLockRelease(auto_index_lock);
                }
                else
                {
                    elog(WARNING, "auto_index: failed to create index on %s(%s), SPI ret=%d",
                         table_name, col_name, ret);

                    /* Reset index_created so we retry next cycle */
                    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                    scan_stats[i].index_created = false;
                    LWLockRelease(auto_index_lock);
                }
            }

            LWLockAcquire(auto_index_lock, LW_SHARED);  /* reacquire for next iteration */
        }

        LWLockRelease(auto_index_lock);


        /* ================================================================
         * PASS 2: Drop stale unused indexes (Feature 9)
         * ================================================================ */
        LWLockAcquire(auto_index_lock, LW_SHARED);

        for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
        {
            Oid         index_oid;
            char       *index_name;
            int64       current_scan_count;
            TimestampTz now;
            long        secs_unused;
            long        secs;
            int         usecs;

            if (!scan_stats[i].in_use)                      continue;
            if (scan_stats[i].auto_index_oid == InvalidOid) continue; /* no auto index */

            index_oid = scan_stats[i].auto_index_oid;
            LWLockRelease(auto_index_lock);

            /* Query pg_stat_user_indexes to see if the index has been used */
            current_scan_count = get_index_scan_count(index_oid);
            index_name = get_rel_name(index_oid);  /* get the index name by OID */

            now = GetCurrentTimestamp();

            if (current_scan_count <= 0 && index_name != NULL)
            {
                /* Index exists but has never been used — check how long */
                LWLockAcquire(auto_index_lock, LW_SHARED);
                TimestampDifference(scan_stats[i].index_last_used, now, &secs, &usecs);
                secs_unused = secs;
                LWLockRelease(auto_index_lock);

                if (secs_unused >= AUTO_INDEX_STALE_SECONDS)  /* >= 3600 seconds (1 hour) */
                {
                    /* Feature 9: drop it — it's wasting disk space */
                    drop_stale_index(index_name);

                    /* Clear the slot: same column can be re-tracked and re-indexed */
                    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                    scan_stats[i].auto_index_oid  = InvalidOid;
                    scan_stats[i].index_created   = false;
                    scan_stats[i].total_scan_cost = 0.0;
                    scan_stats[i].scan_count      = 0;
                    LWLockRelease(auto_index_lock);
                }
            }
            else if (current_scan_count > 0)
            {
                /* Index is being used — update the last_used timestamp so it
                 * doesn't get dropped while it's still helpful */
                LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                scan_stats[i].index_last_used = now;
                LWLockRelease(auto_index_lock);
            }

            LWLockAcquire(auto_index_lock, LW_SHARED);  /* reacquire for next iteration */
        }

        LWLockRelease(auto_index_lock);

        /* End the transaction */
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
}
```

---

## How All 9 Features Map to the Code

| Feature | What It Does | Where in Code |
|---------|-------------|---------------|
| **1** Equality predicate detection | Detects `col = const` in WHERE | `get_pred_col_attnum()` in nodeSeqscan.c, `strcmp(opname, "=")` |
| **2** Ski Rental decision | Compares Δ1 (accumulated scan cost) vs ICT | `record_scan_stat()`, lines comparing `total_scan_cost >= index_build_cost` |
| **3** Write penalty | Skips index if writes are too frequent | `record_write_stat()` in nodeSeqscan.c + nodeModifyTable.c calls; `write_ratio <= max_write_ratio` check in record_scan_stat() |
| **4** Range predicates | Detects `col > const`, `col < const`, etc. | `get_pred_col_attnum()`, `strcmp(opname, ">")` etc.; `is_range` flag |
| **5** Selectivity guard | Skips columns where too many rows match | `estimate_selectivity()` from pg_statistic; `selectivity > max_selectivity` check |
| **6** Redundancy check | Skips if index already exists | `index_already_exists()` called from bgworker before CREATE INDEX |
| **7** Background creation | Index created without blocking user queries | `auto_index_worker_main()` PASS 1, `SPI_execute("CREATE INDEX IF NOT EXISTS...")` |
| **8** Time decay | Halves old cost so stale workloads don't trigger | `record_scan_stat()`, `TimestampDifference` + `total_scan_cost *= 0.5` loop |
| **9** Stale index dropping | Drops unused auto indexes after 1 hour | `auto_index_worker_main()` PASS 2, `get_index_scan_count()` + `drop_stale_index()` |

---

## Why Each Threshold Is Computed, Not Hardcoded

| Old hardcoded value | What we replaced it with | Formula | Reason |
|--------------------|--------------------------|---------|--------|
| `AUTO_INDEX_CREATION_COST = 1000.0` | `ICT = N * log2(N) * R` | B-tree build cost formula | 1000 is meaningless; depends on table size and disk speed |
| `AUTO_INDEX_MAX_SELECTIVITY = 0.2` | `S / R` from pg_settings | Break-even analysis | 0.2 is wrong for SSDs where R ≈ S; formula gives 0.91 for SSD |
| `max_write_ratio = 0.5` | `S / (S + R * log2(N) / N)` | Write break-even | 0.5 is arbitrary; large tables tolerate nearly 100% writes |
| `decay period = 300s hardcoded` | `checkpoint_timeout` from pg_settings | DBA's workload epoch | 300s might be wrong for the DBA's setup |

---

## Complete Testing Guide

### Start the server and enable debug logs

```bash
cd /home/aditi_tapase/CS349/Project/automatic_threshold

# Start PostgreSQL:
./scripts/start_local_cluster.sh

# In a separate terminal, watch auto_index log messages:
tail -f ./pg-log/postgresql.log | grep auto_index

# Connect to the database:
./pg-install/bin/psql -h ./pg-run -p 55432 -d auto_index_demo
```

### Inside psql: enable debug messages

```sql
-- Enable debug messages so you can see sub-threshold tracking:
SET log_min_messages = 'debug1';
```

### Test Feature 1 + 2 (Equality + Ski Rental)

```sql
-- Drop any existing auto indexes first (clean demo):
DO $$ DECLARE r RECORD;
BEGIN FOR r IN SELECT indexname FROM pg_indexes WHERE indexname LIKE 'auto_idx_%'
LOOP EXECUTE 'DROP INDEX IF EXISTS ' || r.indexname; END LOOP; END; $$;

ANALYZE olist_customers;  -- needed for Feature 5 selectivity check

-- Run a query with an equality predicate on a CHAR(2) column:
EXPLAIN ANALYZE SELECT customer_id FROM olist_customers WHERE customer_state = 'SP';

-- Expected in log (immediately):
-- DEBUG: auto_index: thresholds computed: seq_page_cost=1.00 random_page_cost=4.00 ...
-- DEBUG: auto_index: tracked equality scan on table OID ... col 5 ...
-- LOG:   auto_index: table OID ... col 5 (equality predicate): scan cost ... >= index build cost ... Signaling index creation.

-- Wait ~10 seconds, then check for the auto-created index:
SELECT indexname, tablename FROM pg_indexes WHERE indexname LIKE 'auto_idx_%';
-- Expected: auto_idx_olist_customers_customer_state | olist_customers

-- Run a query with VARCHAR column (the RelabelType fix enables this):
ANALYZE olist_orders;
EXPLAIN ANALYZE SELECT order_id FROM olist_orders WHERE order_status = 'delivered';
-- Expected: auto_idx_olist_orders_order_status created after ~10s
```

### Test Feature 4 (Range Predicate)

```sql
ANALYZE olist_order_items;
EXPLAIN ANALYZE SELECT order_id, price FROM olist_order_items WHERE price > 100.00;
SELECT order_id FROM olist_order_items WHERE price < 50.00;
-- Expected log: "tracked range scan on table OID ... col ..."
-- Expected: auto_idx_olist_order_items_price created after ~10s
```

### Test Feature 5 (Selectivity Guard)

```sql
ANALYZE olist_order_reviews;
-- review_score has 5 distinct values → selectivity = 1/5 = 0.20 < 0.25 → PASSES guard
SELECT review_id FROM olist_order_reviews WHERE review_score = 5;
SELECT review_id FROM olist_order_reviews WHERE review_score = 4;
-- Expected: auto_idx_olist_order_reviews_review_score created

-- To see selectivity values from pg_statistic:
SELECT a.attname, s.stadistinct, ROUND((1.0/GREATEST(ABS(s.stadistinct),1))::numeric,4) AS selectivity
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
JOIN pg_class c ON c.oid = s.starelid
WHERE c.relname = 'olist_order_reviews' AND a.attname = 'review_score';
```

### Test Feature 6 (Redundancy Check)

```sql
-- Create a manual index on customer_city:
CREATE INDEX manual_city_idx ON olist_customers (customer_city);

-- Run queries that would normally trigger auto-index:
SELECT customer_id FROM olist_customers WHERE customer_city = 'sao paulo';
SELECT customer_id FROM olist_customers WHERE customer_city = 'rio de janeiro';

-- Wait 10 seconds. Expected log:
-- LOG: auto_index: index already exists on olist_customers(customer_city), skipping

-- Verify: only manual index exists, no auto_idx_:
SELECT indexname FROM pg_indexes WHERE tablename='olist_customers' AND indexname LIKE '%customer_city%';
-- Expected: only "manual_city_idx"
```

### Test Feature 7 (Background Creation)

```sql
-- After running any Feature 1/2/4/5 queries, wait 10 seconds.
-- Check that indexes appeared WITHOUT you running any CREATE INDEX command:
SELECT indexname, tablename, indexdef
FROM pg_indexes WHERE indexname LIKE 'auto_idx_%' ORDER BY tablename;
-- Each auto_idx_* line confirms Feature 7 worked.
```

### Test Feature 8 (Time Decay)

```sql
-- Speed up decay for demo:
ALTER SYSTEM SET checkpoint_timeout = '30s';
SELECT pg_reload_conf();

-- Wait 30+ seconds WITHOUT running queries on a tracked column.
-- Then run a query on that column. Expected log:
-- DEBUG: auto_index: table OID X col Y: applied 1 decay(s), cost now <half> (decay_seconds=30)

-- Restore to default:
ALTER SYSTEM SET checkpoint_timeout = '300s';
SELECT pg_reload_conf();
```

### Test Feature 9 (Stale Index Dropping)

```sql
-- Check index usage counts:
SELECT indexrelname AS index_name, idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE indexrelname LIKE 'auto_idx_%'
ORDER BY idx_scan;

-- If idx_scan = 0 and 3600 seconds pass, the bgworker will log:
-- LOG: auto_index: dropping stale index: DROP INDEX IF EXISTS auto_idx_...
-- LOG: auto_index: successfully dropped stale index auto_idx_...

-- To test faster, recompile with AUTO_INDEX_STALE_SECONDS = 60 in auto_index.h
```

### Test Feature 3 (Write Penalty)

```sql
-- Show the write ratio threshold for a large table:
SELECT ROUND((1.0 / (1.0 + 4.0 * log(2, 112650) / 112650))::numeric, 6) AS max_write_ratio;
-- Expected: ~0.999406 (large tables almost never blocked by write penalty)

-- Insert test rows (these call record_write_stat() internally):
INSERT INTO olist_order_items VALUES ('order_w1', 1, 'p1', 's1', NOW(), 99.99, 5.00);
INSERT INTO olist_order_items VALUES ('order_w2', 1, 'p2', 's1', NOW(), 49.99, 3.00);
DELETE FROM olist_order_items WHERE order_id LIKE 'order_w%';
-- (For large tables, write penalty rarely blocks. To see it block, use a tiny table ~100 rows
-- with many more writes than reads: write_ratio > max_write_ratio logs "skipping")
```

### Verify indexes are actually used

```sql
-- After indexes are created, confirm PostgreSQL uses them:
EXPLAIN ANALYZE SELECT customer_id FROM olist_customers WHERE customer_state = 'SP';
-- Expect: "Index Scan using auto_idx_olist_customers_customer_state"

EXPLAIN ANALYZE SELECT order_id FROM olist_orders WHERE order_status = 'delivered';
-- Expect: "Index Scan using auto_idx_olist_orders_order_status"

EXPLAIN ANALYZE SELECT order_id FROM olist_order_items WHERE price > 100.00;
-- Expect: "Index Scan using auto_idx_olist_order_items_price"

EXPLAIN ANALYZE SELECT review_id FROM olist_order_reviews WHERE review_score = 5;
-- Expect: "Index Scan using auto_idx_olist_order_reviews_review_score"
```

---

## Summary of All Changes Made

| # | File | Change | Why |
|---|------|--------|-----|
| 1 | `src/include/executor/auto_index.h` | **New file** — defines ScanStat, AutoIndexThresholds, constants, extern declarations | Header needed by all files that use the auto-index system |
| 2 | `src/backend/postmaster/auto_index_bgworker.c` | **New file** — background worker that creates and drops indexes | Must run outside normal query processing so it doesn't block users |
| 3 | `nodeSeqscan.c` lines 31–52 | Added 14 `#include` lines | New functions need new headers (math.h for log2, spi.h for pg_settings queries, etc.) |
| 4 | `nodeSeqscan.c` lines 58–64 | Added 3 global variable declarations | scan_stats, auto_thresholds, auto_index_lock must be accessible to bgworker too |
| 5 | `nodeSeqscan.c` lines 143–175 | Added `auto_index_shmem_init()` | Must allocate shared memory before any tracking can happen |
| 6 | `nodeSeqscan.c` lines 189–241 | Added `read_guc_double()` and `read_guc_long()` | Read pg_settings values via SPI to avoid hardcoding |
| 7 | `nodeSeqscan.c` lines 288–334 | Added `compute_thresholds()` | Derive all thresholds from actual pg_settings values |
| 8 | `nodeSeqscan.c` lines 341–365 | Added `ensure_thresholds_fresh()` | Refresh thresholds every 60s without re-reading every tuple |
| 9 | `nodeSeqscan.c` lines 381–404 | **NEW: Added `get_var_from_node()`** | Bug fix: unwrap RelabelType so VARCHAR/TEXT columns are detected |
| 10 | `nodeSeqscan.c` lines 417–489 | Added `get_pred_col_attnum()` (updated from original) | Detect equality and range predicates; now uses get_var_from_node() |
| 11 | `nodeSeqscan.c` lines 500–530 | Added `find_or_create_slot()` | Manage the 64-slot tracking array in shared memory |
| 12 | `nodeSeqscan.c` lines 540–577 | Added `index_already_exists()` | Feature 6: check pg_index before creating a duplicate |
| 13 | `nodeSeqscan.c` lines 597–647 | Added `estimate_selectivity()` | Feature 5: read pg_statistic to skip high-selectivity columns |
| 14 | `nodeSeqscan.c` lines 672–896 | Added `record_scan_stat()` | The main hook: accumulate cost, apply decay, make ski-rental decision |
| 15 | `nodeSeqscan.c` lines 904–919 | Added `record_write_stat()` | Feature 3: count writes for the write-penalty check |
| 16 | `nodeSeqscan.c` line 945 | Added `record_scan_stat(node);` in `ExecSeqScan()` | One-line hook that connects all our code to every sequential scan |
| 17 | `nodeModifyTable.c` line 65 | Added `#include "executor/auto_index.h"` | Need to call record_write_stat from this file |
| 18 | `nodeModifyTable.c` line 763 | Added `record_write_stat(...)` in `ExecInsert()` | Count INSERT operations as writes |
| 19 | `nodeModifyTable.c` line 1436 | Added `record_write_stat(...)` in `ExecDeleteAct()` | Count DELETE operations as writes |
| 20 | `nodeModifyTable.c` line 2268 | Added `record_write_stat(...)` in `ExecUpdate()` | Count UPDATE operations as writes |
| 21 | `postmaster.c` lines 1009–1021 | Added `RegisterBackgroundWorker()` block | Start the auto_index_worker_main process at PostgreSQL startup |
| 22 | `ipci.c` line 28 | Added `#include "executor/auto_index.h"` | Need to call auto_index_shmem_init from this file |
| 23 | `ipci.c` line 301 | Added `auto_index_shmem_init();` | Allocate shared memory at startup before any backend process starts |
| 24 | `postmaster/Makefile` line 28 | Added `auto_index_bgworker.o` to OBJS | Tell the build system to compile the new bgworker file |
