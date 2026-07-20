/*
 * nodeSeqscan_patch.c
 *
 * Code to paste into src/backend/executor/nodeSeqscan.c
 *
 * Team: Mahathi (23B0965), Gehna (23B1012), Aditi (23B1047), Akkshitha (23B1070)
 *
 * CHANGE FROM PREVIOUS VERSION:
 *   All hardcoded thresholds (1000.0, 0.2, 0.5, 300s) have been replaced
 *   with values computed at runtime from pg_settings. This makes the system
 *   work correctly on real databases regardless of table size or hardware.
 *
 * FEATURES IMPLEMENTED:
 *   FEATURE 1 - Equality predicate tracking
 *   FEATURE 2 - Ski Rental cost comparison (professor's suggestion)
 *   FEATURE 3 - Write-penalty awareness
 *   FEATURE 4 - Range predicate support
 *   FEATURE 5 - Selectivity guard using pg_statistic
 *   FEATURE 8 - Workload recency weighting with time-decay
 *
 * HOW TO APPLY:
 *   1. Paste the #include block after the existing includes in nodeSeqscan.c
 *   2. Paste all functions and globals before ExecSeqScan()
 *   3. Add one line at the start of ExecSeqScan(): record_scan_stat(node);
 *   4. Add record_write_stat() calls in execModifyTable.c (see bottom)
 */

/* ---------- ADD THESE INCLUDES at top of nodeSeqscan.c ---------- */
#include "access/genam.h"
#include "catalog/pg_index.h"
#include "catalog/pg_statistic.h"   /* FEATURE 5: selectivity guard */
#include "executor/auto_index.h"    /* our shared header */
#include "math.h"                   /* for log2() used in threshold computation */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/lsyscache.h"        /* for get_rel_name(), get_attname(), get_opname() */
#include "utils/snapmgr.h"
#include "utils/syscache.h"         /* FEATURE 5: SearchSysCache3 */
#include "utils/timestamp.h"        /* FEATURE 8: GetCurrentTimestamp */


/* ---------- GLOBAL SHARED MEMORY POINTERS ---------- */

/* scan_stats[]: array in shared memory, one slot per (table, col) pair */
ScanStat            *scan_stats     = NULL;

/* auto_thresholds: computed from pg_settings, also in shared memory */
AutoIndexThresholds *auto_thresholds = NULL;

/* LWLock protecting both scan_stats[] and auto_thresholds */
LWLockId             auto_index_lock;


/* ---------- SHARED MEMORY INIT ---------- */

/*
 * auto_index_shmem_init()
 * Called once at startup from postmaster. Sets up shared memory for
 * scan_stats[] and auto_thresholds.
 */
void
auto_index_shmem_init(void)
{
    bool found;

    scan_stats = (ScanStat *) ShmemInitStruct(
        "AutoIndexScanStats",
        sizeof(ScanStat) * AUTO_INDEX_MAX_TRACKED,
        &found);
    if (!found)
        memset(scan_stats, 0, sizeof(ScanStat) * AUTO_INDEX_MAX_TRACKED);

    auto_thresholds = (AutoIndexThresholds *) ShmemInitStruct(
        "AutoIndexThresholds",
        sizeof(AutoIndexThresholds),
        &found);
    if (!found)
        memset(auto_thresholds, 0, sizeof(AutoIndexThresholds));

    auto_index_lock = LWLockNewTrancheId();
    LWLockRegisterTranche(auto_index_lock, "auto_index_lock");
}


/* ---------- RUNTIME THRESHOLD COMPUTATION ---------- */

/*
 * read_guc_double()
 * Helper: reads a double-valued GUC from pg_settings using SPI.
 * Returns the default_value if the setting is not found.
 *
 * We use SPI (Server Programming Interface) to run a simple SELECT
 * against pg_settings, which is the standard way to read GUC values
 * from inside C code without needing a direct GUC variable reference.
 */
static double
read_guc_double(const char *setting_name, double default_value)
{
    char    sql[256];
    int     ret;
    double  result = default_value;

    snprintf(sql, sizeof(sql),
             "SELECT setting::double precision FROM pg_settings WHERE name = '%s'",
             setting_name);

    SPI_connect();
    ret = SPI_execute(sql, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            result = DatumGetFloat8(d);
    }
    SPI_finish();
    return result;
}

/*
 * read_guc_long()
 * Helper: reads a long-valued GUC from pg_settings using SPI.
 */
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

/*
 * compute_thresholds()
 * Reads seq_page_cost, random_page_cost, and checkpoint_timeout from
 * pg_settings and derives all decision thresholds from them.
 *
 * Called when auto_thresholds is uninitialized OR when it is older than
 * AUTO_INDEX_THRESHOLD_REFRESH_SECS seconds.
 *
 * MUST be called without auto_index_lock held (SPI needs to run queries).
 *
 * --- DERIVATION OF EACH THRESHOLD ---
 *
 * Let:
 *   S = seq_page_cost    (PostgreSQL default: 1.0)
 *   R = random_page_cost (PostgreSQL default: 4.0 for HDD, 1.1 for SSD)
 *   N = number of rows in the table (used at decision time, not here)
 *
 * 1. max_selectivity (FEATURE 5 - selectivity guard):
 *    An index helps when index scan cost < seq scan cost.
 *    Index scan cost  ≈ selectivity * N * R  (random page reads for matching rows)
 *    Seq scan cost    ≈ N * S                (read all pages sequentially)
 *    Break-even: selectivity * R = S
 *    -> max_selectivity = S / R
 *    On HDD (S=1, R=4): threshold = 0.25  (index helps if <25% rows match)
 *    On SSD (S=1, R=1.1): threshold = 0.91 (index almost always helps)
 *
 * 2. Ski rental threshold (FEATURE 2 - applied at decision time using N):
 *    Building a B-tree index on N rows:
 *      - Sort phase: N * log2(N) comparisons
 *      - Write phase: each of N entries costs R to write to the B-tree leaf
 *      Total: N * log2(N) * R
 *    We store R here. At decision time: threshold = N * log2(N) * R
 *
 * 3. max_write_ratio (FEATURE 3 - write penalty):
 *    Benefit per read saved: S (we save one seq page scan cost per row)
 *    Penalty per write: R * log2(N) / N  (update one B-tree leaf per write)
 *    Break-even write fraction:
 *      writes / total = S / (S + R * log2(N) / N)
 *    We store S and R here. N is applied at decision time.
 *
 * 4. decay_seconds (FEATURE 8 - recency weighting):
 *    Set to checkpoint_timeout (seconds).
 *    If no scans arrive in one checkpoint interval, the pattern is stale.
 *    Default checkpoint_timeout = 300 seconds (5 minutes).
 */
static void
compute_thresholds(void)
{
    double S, R;
    long   checkpoint_timeout;

    /* Read the three GUC values we need */
    S = read_guc_double("seq_page_cost",    1.0);   /* default 1.0 */
    R = read_guc_double("random_page_cost", 4.0);   /* default 4.0 (HDD) */
    checkpoint_timeout = read_guc_long("checkpoint_timeout", 300); /* default 300s */

    /* Protect the write to auto_thresholds */
    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

    auto_thresholds->seq_page_cost    = S;
    auto_thresholds->random_page_cost = R;

    /*
     * max_selectivity = S / R
     * This is the break-even fraction of rows for index vs seq scan.
     * If selectivity > this, a seq scan is actually cheaper than an index scan.
     */
    auto_thresholds->max_selectivity = S / R;

    /*
     * max_write_ratio: stored as seq and random costs so we can compute it
     * per-table at decision time (because it depends on N = table row count).
     * See record_scan_stat() for the actual formula applied with N.
     */
    auto_thresholds->max_write_ratio  = 0.0;  /* computed per-table in record_scan_stat */

    /*
     * decay_seconds = checkpoint_timeout
     * If no scan arrives in one checkpoint interval, the workload has changed.
     */
    auto_thresholds->decay_seconds    = checkpoint_timeout;

    auto_thresholds->computed_at      = GetCurrentTimestamp();
    auto_thresholds->initialized      = true;

    LWLockRelease(auto_index_lock);

    elog(DEBUG1,
         "auto_index: thresholds computed: seq_page_cost=%.2f "
         "random_page_cost=%.2f max_selectivity=%.3f decay_seconds=%ld",
         S, R, S / R, checkpoint_timeout);
}

/*
 * ensure_thresholds_fresh()
 * Checks if auto_thresholds needs to be (re)computed and does so if needed.
 * Called at the start of record_scan_stat() before using any threshold values.
 */
static void
ensure_thresholds_fresh(void)
{
    bool needs_refresh = false;
    TimestampTz now = GetCurrentTimestamp();
    long secs;
    int  usecs;

    /* Read under shared lock to check age */
    LWLockAcquire(auto_index_lock, LW_SHARED);
    if (!auto_thresholds->initialized)
    {
        needs_refresh = true;
    }
    else
    {
        TimestampDifference(auto_thresholds->computed_at, now, &secs, &usecs);
        if (secs > AUTO_INDEX_THRESHOLD_REFRESH_SECS)
            needs_refresh = true;
    }
    LWLockRelease(auto_index_lock);

    if (needs_refresh)
        compute_thresholds();
}


/* ---------- HELPER: Detect predicate column ---------- */

/*
 * get_var_from_node()
 *
 * Unwraps RelabelType nodes (implicit casts like VARCHAR::text) to reach the
 * underlying Var. VARCHAR columns in PostgreSQL generate a RelabelType when
 * compared with text constants, so without this unwrapping those columns are
 * silently skipped by get_pred_col_attnum().
 *
 * Also unwraps CoerceToDomain for domain types over base types.
 * Returns NULL if the node is not ultimately a Var.
 */
static Var *
get_var_from_node(Node *node)
{
    for (;;)
    {
        if (node == NULL)
            return NULL;
        if (IsA(node, Var))
            return (Var *) node;
        if (IsA(node, RelabelType))
        {
            /* RelabelType = no-op binary-compatible cast, e.g. varchar -> text */
            node = (Node *) ((RelabelType *) node)->arg;
        }
        else if (IsA(node, CoerceToDomain))
        {
            node = (Node *) ((CoerceToDomain *) node)->arg;
        }
        else
        {
            return NULL;
        }
    }
}

/*
 * get_pred_col_attnum()
 *
 * FEATURE 1 + FEATURE 4: looks at the WHERE clause quals and returns the
 * column attnum if an equality or range predicate on a column is found.
 * Sets *is_range = true for range predicates (>, <, >=, <=).
 * Returns -1 if no trackable predicate found.
 *
 * Handles both direct Var nodes and RelabelType-wrapped Vars so that
 * VARCHAR, TEXT, and domain-typed columns are tracked correctly.
 */
static int
get_pred_col_attnum(List *qual, bool *is_range)
{
    ListCell *lc;

    *is_range = false;

    foreach(lc, qual)
    {
        Node *clause = (Node *) lfirst(lc);

        if (IsA(clause, OpExpr))
        {
            OpExpr *op   = (OpExpr *) clause;
            List   *args = op->args;

            if (list_length(args) == 2)
            {
                Node *left  = (Node *) linitial(args);
                Node *right = (Node *) lsecond(args);
                Var  *lvar  = get_var_from_node(left);
                Var  *rvar  = get_var_from_node(right);

                /* Pattern: column OP constant (or cast(column) OP constant) */
                if (lvar != NULL && IsA(right, Const))
                {
                    char *opname = get_opname(op->opno);

                    if (opname == NULL) continue;

                    if (strcmp(opname, "=") == 0)
                    {
                        *is_range = false;
                        return lvar->varattno;
                    }
                    else if (strcmp(opname, ">")  == 0 ||
                             strcmp(opname, "<")  == 0 ||
                             strcmp(opname, ">=") == 0 ||
                             strcmp(opname, "<=") == 0)
                    {
                        /* FEATURE 4: range predicate */
                        *is_range = true;
                        return lvar->varattno;
                    }
                }

                /* Also handle: constant OP column (reversed) */
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

    return -1;
}


/* ---------- HELPER: Find or create a slot ---------- */

/*
 * find_or_create_slot()
 * Finds an existing scan_stats[] entry for (table_oid, col_attnum), or
 * creates a new one. Returns the index, or -1 if the array is full.
 * Must be called with auto_index_lock held exclusively.
 */
static int
find_or_create_slot(Oid table_oid, int col_attnum)
{
    int i;
    int free_slot = -1;

    for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
    {
        if (scan_stats[i].in_use &&
            scan_stats[i].table_oid  == table_oid &&
            scan_stats[i].col_attnum == col_attnum)
            return i;

        if (!scan_stats[i].in_use && free_slot == -1)
            free_slot = i;
    }

    if (free_slot != -1)
    {
        memset(&scan_stats[free_slot], 0, sizeof(ScanStat));
        scan_stats[free_slot].table_oid       = table_oid;
        scan_stats[free_slot].col_attnum      = col_attnum;
        scan_stats[free_slot].in_use          = true;
        scan_stats[free_slot].last_scan_time  = GetCurrentTimestamp();
        scan_stats[free_slot].auto_index_oid  = InvalidOid;
        scan_stats[free_slot].index_last_used = GetCurrentTimestamp();
        return free_slot;
    }

    return -1;
}


/* ---------- HELPER: Redundancy check (FEATURE 6) ---------- */

/*
 * index_already_exists()
 * Checks pg_index to see if an index on this column already exists.
 * Returns true if so (to prevent duplicates).
 */
bool
index_already_exists(Oid table_oid, int col_attnum)
{
    Relation    indrel;
    SysScanDesc indscan;
    ScanKeyData skey;
    HeapTuple   htup;
    bool        found = false;

    indrel = table_open(IndexRelationId, AccessShareLock);

    ScanKeyInit(&skey,
                Anum_pg_index_indrelid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(table_oid));

    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(indscan)))
    {
        Form_pg_index idxform = (Form_pg_index) GETSTRUCT(htup);
        int k;
        for (k = 0; k < idxform->indnatts; k++)
        {
            if (idxform->indkey.values[k] == col_attnum)
            {
                found = true;
                break;
            }
        }
        if (found) break;
    }

    systable_endscan(indscan);
    table_close(indrel, AccessShareLock);
    return found;
}


/* ---------- HELPER: Selectivity guard (FEATURE 5) ---------- */

/*
 * estimate_selectivity()
 *
 * FEATURE 5: reads pg_statistic for this (table, column) and returns
 * an estimate of what fraction of rows match a typical equality predicate.
 *
 * stadistinct interpretation:
 *   > 0: absolute count of distinct values (e.g., 1000 cities)
 *   < 0: fraction of total rows that are distinct (e.g., -1.0 = all unique)
 *   = 0: unknown
 *
 * Selectivity = 1 / distinct_count
 * e.g., gender (2 distinct values) -> selectivity = 0.5 (bad for indexing)
 *       city   (1000 distinct)     -> selectivity = 0.001 (good for indexing)
 */
static double
estimate_selectivity(Oid table_oid, int col_attnum)
{
    HeapTuple  statup;
    double     selectivity = 1.0; /* worst case: return all rows */

    statup = SearchSysCache3(STATRELATTINH,
                             ObjectIdGetDatum(table_oid),
                             Int16GetDatum((int16) col_attnum),
                             BoolGetDatum(false));

    if (!HeapTupleIsValid(statup))
        return 1.0; /* no stats yet - table not ANALYZEd */

    {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(statup);
        double stadistinct  = stats->stadistinct;
        float4 stanullfrac  = stats->stanullfrac;

        if (stadistinct == 0)
        {
            selectivity = 1.0; /* unknown */
        }
        else if (stadistinct > 0)
        {
            /* absolute count: selectivity = 1 / count */
            selectivity = 1.0 / stadistinct;
        }
        else
        {
            /*
             * stadistinct < 0 means fraction of rows are distinct.
             * e.g., stadistinct = -0.5 means 50% of rows are distinct values.
             * selectivity per value = 1 / (total_distinct_values)
             *                       = 1 / (-stadistinct * N)  ... but we don't have N here.
             * Simplified approximation: selectivity = -1.0 / stadistinct
             * (treats it as if 1/fraction of rows match each value)
             */
            selectivity = 1.0 / (-stadistinct);
        }

        /* Account for NULLs - they never match a predicate */
        selectivity *= (1.0 - stanullfrac);
    }

    ReleaseSysCache(statup);

    if (selectivity < 0.0) selectivity = 0.0;
    if (selectivity > 1.0) selectivity = 1.0;
    return selectivity;
}


/* ---------- MAIN HOOK ---------- */

/*
 * record_scan_stat()
 * Called at the start of ExecSeqScan() for every sequential scan.
 *
 * FEATURE 2 - Ski Rental (professor's suggestion: SC - IS decision):
 *   Δ1 = total accumulated scan cost (keep paying rent for seq scans)
 *   ICT = cost to build the index once (buy the skis)
 *       = N * log2(N) * random_page_cost   <-- now computed from actual N and pg_settings
 *   When Δ1 >= ICT: mark for index creation (buying is now cheaper than renting)
 *
 * FEATURE 3 - Write penalty (also now uses real N):
 *   max_write_ratio = S / (S + R * log2(N) / N)
 *   Where S = seq_page_cost, R = random_page_cost, N = table rows.
 *
 * FEATURE 5 - Selectivity guard:
 *   max_selectivity = S / R  (now from pg_settings, not hardcoded 0.2)
 *
 * FEATURE 8 - Recency weighting:
 *   decay_seconds = checkpoint_timeout from pg_settings (not hardcoded 300)
 */
void
record_scan_stat(SeqScanState *node)
{
    SeqScan     *plan;
    Oid          table_oid;
    List        *qual;
    int          col_attnum;
    bool         is_range;
    int          slot;
    double       this_scan_cost;
    double       selectivity;
    double       max_selectivity;
    double       index_build_cost;  /* ICT = N * log2(N) * R */
    double       max_write_ratio;
    int64        nrows;
    double       log2_n;
    TimestampTz  now;
    long         secs;
    int          usecs;
    double       S, R;

    if (scan_stats == NULL || auto_thresholds == NULL)
        return;

    if (node->ss.ss_currentRelation == NULL)
        return;

    table_oid = RelationGetRelid(node->ss.ss_currentRelation);
    plan      = (SeqScan *) node->ss.ps.plan;
    qual      = plan->plan.qual;

    /* FEATURE 1 + 4: check for equality or range predicate */
    col_attnum = get_pred_col_attnum(qual, &is_range);
    if (col_attnum < 0)
        return;

    /* Make sure thresholds are current (reads from pg_settings if stale) */
    ensure_thresholds_fresh();

    /* Get table row count - the key number that drives all our formulas */
    nrows = (int64) node->ss.ss_currentRelation->rd_rel->reltuples;
    if (nrows < 1)
        nrows = 1;  /* avoid log2(0) and division by zero */

    /*
     * log2(N): used in both the ski rental threshold and write ratio.
     * This is a standard factor in B-tree analysis - the height of the tree
     * is log2(N), so insert and index-scan cost both scale with log2(N).
     */
    log2_n = log2((double) nrows);
    if (log2_n < 1.0)
        log2_n = 1.0;  /* minimum height of 1 for tiny tables */

    /* Read the cost constants we computed from pg_settings */
    LWLockAcquire(auto_index_lock, LW_SHARED);
    S = auto_thresholds->seq_page_cost;
    R = auto_thresholds->random_page_cost;
    LWLockRelease(auto_index_lock);

    /*
     * FEATURE 5: Selectivity guard.
     * max_selectivity = S / R  (derived from pg_settings, not hardcoded).
     * Skip this column if too many rows would be returned anyway.
     */
    max_selectivity = S / R;
    selectivity     = estimate_selectivity(table_oid, col_attnum);
    if (selectivity > max_selectivity)
    {
        elog(DEBUG1,
             "auto_index: table OID %u col %d: skipping (selectivity %.3f > threshold %.3f)",
             table_oid, col_attnum, selectivity, max_selectivity);
        return;
    }

    /*
     * FEATURE 2: Ski rental threshold.
     * ICT = N * log2(N) * R
     * This is the estimated cost to build a B-tree index on N rows.
     * (Sort N rows in log2(N) passes, each pass costs R per row in random I/O.)
     * Once Δ1 >= ICT, buying (creating the index) beats renting (seq scanning).
     */
    index_build_cost = (double) nrows * log2_n * R;

    /*
     * FEATURE 3: Write-penalty max_write_ratio.
     * Break-even fraction of writes:
     *   max_write_ratio = S / (S + R * log2(N) / N)
     *
     * Intuition: if writes are too frequent, the cost of maintaining the index
     * (R * log2(N) per write) overwhelms the benefit of faster reads (S per scan).
     * As N grows, log2(N)/N shrinks, so larger tables tolerate more writes.
     */
    max_write_ratio = S / (S + R * log2_n / (double) nrows);

    /* Cost of this particular scan: rows * cost_per_row */
    this_scan_cost = (double) nrows * AUTO_INDEX_COST_PER_ROW;

    now = GetCurrentTimestamp();

    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

    slot = find_or_create_slot(table_oid, col_attnum);
    if (slot < 0)
    {
        LWLockRelease(auto_index_lock);
        return;
    }

    if (scan_stats[slot].index_created)
    {
        /*
         * If an auto-created index was later dropped manually, clear the stale
         * shared-memory state so the same predicate can be tracked and
         * recreated again during a live demo.
         */
        if (scan_stats[slot].auto_index_oid != InvalidOid &&
            get_rel_name(scan_stats[slot].auto_index_oid) == NULL)
        {
            scan_stats[slot].index_created   = false;
            scan_stats[slot].auto_index_oid  = InvalidOid;
            scan_stats[slot].total_scan_cost = 0.0;
            scan_stats[slot].scan_count      = 0;
            scan_stats[slot].write_count     = 0;
            scan_stats[slot].has_range_pred  = false;
        }
        else
        {
            LWLockRelease(auto_index_lock);
            return;
        }
    }

    /*
     * FEATURE 8: Workload recency weighting.
     * If no scan arrived in the last decay_seconds (= checkpoint_timeout),
     * halve the accumulated cost. This prevents old query patterns from
     * triggering index creation after the workload has moved on.
     */
    {
        long decay_seconds;

        LWLockRelease(auto_index_lock);  /* release to read thresholds safely */
        LWLockAcquire(auto_index_lock, LW_SHARED);
        decay_seconds = auto_thresholds->decay_seconds;
        LWLockRelease(auto_index_lock);

        LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

        TimestampDifference(scan_stats[slot].last_scan_time, now, &secs, &usecs);
        if (secs > decay_seconds && scan_stats[slot].total_scan_cost > 0)
        {
            int decay_periods = (int)(secs / decay_seconds);
            int d;
            for (d = 0; d < decay_periods; d++)
                scan_stats[slot].total_scan_cost *= AUTO_INDEX_DECAY_FACTOR;

            elog(DEBUG1,
                 "auto_index: table OID %u col %d: applied %d decay(s), cost now %.2f "
                 "(decay_seconds=%ld from checkpoint_timeout)",
                 table_oid, col_attnum, decay_periods,
                 scan_stats[slot].total_scan_cost, decay_seconds);
        }
    }

    scan_stats[slot].last_scan_time   = now;
    scan_stats[slot].total_scan_cost += this_scan_cost;
    scan_stats[slot].scan_count      += 1;
    if (is_range)
        scan_stats[slot].has_range_pred = true;

    /*
     * FEATURE 1 / 4 visibility:
     * Log every tracked scan so live demos can show that equality/range
     * predicates are being recorded even before the ski-rental threshold is
     * crossed.
     */
    elog(DEBUG1,
         "auto_index: tracked %s scan on table OID %u col %d: "
         "scan_count=%d total_scan_cost=%.2f threshold=%.2f",
         is_range ? "range" : "equality",
         table_oid, col_attnum,
         scan_stats[slot].scan_count,
         scan_stats[slot].total_scan_cost,
         index_build_cost);

    /*
     * SKI RENTAL DECISION (FEATURE 2):
     * If Δ1 >= ICT (index_build_cost), buying beats renting.
     * Also check write ratio (FEATURE 3) before committing.
     */
    if (scan_stats[slot].total_scan_cost >= index_build_cost)
    {
        int    total_ops   = scan_stats[slot].scan_count + scan_stats[slot].write_count;
        double write_ratio = (total_ops > 0) ?
                             ((double) scan_stats[slot].write_count / total_ops) : 0.0;

        if (write_ratio <= max_write_ratio)
        {
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

            scan_stats[slot].index_created = true;
        }
        else
        {
            elog(LOG,
                 "auto_index: table OID %u col %d: "
                 "cost threshold crossed but skipping (write_ratio=%.2f > max %.2f, N=%lld)",
                 table_oid, col_attnum, write_ratio, max_write_ratio, (long long) nrows);
        }
    }

    LWLockRelease(auto_index_lock);
}


/*
 * record_write_stat()
 * Call from ExecInsert(), ExecUpdate(), ExecDelete() in execModifyTable.c.
 * Counts writes per table for the write-penalty check (FEATURE 3).
 */
void
record_write_stat(Oid table_oid)
{
    int i;

    if (scan_stats == NULL)
        return;

    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
    for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
    {
        if (scan_stats[i].in_use && scan_stats[i].table_oid == table_oid)
            scan_stats[i].write_count++;
    }
    LWLockRelease(auto_index_lock);
}


/*
 * ---- HOW TO ADD THE CALL IN nodeSeqscan.c ----
 *
 * Find ExecSeqScan() and add ONE line at the start:
 *
 *   static TupleTableSlot *
 *   ExecSeqScan(PlanState *pstate)
 *   {
 *       SeqScanState *node = castNode(SeqScanState, pstate);
 *       record_scan_stat(node);   // <-- ADD THIS LINE
 *       return ExecScan(&node->ss, ...);
 *   }
 *
 * ---- HOW TO ADD record_write_stat() CALLS ----
 *
 * In execModifyTable.c, inside ExecInsert(), ExecUpdate(), ExecDelete():
 *   record_write_stat(RelationGetRelid(resultRelInfo->ri_RelationDesc));
 */
