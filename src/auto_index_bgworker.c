/*
 * auto_index_bgworker.c
 *
 * Background worker for automatic index creation and dropping.
 *
 * Place at: src/backend/postmaster/auto_index_bgworker.c
 *
 * Team: Mahathi (23B0965), Gehna (23B1012), Aditi (23B1047), Akkshitha (23B1070)
 *
 * FEATURES IMPLEMENTED:
 *   FEATURE 6 - Redundancy check (index_already_exists before creating)
 *   FEATURE 7 - Background index creation (CREATE INDEX CONCURRENTLY)
 *   FEATURE 9 - Automatic dropping of stale unused indexes
 *
 * The ski rental decision (FEATURE 2) and all threshold computation happen in
 * nodeSeqscan_patch.c. This worker just acts on scan_stats[].index_created = true.
 * All thresholds (ICT, max_selectivity, max_write_ratio, decay_seconds) are now
 * computed from real pg_settings values - see compute_thresholds() in
 * nodeSeqscan_patch.c.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "executor/auto_index.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* How often the worker wakes up (seconds) */
#define AUTO_INDEX_WORKER_NAPTIME  10

void auto_index_worker_main(Datum main_arg) pg_attribute_noreturn();


/*
 * get_index_scan_count()
 *
 * FEATURE 9: Query pg_stat_user_indexes to find how many times our
 * auto-created index has been used (idx_scan column).
 * Returns -1 if the index is not found in the stats view.
 */
static int64
get_index_scan_count(Oid index_oid)
{
    int     ret;
    int64   scan_count = -1;
    char    sql[256];

    snprintf(sql, sizeof(sql),
             "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelid = %u",
             index_oid);

    SPI_connect();
    ret = SPI_execute(sql, true, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            scan_count = DatumGetInt64(d);
    }

    SPI_finish();
    return scan_count;
}

/*
 * get_stale_seconds()
 *
 * Optional runtime override for Feature 9 demos.
 * If public.auto_index_settings contains:
 *   ('stale_seconds', <integer>)
 * the worker uses that value instead of the 1-hour compile-time default.
 */
static long
get_stale_seconds(void)
{
    Oid  public_nsp;
    Oid  relid;
    int  ret;
    long stale_seconds = AUTO_INDEX_STALE_SECONDS;

    public_nsp = get_namespace_oid("public", true);
    if (!OidIsValid(public_nsp))
        return stale_seconds;

    relid = get_relname_relid("auto_index_settings", public_nsp);
    if (!OidIsValid(relid))
        return stale_seconds;

    SPI_connect();
    ret = SPI_execute(
        "SELECT value FROM auto_index_settings WHERE key = 'stale_seconds'",
        true, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
        {
            stale_seconds = (long) DatumGetInt64(d);
            if (stale_seconds < 1)
                stale_seconds = 1;
        }
    }

    SPI_finish();
    return stale_seconds;
}


/*
 * drop_stale_index()
 *
 * FEATURE 9: Drop an auto-created index that has not been used recently.
 * Uses DROP INDEX CONCURRENTLY so running queries are not blocked.
 */
static void
drop_stale_index(const char *index_name)
{
    char sql[NAMEDATALEN + 64];
    int  ret;

    snprintf(sql, sizeof(sql),
             "DROP INDEX CONCURRENTLY IF EXISTS %s", index_name);

    elog(LOG, "auto_index: dropping stale index: %s", sql);

    SPI_connect();
    ret = SPI_execute(sql, false, 0);
    SPI_finish();

    if (ret == SPI_OK_UTILITY)
        elog(LOG, "auto_index: successfully dropped stale index %s", index_name);
    else
        elog(WARNING,
             "auto_index: failed to drop stale index %s (SPI ret=%d)",
             index_name, ret);
}


/*
 * auto_index_worker_main()
 *
 * Main loop. Wakes every AUTO_INDEX_WORKER_NAPTIME seconds and does:
 *
 *   PASS 1 - Index creation (FEATURE 7):
 *     Find slots where index_created = true (ski rental threshold crossed
 *     in nodeSeqscan_patch.c), check for duplicates (FEATURE 6), then
 *     run CREATE INDEX CONCURRENTLY.
 *
 *   PASS 2 - Stale index dropping (FEATURE 9):
 *     For each auto-created index, query pg_stat_user_indexes.
 *     If idx_scan hasn't grown in AUTO_INDEX_STALE_SECONDS, drop the index.
 */
void
auto_index_worker_main(Datum main_arg)
{
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();
    /*
     * The scan statistics we act on are produced by queries running in the
     * project demo database, so the worker must connect there before looking
     * up relation names and creating indexes.
     */
    BackgroundWorkerInitializeConnection("auto_index_demo", NULL, 0);

    elog(LOG, "auto_index background worker started");

    for (;;)
    {
        int i;

        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  AUTO_INDEX_WORKER_NAPTIME * 1000L,
                  WAIT_EVENT_PG_SLEEP);
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        /* ============================================================
         * PASS 1: Create indexes where ski rental threshold was crossed
         * ============================================================ */
        LWLockAcquire(auto_index_lock, LW_SHARED);

        for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
        {
            Oid    table_oid;
            int    col_attnum;
            char  *table_name;
            char  *col_name;
            char   index_name[NAMEDATALEN * 2 + 16];
            char   sql[1024];
            bool   is_range;

            if (!scan_stats[i].in_use)        continue;
            if (!scan_stats[i].index_created) continue;
            if (scan_stats[i].auto_index_oid != InvalidOid) continue; /* already done */

            table_oid  = scan_stats[i].table_oid;
            col_attnum = scan_stats[i].col_attnum;
            is_range   = scan_stats[i].has_range_pred;

            table_name = get_rel_name(table_oid);
            col_name   = get_attname(table_oid, col_attnum, true);

            if (table_name == NULL || col_name == NULL)
                continue;

            /* FEATURE 6: Redundancy check - skip if index already exists */
            if (index_already_exists(table_oid, col_attnum))
            {
                elog(LOG,
                     "auto_index: index already exists on %s(%s), skipping",
                     table_name, col_name);

                /*
                 * Treat an existing index as a satisfied request. Otherwise the
                 * worker will keep revisiting the same slot and emit the same
                 * skip log every nap cycle even though no auto-created index is
                 * needed anymore.
                 */
                LWLockRelease(auto_index_lock);
                LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);

                if (scan_stats[i].in_use &&
                    scan_stats[i].table_oid == table_oid &&
                    scan_stats[i].col_attnum == col_attnum)
                {
                    scan_stats[i].index_created   = false;
                    scan_stats[i].total_scan_cost = 0.0;
                    scan_stats[i].scan_count      = 0;
                    scan_stats[i].write_count     = 0;
                    scan_stats[i].has_range_pred  = false;
                    scan_stats[i].auto_index_oid  = InvalidOid;
                }

                LWLockRelease(auto_index_lock);
                LWLockAcquire(auto_index_lock, LW_SHARED);
                continue;
            }

            LWLockRelease(auto_index_lock);

            /* Name our indexes with a prefix so they are easy to identify */
            snprintf(index_name, sizeof(index_name),
                     "auto_idx_%s_%s", table_name, col_name);

            /*
             * FEATURE 7: CREATE INDEX CONCURRENTLY
             * - CONCURRENTLY: builds the index without locking out reads/writes
             * - IF NOT EXISTS: safe if a concurrent process already created it
             * - Default B-tree type supports both equality (FEATURE 1) and
             *   range predicates (FEATURE 4)
             */
            snprintf(sql, sizeof(sql),
                     "CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
                     index_name, table_name, col_name);

            elog(LOG,
                 "auto_index: creating index (predicate: %s): %s",
                 is_range ? "range" : "equality", sql);

            {
                int ret;
                Oid new_oid = InvalidOid;

                SPI_connect();
                ret = SPI_execute(sql, false, 0);
                SPI_finish();

                if (ret == SPI_OK_UTILITY)
                {
                    elog(LOG, "auto_index: successfully created %s", index_name);

                    /* FEATURE 9: store the OID so we can monitor usage later */
                    new_oid = get_relname_relid(index_name,
                                  get_namespace_oid("public", true));

                    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                    scan_stats[i].auto_index_oid  = new_oid;
                    scan_stats[i].index_last_used = GetCurrentTimestamp();
                    LWLockRelease(auto_index_lock);
                }
                else
                {
                    elog(WARNING,
                         "auto_index: failed to create index on %s(%s), SPI ret=%d",
                         table_name, col_name, ret);

                    /* Reset so we retry next cycle */
                    LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                    scan_stats[i].index_created = false;
                    LWLockRelease(auto_index_lock);
                }
            }

            LWLockAcquire(auto_index_lock, LW_SHARED);
        }

        LWLockRelease(auto_index_lock);


        /* ============================================================
         * PASS 2: Drop stale indexes (FEATURE 9)
         * ============================================================ */
        LWLockAcquire(auto_index_lock, LW_SHARED);

        for (i = 0; i < AUTO_INDEX_MAX_TRACKED; i++)
        {
            Oid         index_oid;
            char       *index_name;
            int64       current_scan_count;
            TimestampTz now;
            long        stale_seconds;
            long        secs_unused;
            long        secs;
            int         usecs;

            if (!scan_stats[i].in_use)                    continue;
            if (scan_stats[i].auto_index_oid == InvalidOid) continue;

            index_oid = scan_stats[i].auto_index_oid;

            LWLockRelease(auto_index_lock);

            /* Check how many times this index has been used */
            current_scan_count = get_index_scan_count(index_oid);
            index_name = get_rel_name(index_oid);
            stale_seconds = get_stale_seconds();

            now = GetCurrentTimestamp();

            if (current_scan_count <= 0 && index_name != NULL)
            {
                /*
                 * Index exists but has never been used.
                 * Check how long it has been sitting unused.
                 */
                LWLockAcquire(auto_index_lock, LW_SHARED);
                TimestampDifference(scan_stats[i].index_last_used,
                                    now, &secs, &usecs);
                secs_unused = secs;
                LWLockRelease(auto_index_lock);

                if (secs_unused >= stale_seconds)
                {
                    /*
                     * FEATURE 9: unused for long enough - drop it.
                     * Full lifecycle: detect -> create -> monitor -> drop.
                     */
                    drop_stale_index(index_name);

                    /* Clear the slot so this pair can be re-evaluated */
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
                /* Index is being used - update last_used timestamp */
                LWLockAcquire(auto_index_lock, LW_EXCLUSIVE);
                scan_stats[i].index_last_used = now;
                LWLockRelease(auto_index_lock);
            }

            LWLockAcquire(auto_index_lock, LW_SHARED);
        }

        LWLockRelease(auto_index_lock);

        PopActiveSnapshot();
        CommitTransactionCommand();
    }
}


/*
 * =========================================================================
 * HOW TO REGISTER THIS BACKGROUND WORKER
 *
 * In src/backend/postmaster/postmaster.c, inside PostmasterMain(),
 * after shared memory initialization:
 *
 *     {
 *         BackgroundWorker worker;
 *         memset(&worker, 0, sizeof(worker));
 *         worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
 *                            BGWORKER_BACKEND_DATABASE_CONNECTION;
 *         worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
 *         worker.bgw_restart_time = 10;
 *         snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
 *         snprintf(worker.bgw_function_name, BGW_MAXLEN, "auto_index_worker_main");
 *         snprintf(worker.bgw_name, BGW_MAXLEN, "auto index worker");
 *         snprintf(worker.bgw_type, BGW_MAXLEN, "auto index worker");
 *         RegisterBackgroundWorker(&worker);
 *     }
 * =========================================================================
 */
