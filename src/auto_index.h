/*
 * auto_index.h
 *
 * Shared declarations used by nodeSeqscan.c and auto_index_bgworker.c.
 *
 * Place at: src/include/executor/auto_index.h
 *
 * Team: Mahathi (23B0965), Gehna (23B1012), Aditi (23B1047), Akkshitha (23B1070)
 *
 * WHY WE REMOVED HARDCODED THRESHOLDS:
 *   The original version had fixed values like AUTO_INDEX_CREATION_COST = 1000.0
 *   and AUTO_INDEX_MAX_SELECTIVITY = 0.2. These have no real basis and won't
 *   work correctly on real-world databases with different table sizes, hardware
 *   (SSD vs HDD), and workload patterns.
 *
 *   All thresholds are now computed at runtime from:
 *     - Actual table row count (N) from pg_class
 *     - seq_page_cost and random_page_cost from pg_settings
 *       (set by the DBA to reflect actual hardware)
 *     - checkpoint_timeout from pg_settings (for decay interval)
 *
 *   See compute_thresholds() in nodeSeqscan_patch.c for full derivation.
 */

#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "storage/lwlock.h"

/* Maximum number of (table, column) pairs we track simultaneously */
#define AUTO_INDEX_MAX_TRACKED      64

/*
 * COST_PER_ROW: cost of processing one row in a sequential scan.
 * Matches PostgreSQL's cpu_tuple_cost default (0.01).
 * We use this when accumulating total_scan_cost per scan.
 */
#define AUTO_INDEX_COST_PER_ROW     0.01

/*
 * STALE_SECONDS: how long an unused auto-created index is kept before dropping.
 * Set to 60 for live demo (index dropped after 60s of no usage).
 * For production use, change to 3600 (1 hour).
 */
#define AUTO_INDEX_STALE_SECONDS    60

/*
 * DECAY_FACTOR: fraction by which accumulated scan cost is reduced each decay
 * period when no new scans arrive. 0.5 = halve the cost.
 * The decay PERIOD is computed from checkpoint_timeout (see AutoIndexThresholds).
 */
#define AUTO_INDEX_DECAY_FACTOR     0.5

/*
 * THRESHOLD_REFRESH_SECONDS: how often we recompute thresholds from pg_settings.
 * 60 seconds is fine - pg_settings values rarely change.
 */
#define AUTO_INDEX_THRESHOLD_REFRESH_SECS  60


/*
 * AutoIndexThresholds: all decision thresholds, computed at runtime.
 * Stored in shared memory so all backends use the same values.
 * Refreshed when computed_at is older than THRESHOLD_REFRESH_SECONDS.
 *
 * All values are derived from seq_page_cost, random_page_cost, and
 * checkpoint_timeout read from pg_settings. See compute_thresholds()
 * in nodeSeqscan_patch.c for the derivation of each one.
 */
typedef struct AutoIndexThresholds
{
    /*
     * index_build_cost_per_row:
     * Used to compute the ski rental threshold for a table of N rows as:
     *   threshold = N * index_build_cost_per_row
     *
     * Derivation: building a B-tree index on N rows costs roughly
     *   N * log2(N) * random_page_cost   (sort N rows, write log2(N) pages each)
     * Dividing by N gives the per-row factor:
     *   index_build_cost_per_row = log2(N) * random_page_cost
     * We store just random_page_cost here; the log2(N) part is multiplied in
     * at decision time because N is per-table and changes.
     *
     * Formula stored: random_page_cost  (log2(N) applied at decision time)
     */
    double  random_page_cost;   /* from pg_settings: random_page_cost */
    double  seq_page_cost;      /* from pg_settings: seq_page_cost */

    /*
     * max_selectivity: maximum fraction of rows that can match the predicate
     * and still justify creating an index.
     *
     * Derivation (break-even analysis):
     *   Index scan cost  = matching_fraction * N * random_page_cost
     *   Seq scan cost    = N * seq_page_cost
     *   Break-even when: matching_fraction * random_page_cost = seq_page_cost
     *   -> max_selectivity = seq_page_cost / random_page_cost
     *
     * On HDD (seq=1.0, random=4.0): max_selectivity = 0.25
     * On SSD (seq=1.0, random=1.1): max_selectivity = 0.91
     *
     * This adapts automatically when the DBA changes random_page_cost in
     * postgresql.conf (e.g., to reflect SSD hardware).
     */
    double  max_selectivity;    /* = seq_page_cost / random_page_cost */

    /*
     * max_write_ratio: maximum fraction of write operations tolerated before
     * we skip index creation for a write-heavy table.
     *
     * Derivation: Let B = benefit per read (seq scan cost avoided),
     *                 P = penalty per write (index maintenance cost).
     *   B = seq_page_cost * pages_in_table   (approx: total_rows * seq_page_cost)
     *   P = random_page_cost * log2(N)        (B-tree leaf update cost)
     *   Break-even: writes/total = B / (B + P)
     *             = (seq_page_cost) / (seq_page_cost + random_page_cost * log2(N) / N)
     * Simplified for typical N (where log2(N)/N is small):
     *             ≈ seq_page_cost / (seq_page_cost + random_page_cost * log2(N) / N)
     * We compute this with actual N at decision time.
     * Stored here: the two cost constants; N is applied at decision time.
     */
    double  max_write_ratio;    /* computed from seq and random page costs + N */

    /*
     * decay_seconds: decay period for workload recency weighting.
     * Set to checkpoint_timeout from pg_settings.
     *
     * Rationale: checkpoint_timeout is the DBA's chosen "workload epoch."
     * If no scans arrive within one checkpoint interval, the query pattern
     * is likely no longer active. We halve accumulated cost each interval.
     */
    long    decay_seconds;      /* from pg_settings: checkpoint_timeout */

    /* Timestamp when these were last read from pg_settings */
    TimestampTz computed_at;

    /* Whether this struct has been initialized at all */
    bool    initialized;
} AutoIndexThresholds;


/*
 * ScanStat: one entry per (table OID, column) pair we track.
 * Lives in PostgreSQL shared memory.
 */
typedef struct ScanStat
{
    Oid     table_oid;
    int     col_attnum;
    double  total_scan_cost;    /* Δ1: accumulated sequential scan cost */
    int     scan_count;         /* number of seq scans recorded */
    int     write_count;        /* number of INSERT/UPDATE/DELETE recorded */
    bool    index_created;      /* true = bgworker should create index */
    bool    in_use;             /* true = slot is occupied */
    bool    has_range_pred;     /* true = a range predicate was seen */

    TimestampTz last_scan_time;     /* FEATURE 8: for time-decay */
    Oid         auto_index_oid;     /* FEATURE 9: OID of index we created */
    TimestampTz index_last_used;    /* FEATURE 9: last time index was used */
} ScanStat;


/* Shared memory pointers - initialized in auto_index_shmem_init() */
extern ScanStat            *scan_stats;
extern AutoIndexThresholds *auto_thresholds;
extern LWLock              *auto_index_lock;   /* LWLock* in PG16; LWLockId was deprecated */

/* Functions */
extern void auto_index_shmem_init(void);
extern bool index_already_exists(Oid table_oid, int col_attnum);
extern void record_scan_stat(struct SeqScanState *node);
extern void record_write_stat(Oid table_oid);
extern void auto_index_worker_main(Datum main_arg) pg_attribute_noreturn();

#endif /* AUTO_INDEX_H */
