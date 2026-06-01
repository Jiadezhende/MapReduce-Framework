package companion.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * Central registry of configuration keys + defaults. Every Job loads
 * {@code companion-conf.xml} (via {@link #applyDefaults(Configuration)})
 * and exposes typed accessors here so no string keys leak into Job code.
 */
public final class CompanionConf {

    public static final int    T0_DEFAULT                = 1420041600;
    public static final int    DELTA_T_DEFAULT           = 300;
    public static final int    K_MIN_DEFAULT             = 3;
    public static final int    SLOT_SIZE_DEFAULT         = 300;
    public static final int    LOC_SKEW_CAP_DEFAULT      = 200_000;
    public static final int    PAIR_SALT_N_DEFAULT       = 16;
    public static final int    HLL_THRESHOLD_DEFAULT      = 1_000_000;
    public static final int    TOP_N_DEFAULT             = 10_000;
    public static final int    STAGE0A_REDUCERS_DEFAULT  = 8;
    public static final int    STAGE1_REDUCERS_DEFAULT   = 8;
    public static final int    STAGE2_REDUCERS_DEFAULT   = 8;
    public static final int    STAGE3_REDUCERS_DEFAULT   = 8;
    public static final int    STAGE2_ROUNDS_DEFAULT     = 1;
    public static final int    STAGE2_ROUND_DEFAULT      = 0;
    public static final int    STAGE1_SLOT_OFFSET_DEFAULT = 0;
    public static final boolean STAGE1_EMIT_WITHIN_SLOT_DEFAULT = true;
    public static final boolean STAGE1_COMPENSATION_ENABLED_DEFAULT = true;
    public static final String RUN_TAG_DEFAULT           = "";

    public static final String KEY_T0               = "companion.t0";
    public static final String KEY_DELTA_T          = "companion.delta.t";
    public static final String KEY_K_MIN            = "companion.k.min";
    public static final String KEY_SLOT_SIZE        = "companion.slot.size";
    public static final String KEY_LOC_SKEW_CAP     = "companion.loc.skew.cap";
    public static final String KEY_PAIR_SALT_N      = "companion.pair.salt.n";
    public static final String KEY_HLL_THRESHOLD    = "companion.hll.threshold";
    public static final String KEY_TOP_N            = "companion.top.n";
    public static final String KEY_STAGE0A_REDUCERS = "companion.stage0a.reducers";
    public static final String KEY_STAGE1_REDUCERS  = "companion.stage1.reducers";
    public static final String KEY_STAGE2_REDUCERS  = "companion.stage2.reducers";
    public static final String KEY_STAGE3_REDUCERS  = "companion.stage3.reducers";
    // Stage2 pair-hash sharding: split the run into STAGE2_ROUNDS sequential
    // sub-jobs, each emitting only pairs whose mix(vidA,vidB) % rounds == round.
    // Caps per-node nm-local-dir shuffle peak at 1/rounds (see docs/runs/31d-cf1f2f6-failed).
    public static final String KEY_STAGE2_ROUNDS   = "companion.stage2.rounds";
    public static final String KEY_STAGE2_ROUND    = "companion.stage2.round";
    public static final String KEY_STAGE1_SLOT_OFFSET = "companion.stage1.slot.offset";
    public static final String KEY_STAGE1_EMIT_WITHIN_SLOT = "companion.stage1.emit.within.slot";
    public static final String KEY_STAGE1_COMPENSATION_ENABLED = "companion.stage1.compensation.enabled";
    public static final String KEY_SALT_SEED        = "companion.salt.seed";
    public static final String KEY_RUN_TAG          = "companion.run.tag";
    public static final String KEY_HISTORY_PATH     = "companion.history.path";

    /** Load companion-conf.xml from classpath; no-op if already loaded. */
    public static Configuration applyDefaults(Configuration conf) {
        conf.addResource("companion-conf.xml");
        return conf;
    }

    public static int t0(Configuration c)          { return c.getInt(KEY_T0, T0_DEFAULT); }
    public static int deltaT(Configuration c)      { return c.getInt(KEY_DELTA_T, DELTA_T_DEFAULT); }
    public static int kMin(Configuration c)        { return c.getInt(KEY_K_MIN, K_MIN_DEFAULT); }
    public static int slotSize(Configuration c)    { return c.getInt(KEY_SLOT_SIZE, SLOT_SIZE_DEFAULT); }
    public static int locSkewCap(Configuration c)  { return c.getInt(KEY_LOC_SKEW_CAP, LOC_SKEW_CAP_DEFAULT); }
    public static int pairSaltN(Configuration c)   { return c.getInt(KEY_PAIR_SALT_N, PAIR_SALT_N_DEFAULT); }
    public static int hllThreshold(Configuration c){ return c.getInt(KEY_HLL_THRESHOLD, HLL_THRESHOLD_DEFAULT); }
    public static int topN(Configuration c)        { return c.getInt(KEY_TOP_N, TOP_N_DEFAULT); }
    public static int stage0aReducers(Configuration c){return c.getInt(KEY_STAGE0A_REDUCERS, STAGE0A_REDUCERS_DEFAULT); }
    public static int stage1Reducers(Configuration c){return c.getInt(KEY_STAGE1_REDUCERS, STAGE1_REDUCERS_DEFAULT); }
    public static int stage2Reducers(Configuration c){return c.getInt(KEY_STAGE2_REDUCERS, STAGE2_REDUCERS_DEFAULT); }
    public static int stage3Reducers(Configuration c){return c.getInt(KEY_STAGE3_REDUCERS, STAGE3_REDUCERS_DEFAULT); }
    public static int stage2Rounds(Configuration c){return c.getInt(KEY_STAGE2_ROUNDS, STAGE2_ROUNDS_DEFAULT); }
    public static int stage2Round(Configuration c) {return c.getInt(KEY_STAGE2_ROUND, STAGE2_ROUND_DEFAULT); }
    public static int stage1SlotOffset(Configuration c){return c.getInt(KEY_STAGE1_SLOT_OFFSET, STAGE1_SLOT_OFFSET_DEFAULT); }
    public static boolean stage1EmitWithinSlot(Configuration c) {
        return c.getBoolean(KEY_STAGE1_EMIT_WITHIN_SLOT, STAGE1_EMIT_WITHIN_SLOT_DEFAULT);
    }
    public static boolean stage1CompensationEnabled(Configuration c) {
        return c.getBoolean(KEY_STAGE1_COMPENSATION_ENABLED, STAGE1_COMPENSATION_ENABLED_DEFAULT);
    }
    public static String runTag(Configuration c)   { return c.get(KEY_RUN_TAG, RUN_TAG_DEFAULT); }
    public static String historyPath(Configuration c) { return c.get(KEY_HISTORY_PATH, null); }

    private CompanionConf() {}
}
