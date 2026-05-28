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
    public static final int    TOP_N_DEFAULT             = 10_000;
    public static final int    STAGE1_REDUCERS_DEFAULT   = 8;
    public static final int    STAGE2_REDUCERS_DEFAULT   = 8;
    public static final String RUN_TAG_DEFAULT           = "";

    public static final String KEY_T0               = "companion.t0";
    public static final String KEY_DELTA_T          = "companion.delta.t";
    public static final String KEY_K_MIN            = "companion.k.min";
    public static final String KEY_SLOT_SIZE        = "companion.slot.size";
    public static final String KEY_LOC_SKEW_CAP     = "companion.loc.skew.cap";
    public static final String KEY_PAIR_SALT_N      = "companion.pair.salt.n";
    public static final String KEY_TOP_N            = "companion.top.n";
    public static final String KEY_STAGE1_REDUCERS  = "companion.stage1.reducers";
    public static final String KEY_STAGE2_REDUCERS  = "companion.stage2.reducers";
    public static final String KEY_SALT_SEED        = "companion.salt.seed";
    public static final String KEY_RUN_TAG          = "companion.run.tag";

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
    public static int topN(Configuration c)        { return c.getInt(KEY_TOP_N, TOP_N_DEFAULT); }
    public static int stage1Reducers(Configuration c){return c.getInt(KEY_STAGE1_REDUCERS, STAGE1_REDUCERS_DEFAULT); }
    public static int stage2Reducers(Configuration c){return c.getInt(KEY_STAGE2_REDUCERS, STAGE2_REDUCERS_DEFAULT); }
    public static String runTag(Configuration c)   { return c.get(KEY_RUN_TAG, RUN_TAG_DEFAULT); }

    private CompanionConf() {}
}
