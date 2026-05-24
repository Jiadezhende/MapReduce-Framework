package companion.stage0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

final class Stage0Bloom {
    static final String KEY_EXPECTED_ENTRIES = "companion.stage0.bloom.expected.entries";
    static final String KEY_FALSE_POSITIVE_RATE = "companion.stage0.bloom.false.positive.rate";

    private static final long EXPECTED_ENTRIES_DEFAULT = 25_000_000L;
    private static final double FALSE_POSITIVE_RATE_DEFAULT = 1.0e-9d;
    private static final double LN_2 = Math.log(2.0d);

    private Stage0Bloom() {
    }

    static BloomFilter create(Configuration conf) {
        long expectedEntries = conf.getLong(KEY_EXPECTED_ENTRIES, EXPECTED_ENTRIES_DEFAULT);
        double falsePositiveRate = conf.getFloat(KEY_FALSE_POSITIVE_RATE,
                (float) FALSE_POSITIVE_RATE_DEFAULT);
        if (expectedEntries <= 0L) {
            throw new IllegalArgumentException(KEY_EXPECTED_ENTRIES + " must be positive");
        }
        if (!(falsePositiveRate > 0.0d && falsePositiveRate < 1.0d)) {
            throw new IllegalArgumentException(KEY_FALSE_POSITIVE_RATE + " must be in (0, 1)");
        }

        long vectorSize = (long) Math.ceil(-expectedEntries * Math.log(falsePositiveRate)
                / (LN_2 * LN_2));
        if (vectorSize <= 0L || vectorSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Bloom vector size out of range: " + vectorSize);
        }
        int nbHash = Math.max(1, (int) Math.round((double) vectorSize / expectedEntries * LN_2));
        return new BloomFilter((int) vectorSize, nbHash, Hash.MURMUR_HASH);
    }

    static void setVid(Key key, byte[] bytes, int vid) {
        bytes[0] = (byte) (vid >>> 24);
        bytes[1] = (byte) (vid >>> 16);
        bytes[2] = (byte) (vid >>> 8);
        bytes[3] = (byte) vid;
        key.set(bytes, 1.0d);
    }

    static void applyTrailingDefines(Configuration conf, String[] args) {
        if (conf == null) {
            return;
        }
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("-D")) {
                continue;
            }
            String define = arg.substring(2).trim();
            if (define.isEmpty() && i + 1 < args.length) {
                define = args[++i].trim();
            }
            int eq = define.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = define.substring(0, eq).trim();
            String value = define.substring(eq + 1).trim();
            if (!key.isEmpty()) {
                conf.set(key, value);
            }
        }
    }
}
