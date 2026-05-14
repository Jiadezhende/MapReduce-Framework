package companion.util;

/**
 * Deterministic mixers for partitioning and salting. We intentionally avoid
 * {@link Object#hashCode()} on Writables — those are JVM-specific and not
 * stable across runs.
 */
public final class HashUtil {

    private static final int MIX_32 = 0x9E3779B1;

    /** Stable 32-bit mix of two ints. */
    public static int mix(int a, int b) {
        int h = a * MIX_32;
        h ^= Integer.rotateLeft(b * MIX_32, 15);
        h *= MIX_32;
        return h ^ (h >>> 16);
    }

    /** Stable 32-bit mix of three ints. */
    public static int mix(int a, int b, int c) {
        int h = mix(a, b);
        h ^= Integer.rotateLeft(c * MIX_32, 11);
        h *= MIX_32;
        return h ^ (h >>> 16);
    }

    /** Pick a salt bucket in [0, saltN) from a (vidA, vidB) pair + seed. */
    public static int pairSaltBucket(int vidA, int vidB, int saltN, int seed) {
        int h = mix(vidA, vidB, seed);
        return Math.floorMod(h, saltN);
    }

    private HashUtil() {}
}
