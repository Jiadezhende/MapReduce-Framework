package companion.util;

/** Time-axis helpers. ts → t_norm (seconds since T0) → slot id. */
public final class TimeUtil {

    /** Normalize an absolute unix-second timestamp to seconds-since-T0. */
    public static int normalize(long ts, int t0) {
        return (int) (ts - t0);
    }

    /** Slot id for a normalized timestamp under a given slot width. */
    public static int slot(int tNorm, int slotSize) {
        return Math.floorDiv(tNorm, slotSize);
    }

    /** Inverse: slot start (in t_norm). */
    public static int slotStart(int slot, int slotSize) {
        return slot * slotSize;
    }

    private TimeUtil() {}
}
