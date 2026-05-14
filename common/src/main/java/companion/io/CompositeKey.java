package companion.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * J1 secondary-sort key: (loc, slot, ts). Fixed-width 3 * int = 12 bytes,
 * binary-comparable big-endian so RawComparator avoids deserialization.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    public static final int SERIALIZED_LENGTH = 12;

    private int loc;
    private int slot;
    private int ts;

    public CompositeKey() {}

    public CompositeKey(int loc, int slot, int ts) {
        this.loc = loc;
        this.slot = slot;
        this.ts = ts;
    }

    public int getLoc() { return loc; }
    public int getSlot() { return slot; }
    public int getTs() { return ts; }

    public void set(int loc, int slot, int ts) {
        this.loc = loc;
        this.slot = slot;
        this.ts = ts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(loc);
        out.writeInt(slot);
        out.writeInt(ts);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.loc = in.readInt();
        this.slot = in.readInt();
        this.ts = in.readInt();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int c = Integer.compare(loc, o.loc);
        if (c != 0) return c;
        c = Integer.compare(slot, o.slot);
        if (c != 0) return c;
        return Integer.compare(ts, o.ts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeKey)) return false;
        CompositeKey c = (CompositeKey) o;
        return loc == c.loc && slot == c.slot && ts == c.ts;
    }

    @Override
    public int hashCode() {
        int h = loc;
        h = 31 * h + slot;
        h = 31 * h + ts;
        return h;
    }

    @Override
    public String toString() {
        return "(loc=" + loc + ", slot=" + slot + ", ts=" + ts + ")";
    }

    /** Raw byte-level comparator for shuffle-side full sort by (loc, slot, ts). */
    public static class FullKeyComparator extends WritableComparator {
        public FullKeyComparator() { super(CompositeKey.class); }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int loc1 = readInt(b1, s1);
            int loc2 = readInt(b2, s2);
            if (loc1 != loc2) return Integer.compare(loc1, loc2);
            int slot1 = readInt(b1, s1 + 4);
            int slot2 = readInt(b2, s2 + 4);
            if (slot1 != slot2) return Integer.compare(slot1, slot2);
            int ts1 = readInt(b1, s1 + 8);
            int ts2 = readInt(b2, s2 + 8);
            return Integer.compare(ts1, ts2);
        }
    }

    /** Grouping comparator: same reduce-call iff (loc, slot) match. */
    public static class LocSlotGroupComparator extends WritableComparator {
        public LocSlotGroupComparator() { super(CompositeKey.class); }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int loc1 = readInt(b1, s1);
            int loc2 = readInt(b2, s2);
            if (loc1 != loc2) return Integer.compare(loc1, loc2);
            int slot1 = readInt(b1, s1 + 4);
            int slot2 = readInt(b2, s2 + 4);
            return Integer.compare(slot1, slot2);
        }
    }

    static {
        WritableComparator.define(CompositeKey.class, new FullKeyComparator());
    }
}
