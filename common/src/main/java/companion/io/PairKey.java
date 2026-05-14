package companion.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * J1 → J2 key for an unordered vehicle pair. Always stores vidA &lt; vidB
 * so equal pairs serialize identically.
 *
 * Fixed-width 8 bytes (2 * int). Big-endian so RawComparator is correct
 * via byte-wise lexicographic compare.
 */
public class PairKey implements WritableComparable<PairKey> {

    public static final int SERIALIZED_LENGTH = 8;

    private int vidA;
    private int vidB;

    public PairKey() {}

    public PairKey(int v1, int v2) {
        set(v1, v2);
    }

    /** Stores the pair in canonical order (smaller id first). */
    public void set(int v1, int v2) {
        if (v1 < v2) {
            this.vidA = v1;
            this.vidB = v2;
        } else if (v1 > v2) {
            this.vidA = v2;
            this.vidB = v1;
        } else {
            throw new IllegalArgumentException("PairKey requires distinct vids, got " + v1);
        }
    }

    public int getVidA() { return vidA; }
    public int getVidB() { return vidB; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(vidA);
        out.writeInt(vidB);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vidA = in.readInt();
        this.vidB = in.readInt();
    }

    @Override
    public int compareTo(PairKey o) {
        int c = Integer.compare(vidA, o.vidA);
        if (c != 0) return c;
        return Integer.compare(vidB, o.vidB);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PairKey)) return false;
        PairKey p = (PairKey) o;
        return vidA == p.vidA && vidB == p.vidB;
    }

    @Override
    public int hashCode() {
        return 31 * vidA + vidB;
    }

    @Override
    public String toString() {
        return "(" + vidA + "," + vidB + ")";
    }

    /** Raw byte-level comparator. Identical to natural ordering. */
    public static class Comparator extends WritableComparator {
        public Comparator() { super(PairKey.class); }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int a1 = readInt(b1, s1);
            int a2 = readInt(b2, s2);
            if (a1 != a2) return Integer.compare(a1, a2);
            int bb1 = readInt(b1, s1 + 4);
            int bb2 = readInt(b2, s2 + 4);
            return Integer.compare(bb1, bb2);
        }
    }

    static {
        WritableComparator.define(PairKey.class, new Comparator());
    }
}
