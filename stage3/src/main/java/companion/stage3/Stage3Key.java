package companion.stage3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Sort key for Stage3: count desc, vidA asc, vidB asc. */
public class Stage3Key implements WritableComparable<Stage3Key> {
    private long count;
    private int vidA;
    private int vidB;

    public Stage3Key() {
    }

    public Stage3Key(long count, int vidA, int vidB) {
        set(count, vidA, vidB);
    }

    public void set(long count, int vidA, int vidB) {
        this.count = count;
        this.vidA = vidA;
        this.vidB = vidB;
    }

    public long getCount() {
        return count;
    }

    public int getVidA() {
        return vidA;
    }

    public int getVidB() {
        return vidB;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeInt(vidA);
        out.writeInt(vidB);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        vidA = in.readInt();
        vidB = in.readInt();
    }

    @Override
    public int compareTo(Stage3Key other) {
        int cmp = Long.compare(other.count, this.count);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.vidA, other.vidA);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.vidB, other.vidB);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(count);
        result = 31 * result + Integer.hashCode(vidA);
        result = 31 * result + Integer.hashCode(vidB);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Stage3Key)) {
            return false;
        }
        Stage3Key other = (Stage3Key) obj;
        return count == other.count && vidA == other.vidA && vidB == other.vidB;
    }

    @Override
    public String toString() {
        return "Stage3Key{" + count + "," + vidA + "," + vidB + "}";
    }
}
