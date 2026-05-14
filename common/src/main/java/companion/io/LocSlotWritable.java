package companion.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Value type for J1 output / J2 input: a (loc, slot) co-occurrence witness.
 * VInt-encoded to minimize shuffle volume — most loc/slot ids are small.
 */
public class LocSlotWritable implements Writable {

    private int loc;
    private int slot;

    public LocSlotWritable() {}

    public LocSlotWritable(int loc, int slot) {
        this.loc = loc;
        this.slot = slot;
    }

    public int getLoc() { return loc; }
    public int getSlot() { return slot; }

    public void set(int loc, int slot) {
        this.loc = loc;
        this.slot = slot;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, loc);
        WritableUtils.writeVInt(out, slot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.loc = WritableUtils.readVInt(in);
        this.slot = WritableUtils.readVInt(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LocSlotWritable)) return false;
        LocSlotWritable l = (LocSlotWritable) o;
        return loc == l.loc && slot == l.slot;
    }

    @Override
    public int hashCode() {
        return 31 * loc + slot;
    }

    @Override
    public String toString() {
        return "(loc=" + loc + ", slot=" + slot + ")";
    }
}
