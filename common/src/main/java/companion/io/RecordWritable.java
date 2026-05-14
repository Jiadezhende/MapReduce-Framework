package companion.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Value type for the post-J0 filtered SequenceFile.
 * (vid, loc, t_norm) — t_norm is seconds since {@code CompanionConf#T0}.
 */
public class RecordWritable implements Writable {

    private int vid;
    private int loc;
    private int tNorm;

    public RecordWritable() {}

    public RecordWritable(int vid, int loc, int tNorm) {
        this.vid = vid;
        this.loc = loc;
        this.tNorm = tNorm;
    }

    public int getVid() { return vid; }
    public int getLoc() { return loc; }
    public int getTNorm() { return tNorm; }

    public void set(int vid, int loc, int tNorm) {
        this.vid = vid;
        this.loc = loc;
        this.tNorm = tNorm;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(vid);
        out.writeInt(loc);
        out.writeInt(tNorm);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.vid = in.readInt();
        this.loc = in.readInt();
        this.tNorm = in.readInt();
    }

    @Override
    public String toString() {
        return vid + "," + loc + "," + tNorm;
    }
}
