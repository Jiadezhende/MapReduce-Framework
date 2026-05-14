package companion.io;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocSlotWritableTest {

    @Test
    public void roundTripPreservesFields() throws Exception {
        LocSlotWritable v = new LocSlotWritable(513, 4096);
        DataOutputBuffer out = new DataOutputBuffer();
        v.write(out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        LocSlotWritable back = new LocSlotWritable();
        back.readFields(in);

        assertEquals(v, back);
        assertEquals(513, back.getLoc());
        assertEquals(4096, back.getSlot());
    }

    @Test
    public void smallValuesUseFewBytes() throws Exception {
        // VInt encoding: single small int fits in 1 byte. (loc=5, slot=10) → 2 bytes.
        LocSlotWritable v = new LocSlotWritable(5, 10);
        DataOutputBuffer out = new DataOutputBuffer();
        v.write(out);
        assertTrue("expected <= 4 bytes for small values, got " + out.getLength(),
                out.getLength() <= 4);
    }
}
