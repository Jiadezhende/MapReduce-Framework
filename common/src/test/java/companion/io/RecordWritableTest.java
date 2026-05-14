package companion.io;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecordWritableTest {

    @Test
    public void roundTripPreservesFields() throws Exception {
        RecordWritable r = new RecordWritable(99, 7, 12345);
        DataOutputBuffer out = new DataOutputBuffer();
        r.write(out);
        assertEquals(12, out.getLength());

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        RecordWritable back = new RecordWritable();
        back.readFields(in);

        assertEquals(99, back.getVid());
        assertEquals(7, back.getLoc());
        assertEquals(12345, back.getTNorm());
    }
}
