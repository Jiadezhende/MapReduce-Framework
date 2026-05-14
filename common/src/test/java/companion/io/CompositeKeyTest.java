package companion.io;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class CompositeKeyTest {

    @Test
    public void roundTripPreservesFields() throws Exception {
        CompositeKey k = new CompositeKey(7, 42, 1234);
        DataOutputBuffer out = new DataOutputBuffer();
        k.write(out);
        assertEquals(CompositeKey.SERIALIZED_LENGTH, out.getLength());

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        CompositeKey back = new CompositeKey();
        back.readFields(in);

        assertEquals(7, back.getLoc());
        assertEquals(42, back.getSlot());
        assertEquals(1234, back.getTs());
        assertEquals(k, back);
        assertEquals(k.hashCode(), back.hashCode());
    }

    @Test
    public void naturalOrderingIsLocSlotTs() {
        CompositeKey a = new CompositeKey(1, 0, 0);
        CompositeKey b = new CompositeKey(1, 0, 5);
        CompositeKey c = new CompositeKey(1, 1, 0);
        CompositeKey d = new CompositeKey(2, 0, 0);

        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(c) < 0);
        assertTrue(c.compareTo(d) < 0);
        assertTrue(d.compareTo(a) > 0);
    }

    @Test
    public void rawComparatorMatchesNatural() throws Exception {
        CompositeKey[] xs = {
            new CompositeKey(0, 0, 0),
            new CompositeKey(0, 0, 1),
            new CompositeKey(0, 1, 0),
            new CompositeKey(1, 0, 0),
            new CompositeKey(1, 5, 999),
            new CompositeKey(1000, 1000, Integer.MAX_VALUE - 1),
        };

        WritableComparator raw = new CompositeKey.FullKeyComparator();
        for (CompositeKey a : xs) {
            for (CompositeKey b : xs) {
                int natural = Integer.signum(a.compareTo(b));
                int rawCmp = Integer.signum(compareSerialized(raw, a, b));
                assertEquals("mismatch for " + a + " vs " + b, natural, rawCmp);
            }
        }
    }

    @Test
    public void groupComparatorIgnoresTs() throws Exception {
        WritableComparator group = new CompositeKey.LocSlotGroupComparator();

        // Same (loc, slot), different ts → grouped together.
        assertEquals(0, compareSerialized(group,
                new CompositeKey(5, 9, 100),
                new CompositeKey(5, 9, 500)));

        // Different slot → not grouped.
        assertNotEquals(0, compareSerialized(group,
                new CompositeKey(5, 9, 100),
                new CompositeKey(5, 10, 100)));

        // Different loc → not grouped.
        assertNotEquals(0, compareSerialized(group,
                new CompositeKey(5, 9, 100),
                new CompositeKey(6, 9, 100)));
    }

    @Test
    public void registeredComparatorIsRaw() {
        WritableComparator c = WritableComparator.get(CompositeKey.class);
        assertTrue("expected FullKeyComparator, got " + c.getClass(),
                c instanceof CompositeKey.FullKeyComparator);
    }

    private static int compareSerialized(WritableComparator cmp, CompositeKey a, CompositeKey b) throws Exception {
        DataOutputBuffer oa = new DataOutputBuffer();
        DataOutputBuffer ob = new DataOutputBuffer();
        a.write(oa); b.write(ob);
        return cmp.compare(oa.getData(), 0, oa.getLength(),
                           ob.getData(), 0, ob.getLength());
    }

    @Test
    public void serializedBytesAreBigEndian() throws Exception {
        // 0x00000001 → bytes 00 00 00 01 (big-endian via DataOutput.writeInt).
        CompositeKey k = new CompositeKey(1, 2, 3);
        DataOutputBuffer out = new DataOutputBuffer();
        k.write(out);
        byte[] expected = {0,0,0,1, 0,0,0,2, 0,0,0,3};
        byte[] actual = new byte[out.getLength()];
        System.arraycopy(out.getData(), 0, actual, 0, out.getLength());
        assertArrayEquals(expected, actual);
    }
}
