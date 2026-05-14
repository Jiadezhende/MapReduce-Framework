package companion.io;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PairKeyTest {

    @Test
    public void setCanonicalizesOrder() {
        PairKey a = new PairKey(7, 3);
        assertEquals(3, a.getVidA());
        assertEquals(7, a.getVidB());

        PairKey b = new PairKey(3, 7);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void rejectsEqualVids() {
        try {
            new PairKey(5, 5);
            fail("expected IllegalArgumentException for equal vids");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void roundTripPreservesPair() throws Exception {
        PairKey k = new PairKey(12345, 67890);
        DataOutputBuffer out = new DataOutputBuffer();
        k.write(out);
        assertEquals(PairKey.SERIALIZED_LENGTH, out.getLength());

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        PairKey back = new PairKey();
        back.readFields(in);

        assertEquals(k, back);
        assertEquals(12345, back.getVidA());
        assertEquals(67890, back.getVidB());
    }

    @Test
    public void naturalOrderingIsVidAVidB() {
        PairKey p1 = new PairKey(1, 2);
        PairKey p2 = new PairKey(1, 3);
        PairKey p3 = new PairKey(2, 3);

        assertTrue(p1.compareTo(p2) < 0);
        assertTrue(p2.compareTo(p3) < 0);
        assertTrue(p1.compareTo(p3) < 0);
    }

    @Test
    public void rawComparatorMatchesNatural() throws Exception {
        PairKey[] xs = {
            new PairKey(0, 1),
            new PairKey(0, 2),
            new PairKey(1, 2),
            new PairKey(1, 1_000_000),
            new PairKey(999_999, 1_000_000),
        };
        WritableComparator raw = new PairKey.Comparator();
        for (PairKey a : xs) {
            for (PairKey b : xs) {
                int natural = Integer.signum(a.compareTo(b));
                int rawCmp = Integer.signum(compareSerialized(raw, a, b));
                assertEquals("mismatch for " + a + " vs " + b, natural, rawCmp);
            }
        }
    }

    @Test
    public void distinctPairsAreNotEqual() {
        assertNotEquals(new PairKey(1, 2), new PairKey(1, 3));
        assertNotEquals(new PairKey(1, 2), new PairKey(2, 3));
    }

    @Test
    public void registeredComparatorIsRaw() {
        WritableComparator c = WritableComparator.get(PairKey.class);
        assertTrue("expected PairKey.Comparator, got " + c.getClass(),
                c instanceof PairKey.Comparator);
    }

    private static int compareSerialized(WritableComparator cmp, PairKey a, PairKey b) throws Exception {
        DataOutputBuffer oa = new DataOutputBuffer();
        DataOutputBuffer ob = new DataOutputBuffer();
        a.write(oa); b.write(ob);
        return cmp.compare(oa.getData(), 0, oa.getLength(),
                           ob.getData(), 0, ob.getLength());
    }
}
