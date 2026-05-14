package companion.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeUtilTest {

    @Test
    public void normalizeSubtractsT0() {
        assertEquals(0, TimeUtil.normalize(1420041600L, 1420041600));
        assertEquals(300, TimeUtil.normalize(1420041900L, 1420041600));
    }

    @Test
    public void slotIsFloorDivision() {
        assertEquals(0, TimeUtil.slot(0, 300));
        assertEquals(0, TimeUtil.slot(299, 300));
        assertEquals(1, TimeUtil.slot(300, 300));
        assertEquals(1, TimeUtil.slot(599, 300));
        assertEquals(2, TimeUtil.slot(600, 300));
    }

    @Test
    public void slotStartIsInverseOfSlot() {
        for (int t = 0; t < 10_000; t += 137) {
            int s = TimeUtil.slot(t, 300);
            int start = TimeUtil.slotStart(s, 300);
            assertEquals(s, TimeUtil.slot(start, 300));
        }
    }
}
