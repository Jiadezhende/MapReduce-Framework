package companion.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HashUtilTest {

    @Test
    public void mixIsDeterministic() {
        assertEquals(HashUtil.mix(1, 2), HashUtil.mix(1, 2));
        assertEquals(HashUtil.mix(1, 2, 3), HashUtil.mix(1, 2, 3));
    }

    @Test
    public void mixIsAsymmetricInArgs() {
        // Order matters — guards against accidentally using a symmetric mixer.
        assertTrue(HashUtil.mix(1, 2) != HashUtil.mix(2, 1));
    }

    @Test
    public void saltBucketIsInRange() {
        for (int v1 = 0; v1 < 100; v1++) {
            for (int v2 = v1 + 1; v2 < 100; v2++) {
                int b = HashUtil.pairSaltBucket(v1, v2, 16, 7);
                assertTrue("bucket out of range: " + b, b >= 0 && b < 16);
            }
        }
    }

    @Test
    public void saltDistributionIsRoughlyUniform() {
        // 10,000 random pairs into 16 buckets — each bucket should get
        // within 50% of the mean. Generous bound; just catches a broken mixer.
        int n = 10_000;
        int saltN = 16;
        Map<Integer, Integer> hist = new HashMap<>();
        for (int i = 0; i < n; i++) {
            int b = HashUtil.pairSaltBucket(i * 31, i * 31 + 1, saltN, 0);
            hist.merge(b, 1, Integer::sum);
        }
        double mean = n / (double) saltN;
        for (Map.Entry<Integer, Integer> e : hist.entrySet()) {
            double dev = Math.abs(e.getValue() - mean) / mean;
            assertTrue("bucket " + e.getKey() + " skewed: " + e.getValue() + " vs mean " + mean,
                    dev < 0.5);
        }
    }
}
