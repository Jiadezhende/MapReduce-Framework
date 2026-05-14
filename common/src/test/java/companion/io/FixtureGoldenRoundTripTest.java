package companion.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards the committed golden fixtures under {@code tests/data/fixtures/} against silent drift:
 * if {@link RecordWritable}, {@link PairKey}, or {@link LocSlotWritable} byte layouts change
 * without re-running {@code scripts/regenerate_fixtures.sh}, the .seq files will fail to
 * deserialize and this test fires.
 *
 * <p>Also re-runs the reference Stage 0/1/2 against the same prefix of {@code mini.csv} and
 * asserts the result equals the committed fixtures — catching changes to the reference
 * implementation without a re-commit.
 */
public class FixtureGoldenRoundTripTest {

    @Test
    public void filteredSeqDeserializesCleanly() throws IOException {
        File seq = fixture("filtered.seq");
        assertTrue("missing fixture: " + seq, seq.isFile());

        List<int[]> read = new ArrayList<>();
        Configuration conf = new Configuration();
        try (SequenceFile.Reader r = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(new Path(seq.toURI())))) {
            NullWritable k = NullWritable.get();
            RecordWritable v = new RecordWritable();
            while (r.next(k, v)) {
                read.add(new int[]{v.getVid(), v.getLoc(), v.getTNorm()});
            }
        }
        assertTrue("expected at least 1 record in filtered.seq", !read.isEmpty());

        List<int[]> expected = FixtureGenerator.stage0(miniCsv(), 1_420_041_600);
        assertEquals("record count mismatch — regenerate fixtures",
                expected.size(), read.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("vid mismatch at row " + i, expected.get(i)[0], read.get(i)[0]);
            assertEquals("loc mismatch at row " + i, expected.get(i)[1], read.get(i)[1]);
            assertEquals("tNorm mismatch at row " + i, expected.get(i)[2], read.get(i)[2]);
        }
    }

    @Test
    public void pairLocSlotSeqDeserializesCleanly() throws IOException {
        File seq = fixture("pair_loc_slot.seq");
        assertTrue("missing fixture: " + seq, seq.isFile());

        Set<Long> readWitnesses = new HashSet<>();
        int recordCount = 0;
        Configuration conf = new Configuration();
        try (SequenceFile.Reader r = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(new Path(seq.toURI())))) {
            PairKey k = new PairKey();
            LocSlotWritable v = new LocSlotWritable();
            while (r.next(k, v)) {
                recordCount++;
                assertTrue("PairKey violates vidA < vidB at record " + recordCount,
                        k.getVidA() < k.getVidB());
                readWitnesses.add(encode4(k.getVidA(), k.getVidB(), v.getLoc(), v.getSlot()));
            }
        }
        assertTrue("expected at least 1 record in pair_loc_slot.seq", recordCount > 0);

        List<int[]> filtered = FixtureGenerator.stage0(miniCsv(), 1_420_041_600);
        List<long[]> expected = FixtureGenerator.stage1(filtered, 300, 300);
        Set<Long> expectedSet = new HashSet<>();
        for (long[] w : expected) {
            expectedSet.add(encode4((int) w[0], (int) w[1], (int) w[2], (int) w[3]));
        }
        assertEquals("witness-set mismatch — regenerate fixtures or update Stage 1 semantics",
                expectedSet, readWitnesses);
    }

    @Test
    public void companionsCsvMatchesReference() throws IOException {
        File csv = fixture("companions.csv");
        assertTrue("missing fixture: " + csv, csv.isFile());

        List<String> readLines = Files.readAllLines(csv.toPath());
        assertTrue("expected at least 1 row in companions.csv", !readLines.isEmpty());

        List<int[]> filtered = FixtureGenerator.stage0(miniCsv(), 1_420_041_600);
        List<long[]> witnesses = FixtureGenerator.stage1(filtered, 300, 300);
        List<long[]> expected = FixtureGenerator.stage2(witnesses, 3);

        assertEquals("companions.csv row count mismatch", expected.size(), readLines.size());
        for (int i = 0; i < expected.size(); i++) {
            String line = readLines.get(i);
            String[] parts = line.split(",");
            assertEquals("companions.csv line " + i + " column count: " + line,
                    3, parts.length);
            assertEquals("vidA mismatch at row " + i, expected.get(i)[0], Long.parseLong(parts[0]));
            assertEquals("vidB mismatch at row " + i, expected.get(i)[1], Long.parseLong(parts[1]));
            assertEquals("count mismatch at row " + i, expected.get(i)[2], Long.parseLong(parts[2]));
        }
    }

    private static long encode4(int vidA, int vidB, int loc, int slot) {
        // Pack into two longs is overkill; for HashSet equality we just need a deterministic key.
        // We fold into one long under the assumption that all four fit in 16 bits each;
        // assert that here so a wider future fixture doesn't silently collide.
        assertWithin16Bits(vidA);
        assertWithin16Bits(vidB);
        assertWithin16Bits(loc);
        assertWithin16Bits(slot);
        return ((long) vidA << 48) | ((long) vidB << 32) | ((long) (loc & 0xffff) << 16) | (slot & 0xffff);
    }

    private static void assertWithin16Bits(int v) {
        if (v < 0 || v > 0xffff) {
            throw new AssertionError("value out of 16-bit range for fixture encoding: " + v);
        }
    }

    private static File fixture(String name) throws IOException {
        return new File(repoRoot(), "tests/data/fixtures/" + name);
    }

    private static File miniCsv() throws IOException {
        return new File(repoRoot(), "tests/data/mini.csv");
    }

    private static File repoRoot() throws IOException {
        File cur = new File(".").getCanonicalFile();
        while (cur != null) {
            if (new File(cur, "tests/data/mini.csv").isFile()) return cur;
            cur = cur.getParentFile();
        }
        throw new IllegalStateException("repo root not found");
    }
}
