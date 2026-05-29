package companion.stage2;

import companion.conf.CompanionConf;
import companion.io.LocSlotWritable;
import companion.io.PairKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage2JobTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void partitionerKeepsSamePairTogether() {
        Stage2Job.PairPartitioner partitioner = new Stage2Job.PairPartitioner();
        PairKey key = new PairKey(7, 11);
        LocSlotWritable a = new LocSlotWritable(1, 2);
        LocSlotWritable b = new LocSlotWritable(99, 100);

        assertEquals(partitioner.getPartition(key, a, 31),
                partitioner.getPartition(key, b, 31));
        assertEquals(0, partitioner.getPartition(key, a, 0));
    }

    @Test
    public void localJobDeduplicatesWitnessesAndFiltersByKMin() throws Exception {
        Configuration conf = localConf();
        conf.setInt(CompanionConf.KEY_K_MIN, 3);
        conf.setInt(CompanionConf.KEY_HLL_THRESHOLD, 100);

        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(out, true);

        writePairLocSlotFixture(conf, new Path(in, "part-00000"), new Object[][]{
                {1, 2, 7, 0},
                {1, 2, 7, 0},
                {1, 2, 7, 1},
                {1, 2, 8, 1},
                {1, 3, 7, 0},
                {1, 3, 7, 1},
                {2, 3, 7, 0},
                {2, 3, 7, 1},
                {2, 3, 7, 2},
                {2, 3, 7, 2}
        });

        Job job = new Stage2Job().buildJob(conf, in, out);
        assertTrue(job.waitForCompletion(false));

        List<String> actual = readTextPartFiles(conf, out, false);
        Collections.sort(actual);

        List<String> expected = new ArrayList<>();
        expected.add("1,2,3");
        expected.add("2,3,3");
        Collections.sort(expected);
        assertEquals(expected, actual);

        assertEquals(10L, job.getCounters()
                .findCounter(Stage2Job.COUNTER_GROUP_STAGE2,
                        Stage2Job.Stage2Counter.PAIRS_INPUT.name()).getValue());
        assertEquals(2L, job.getCounters()
                .findCounter(Stage2Job.COUNTER_GROUP_STAGE2,
                        Stage2Job.Stage2Counter.PAIRS_OUTPUT.name()).getValue());
        assertEquals(0L, job.getCounters()
                .findCounter(Stage2Job.COUNTER_GROUP_STAGE2,
                        Stage2Job.Stage2Counter.HLL_FALLBACK_COUNT.name()).getValue());
    }

    @Test
    public void localJobWritesHllSideOutputWhenThresholdIsExceeded() throws Exception {
        Configuration conf = localConf();
        conf.setInt(CompanionConf.KEY_K_MIN, 1);
        conf.setInt(CompanionConf.KEY_HLL_THRESHOLD, 2);

        Path in = new Path(tmp.newFolder("hll-in").toURI());
        Path out = new Path(tmp.newFolder("hll-out").toURI());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(out, true);

        writePairLocSlotFixture(conf, new Path(in, "part-00000"), new Object[][]{
                {10, 20, 1, 0},
                {10, 20, 1, 1},
                {10, 20, 1, 2},
                {10, 20, 1, 3}
        });

        Job job = new Stage2Job().buildJob(conf, in, out);
        assertTrue(job.waitForCompletion(false));

        List<String> mainRows = readTextPartFiles(conf, out, false);
        assertEquals(1, mainRows.size());
        assertTrue(mainRows.get(0).startsWith("10,20,"));

        List<String> hllRows = readTextPartFiles(conf, new Path(out, "_hll_pairs"), true);
        assertEquals(1, hllRows.size());
        assertTrue(hllRows.get(0).startsWith("10,20,"));

        long estimatedCount = Long.parseLong(mainRows.get(0).split(",")[2]);
        assertTrue("small HLL estimate should stay close to 4, got " + estimatedCount,
                estimatedCount >= 3L && estimatedCount <= 5L);
        assertEquals(1L, job.getCounters()
                .findCounter(Stage2Job.COUNTER_GROUP_STAGE2,
                        Stage2Job.Stage2Counter.HLL_FALLBACK_COUNT.name()).getValue());
    }

    @Test
    public void localJobMatchesGoldenCompanionsFromGoldenPairFixture() throws Exception {
        Configuration conf = localConf();
        conf.setInt(CompanionConf.KEY_K_MIN, 3);
        conf.setInt(CompanionConf.KEY_HLL_THRESHOLD, 1_000_000);

        Path in = new Path(fixture("pair_loc_slot.seq").toURI());
        Path out = new Path(tmp.newFolder("golden-out").toURI());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(out, true);

        Job job = new Stage2Job().buildJob(conf, in, out);
        assertTrue(job.waitForCompletion(false));

        List<String> actual = readTextPartFiles(conf, out, false);
        Collections.sort(actual);

        List<String> expected = readGoldenCompanions();
        Collections.sort(expected);
        assertEquals(expected, actual);
    }

    private static Configuration localConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt(CompanionConf.KEY_STAGE2_REDUCERS, 1);
        return conf;
    }

    private static void writePairLocSlotFixture(Configuration conf, Path file, Object[][] rows)
            throws Exception {
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(PairKey.class),
                SequenceFile.Writer.valueClass(LocSlotWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE))) {
            PairKey key = new PairKey();
            LocSlotWritable value = new LocSlotWritable();
            for (Object[] row : rows) {
                key.set((Integer) row[0], (Integer) row[1]);
                value.set((Integer) row[2], (Integer) row[3]);
                writer.append(key, value);
            }
        }
    }

    private static List<String> readTextPartFiles(Configuration conf, Path path,
                                                  boolean requireDirectory) throws Exception {
        FileSystem fs = FileSystem.getLocal(conf);
        List<String> rows = new ArrayList<>();
        if (!fs.exists(path)) {
            if (requireDirectory) {
                return rows;
            }
            throw new IllegalArgumentException("missing path: " + path);
        }
        if (fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                String name = status.getPath().getName();
                if (name.startsWith("part-") || name.startsWith("part-m-")) {
                    rows.addAll(readTextPartFiles(conf, status.getPath(), false));
                }
            }
            return rows;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                fs.open(path), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    rows.add(line);
                }
            }
        }
        return rows;
    }

    private static List<String> readGoldenCompanions() throws Exception {
        List<String> rows = new ArrayList<>();
        File file = fixture("companions.csv");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                java.nio.file.Files.newInputStream(file.toPath()), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    rows.add(line);
                }
            }
        }
        return rows;
    }

    private static File fixture(String name) throws Exception {
        return new File(repoRoot(), "tests/data/fixtures/" + name);
    }

    private static File repoRoot() throws Exception {
        File cur = new File(".").getCanonicalFile();
        while (cur != null) {
            if (new File(cur, "tests/data/mini.csv").isFile()) {
                return cur;
            }
            cur = cur.getParentFile();
        }
        throw new IllegalStateException("repo root not found");
    }
}
