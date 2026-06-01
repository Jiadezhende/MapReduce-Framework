package companion.stage1;

import companion.conf.CompanionConf;
import companion.io.CompositeKey;
import companion.io.LocSlotWritable;
import companion.io.PairKey;
import companion.io.RecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage1JobTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void partitionerKeepsEvenOddSlotPairTogether() {
        Stage1Job.SkewAwarePartitioner partitioner = new Stage1Job.SkewAwarePartitioner();
        RecordWritable value = new RecordWritable(1, 7, 0);

        int p0 = partitioner.getPartition(new CompositeKey(7, 0, 10), value, 17);
        int p1 = partitioner.getPartition(new CompositeKey(7, 1, 310), value, 17);
        int p2 = partitioner.getPartition(new CompositeKey(7, 2, 610), value, 17);

        assertEquals(p0, p1);
        assertTrue("slot 2 usually moves to a different slot/2 bucket", p0 != p2 || 17 == 1);
    }

    @Test
    public void localJobEmitsWithinAndCrossSlotWitnesses() throws Exception {
        Configuration conf = new Configuration(false);
        conf.setInt(CompanionConf.KEY_SLOT_SIZE, 300);
        conf.setInt(CompanionConf.KEY_DELTA_T, 300);
        conf.setInt(CompanionConf.KEY_LOC_SKEW_CAP, 100);
        conf.setInt(CompanionConf.KEY_STAGE1_REDUCERS, 1);
        conf.set("mapreduce.framework.name", "local");

        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(out, true);

        writeFilteredFixture(conf, new Path(in, "part-00000"));

        Stage1Job stage1 = new Stage1Job();
        stage1.setConf(conf);
        assertEquals(0, stage1.run(new String[]{in.toString(), out.toString()}));

        List<String> actualRows = readOutput(conf, out);
        Set<String> actual = new HashSet<>(actualRows);

        Set<String> expected = new HashSet<>();
        expected.add("1,2,7,0");
        expected.add("1,3,7,1");
        expected.add("2,3,7,1");
        expected.add("3,4,7,2");
        assertEquals(expected, actual);
        assertEquals("j1b should not duplicate within-slot witnesses", expected.size(), actualRows.size());
    }

    /**
     * vid-bucket reducer collapses multiple ts of the same vid into one bucket,
     * so (X, Y) with X having several ts in the slot must still emit only once.
     */
    @Test
    public void sameVidMultiTimestampInSlotEmittedOnce() throws Exception {
        Configuration conf = baseLocalConf();

        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        FileSystem.getLocal(conf).delete(out, true);

        try (SequenceFile.Writer writer = openWriter(conf, new Path(in, "part-00000"))) {
            RecordWritable value = new RecordWritable();
            // vid 1 appears 3 times in (loc=7, slot=0), vid 2 once.
            value.set(1, 7, 10);   writer.append(NullWritable.get(), value);
            value.set(1, 7, 100);  writer.append(NullWritable.get(), value);
            value.set(2, 7, 150);  writer.append(NullWritable.get(), value);
            value.set(1, 7, 200);  writer.append(NullWritable.get(), value);
        }

        Stage1Job stage1 = new Stage1Job();
        stage1.setConf(conf);
        assertEquals(0, stage1.run(new String[]{in.toString(), out.toString()}));

        List<String> actualRows = readOutput(conf, out);
        Set<String> expected = new HashSet<>();
        expected.add("1,2,7,0");
        assertEquals(expected, new HashSet<>(actualRows));
        assertEquals("pair (1,2) at (7,0) must be emitted exactly once despite 3 vid=1 records",
                1, actualRows.size());
    }

    /**
     * When a vid lives in both the previous slot's tail AND the current slot,
     * the within-slot pass covers any pair against it (slotSize <= deltaT
     * guarantees a within-slot ts pair is always within deltaT). The cross-slot
     * pass must skip such vids to avoid emitting an identical
     * (pair, loc, slot) witness twice.
     */
    @Test
    public void crossSlotOverlapVidNotDuplicated() throws Exception {
        Configuration conf = baseLocalConf();

        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        FileSystem.getLocal(conf).delete(out, true);

        try (SequenceFile.Writer writer = openWriter(conf, new Path(in, "part-00000"))) {
            RecordWritable value = new RecordWritable();
            // vid=1 spans slot 0 (ts=10) and slot 1 (ts=400);
            // vid=2 only in slot 1 (ts=500).
            value.set(1, 7, 10);   writer.append(NullWritable.get(), value);
            value.set(1, 7, 400);  writer.append(NullWritable.get(), value);
            value.set(2, 7, 500);  writer.append(NullWritable.get(), value);
        }

        Stage1Job stage1 = new Stage1Job();
        stage1.setConf(conf);
        assertEquals(0, stage1.run(new String[]{in.toString(), out.toString()}));

        List<String> actualRows = readOutput(conf, out);
        Set<String> expected = new HashSet<>();
        expected.add("1,2,7,1");
        assertEquals(expected, new HashSet<>(actualRows));
        assertEquals("(1,2,7,1) must not be duplicated by both within-slot and cross-slot passes",
                1, actualRows.size());
    }

    /**
     * Safety net for the invariant downgrade path: when deltaT < slotSize, the
     * within-slot pass may legitimately skip a pair whose only co-occurrence is
     * cross-slot. Cross-slot must still cover it (with possible duplicates,
     * absorbed by Stage2).
     */
    @Test
    public void deltaTLessThanSlotSizeStillCorrect() throws Exception {
        Configuration conf = baseLocalConf();
        conf.setInt(CompanionConf.KEY_DELTA_T, 200);
        conf.setInt(CompanionConf.KEY_SLOT_SIZE, 300);

        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        FileSystem.getLocal(conf).delete(out, true);

        try (SequenceFile.Writer writer = openWriter(conf, new Path(in, "part-00000"))) {
            RecordWritable value = new RecordWritable();
            // slot 0 (ts ∈ [0,300)): vid=1@10 and vid=2@290 — diff 280 > deltaT=200, no within-slot emit.
            // slot 1 (ts ∈ [300,600)): vid=3@350.
            //   cross-slot (vid 1@10 → vid 3@350): diff 340 > 200, no emit.
            //   cross-slot (vid 2@290 → vid 3@350): diff 60 ≤ 200, emit (2,3,7,1).
            value.set(1, 7, 10);   writer.append(NullWritable.get(), value);
            value.set(2, 7, 290);  writer.append(NullWritable.get(), value);
            value.set(3, 7, 350);  writer.append(NullWritable.get(), value);
        }

        Stage1Job stage1 = new Stage1Job();
        stage1.setConf(conf);
        assertEquals(0, stage1.run(new String[]{in.toString(), out.toString()}));

        Set<String> actual = new HashSet<>(readOutput(conf, out));
        Set<String> expected = new HashSet<>();
        expected.add("2,3,7,1");
        assertEquals(expected, actual);
    }

    private static Configuration baseLocalConf() {
        Configuration conf = new Configuration(false);
        conf.setInt(CompanionConf.KEY_SLOT_SIZE, 300);
        conf.setInt(CompanionConf.KEY_DELTA_T, 300);
        conf.setInt(CompanionConf.KEY_LOC_SKEW_CAP, 100);
        conf.setInt(CompanionConf.KEY_STAGE1_REDUCERS, 1);
        conf.set("mapreduce.framework.name", "local");
        return conf;
    }

    private static SequenceFile.Writer openWriter(Configuration conf, Path file) throws Exception {
        return SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(NullWritable.class),
                SequenceFile.Writer.valueClass(RecordWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }

    private static void writeFilteredFixture(Configuration conf, Path file) throws Exception {
        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(NullWritable.class),
                SequenceFile.Writer.valueClass(RecordWritable.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE))) {
            RecordWritable value = new RecordWritable();

            value.set(1, 7, 10);
            writer.append(NullWritable.get(), value);
            value.set(2, 7, 50);
            writer.append(NullWritable.get(), value);
            value.set(3, 7, 310);
            writer.append(NullWritable.get(), value);
            value.set(4, 7, 600);
            writer.append(NullWritable.get(), value);
            value.set(9, 8, 10);
            writer.append(NullWritable.get(), value);
        }
    }

    private static List<String> readOutput(Configuration conf, Path outDir) throws Exception {
        List<String> rows = new ArrayList<>();
        File dir = new File(outDir.toUri());
        File[] files = dir.listFiles((d, name) -> name.startsWith("part-"));
        if (files == null) {
            return rows;
        }
        for (File file : files) {
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path(file.toURI())))) {
                PairKey key = new PairKey();
                LocSlotWritable value = new LocSlotWritable();
                while (reader.next(key, value)) {
                    rows.add(key.getVidA() + "," + key.getVidB() + ","
                            + value.getLoc() + "," + value.getSlot());
                }
            }
        }
        return rows;
    }
}
