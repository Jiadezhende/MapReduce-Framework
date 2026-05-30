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
