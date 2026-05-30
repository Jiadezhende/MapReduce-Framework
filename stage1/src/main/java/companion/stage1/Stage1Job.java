package companion.stage1;

import companion.conf.CompanionConf;
import companion.io.CompositeKey;
import companion.io.LocSlotWritable;
import companion.io.PairKey;
import companion.io.RecordWritable;
import companion.job.AbstractCompanionJob;
import companion.util.HashUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Stage 1: generate pair witnesses from filtered vehicle records. */
public class Stage1Job extends AbstractCompanionJob {

    private static final String PASS_A = "j1a";
    private static final String PASS_B = "j1b";

    public enum Stage1Counter {
        INPUT_RECORDS,
        PAIRS_EMITTED,
        CROSS_SLOT_PAIRS,
        SKEW_DROP,
        HOT_LOCS_SLICED
    }

    @Override
    protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(Stage1Job.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, in);

        job.setMapperClass(Stage1Mapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(RecordWritable.class);

        job.setSortComparatorClass(CompositeKey.FullKeyComparator.class);
        job.setGroupingComparatorClass(CompositeKey.LocSlotGroupComparator.class);
        job.setPartitionerClass(SkewAwarePartitioner.class);

        job.setReducerClass(Stage1Reducer.class);
        job.setOutputKeyClass(PairKey.class);
        job.setOutputValueClass(LocSlotWritable.class);
        job.setNumReduceTasks(conf.getInt(MRJobConfig.NUM_REDUCES, CompanionConf.stage1Reducers(conf)));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, out);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: " + jobName() + " <input> <output> [-Dkey=value ...]");
            return 2;
        }

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
        }
        CompanionConf.applyDefaults(conf);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        if (!CompanionConf.stage1CompensationEnabled(conf)) {
            return runSinglePass(conf, in, out, CompanionConf.stage1SlotOffset(conf), "");
        }

        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            System.err.println("Output path already exists: " + out);
            return 1;
        }

        Path tmpRoot = new Path(out.toString() + "._stage1_tmp_" + System.currentTimeMillis());
        Path passAOut = new Path(tmpRoot, PASS_A);
        Path passBOut = new Path(tmpRoot, PASS_B);
        try {
            int passA = runSinglePass(conf, in, passAOut, 0, true, PASS_A);
            if (passA != 0) {
                return passA;
            }
            int passB = runSinglePass(conf, in, passBOut, 1, false, PASS_B);
            if (passB != 0) {
                return passB;
            }
            materializeOutput(fs, out, passAOut, passBOut);
            return 0;
        } finally {
            fs.delete(tmpRoot, true);
        }
    }

    private int runSinglePass(Configuration baseConf, Path in, Path out, int slotOffset, String passName)
            throws Exception {
        return runSinglePass(baseConf, in, out, slotOffset, true, passName);
    }

    private int runSinglePass(Configuration baseConf, Path in, Path out, int slotOffset,
                              boolean emitWithinSlot, String passName)
            throws Exception {
        Configuration passConf = new Configuration(baseConf);
        passConf.setInt(CompanionConf.KEY_STAGE1_SLOT_OFFSET, Math.floorMod(slotOffset, 2));
        passConf.setBoolean(CompanionConf.KEY_STAGE1_EMIT_WITHIN_SLOT, emitWithinSlot);

        log.info("Submitting {}{}: in={} out={} slotOffset={} emitWithinSlot={}", jobName(),
                passName.isEmpty() ? "" : " " + passName, in, out, Math.floorMod(slotOffset, 2),
                emitWithinSlot);
        Job job = buildJob(passConf, in, out);
        String tag = CompanionConf.runTag(passConf);
        String name = passName.isEmpty() ? jobName() : jobName() + " " + passName;
        job.setJobName(tag.isEmpty() ? name : name + " [" + tag + "]");

        boolean ok = job.waitForCompletion(true);
        return ok ? 0 : 1;
    }

    private static void materializeOutput(FileSystem fs, Path out, Path passAOut, Path passBOut)
            throws IOException {
        fs.mkdirs(out);
        movePartFiles(fs, passAOut, out, PASS_A);
        movePartFiles(fs, passBOut, out, PASS_B);
        fs.create(new Path(out, "_SUCCESS"), false).close();
    }

    private static void movePartFiles(FileSystem fs, Path passOut, Path out, String prefix)
            throws IOException {
        FileStatus[] files = fs.listStatus(passOut, path -> path.getName().startsWith("part-"));
        if (files == null) {
            return;
        }
        for (FileStatus file : files) {
            Path src = file.getPath();
            Path dst = new Path(out, "part-" + prefix + "-" + src.getName().substring("part-".length()));
            if (!fs.rename(src, dst)) {
                throw new IOException("Failed to move " + src + " to " + dst);
            }
        }
    }

    public static class Stage1Mapper
            extends Mapper<NullWritable, RecordWritable, CompositeKey, RecordWritable> {

        private final CompositeKey outKey = new CompositeKey();
        private final RecordWritable outValue = new RecordWritable();
        private int slotSize;

        @Override
        protected void setup(Context context) {
            slotSize = CompanionConf.slotSize(context.getConfiguration());
        }

        @Override
        protected void map(NullWritable key, RecordWritable value, Context context)
                throws IOException, InterruptedException {
            int tNorm = value.getTNorm();
            int slot = tNorm / slotSize;
            outKey.set(value.getLoc(), slot, tNorm);
            outValue.set(value.getVid(), value.getLoc(), tNorm);
            context.write(outKey, outValue);
            context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.INPUT_RECORDS.name()).increment(1L);
        }
    }

    /**
     * Partitions by (loc, (slot + offset) / 2). Offset 0 keeps 2k and 2k+1
     * together; offset 1 keeps 2k+1 and 2k+2 together for the compensation pass.
     */
    public static class SkewAwarePartitioner
            extends Partitioner<CompositeKey, RecordWritable> implements Configurable {
        private Configuration conf;
        private int slotOffset;

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            this.slotOffset = Math.floorMod(CompanionConf.stage1SlotOffset(conf), 2);
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public int getPartition(CompositeKey key, RecordWritable value, int numPartitions) {
            if (numPartitions <= 0) {
                return 0;
            }
            return Math.floorMod(HashUtil.mix(key.getLoc(), (key.getSlot() + slotOffset) / 2), numPartitions);
        }
    }

    public static class Stage1Reducer
            extends Reducer<CompositeKey, RecordWritable, PairKey, LocSlotWritable> {

        private final Deque<SeenRecord> window = new ArrayDeque<>();
        private final List<SeenRecord> tailBuffer = new ArrayList<>();
        private final PairKey outKey = new PairKey();
        private final LocSlotWritable outValue = new LocSlotWritable();

        private int deltaT;
        private int locSkewCap;
        private int slotOffset;
        private boolean emitWithinSlot;
        private int lastLoc = Integer.MIN_VALUE;
        private int lastSlot = Integer.MIN_VALUE;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            deltaT = CompanionConf.deltaT(conf);
            locSkewCap = CompanionConf.locSkewCap(conf);
            slotOffset = Math.floorMod(CompanionConf.stage1SlotOffset(conf), 2);
            emitWithinSlot = CompanionConf.stage1EmitWithinSlot(conf);
        }

        @Override
        protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context context)
                throws IOException, InterruptedException {
            int loc = key.getLoc();
            int slot = key.getSlot();
            boolean canUseTail = loc == lastLoc && slot == lastSlot + 1
                    && Math.floorMod(lastSlot + slotOffset, 2) == 0;
            if (!canUseTail) {
                tailBuffer.clear();
            }

            window.clear();
            boolean hotCounterEmitted = false;
            List<SeenRecord> currentRecords = new ArrayList<>();

            for (RecordWritable value : values) {
                SeenRecord cur = new SeenRecord(value.getVid(), value.getTNorm());
                emitCrossSlotPairs(loc, slot, cur, canUseTail, context);
                pruneWindow(cur);
                if (emitWithinSlot) {
                    emitWithinSlotPairs(loc, slot, cur, context);
                }

                if (window.size() >= locSkewCap) {
                    context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.SKEW_DROP.name()).increment(1L);
                    if (!hotCounterEmitted) {
                        context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.HOT_LOCS_SLICED.name()).increment(1L);
                        hotCounterEmitted = true;
                    }
                    continue;
                }
                window.addLast(cur);
                currentRecords.add(cur);
            }

            rebuildTailBuffer(currentRecords);
            lastLoc = loc;
            lastSlot = slot;
        }

        private void emitCrossSlotPairs(int loc, int slot, SeenRecord cur, boolean canUseTail,
                                        Context context)
                throws IOException, InterruptedException {
            if (!canUseTail) {
                return;
            }
            for (SeenRecord prev : tailBuffer) {
                if (cur.ts - prev.ts > deltaT) {
                    continue;
                }
                if (emitPair(prev.vid, cur.vid, loc, slot, context)) {
                    context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.CROSS_SLOT_PAIRS.name()).increment(1L);
                }
            }
        }

        private void emitWithinSlotPairs(int loc, int slot, SeenRecord cur, Context context)
                throws IOException, InterruptedException {
            for (SeenRecord prev : window) {
                emitPair(prev.vid, cur.vid, loc, slot, context);
            }
        }

        private void pruneWindow(SeenRecord cur) {
            while (!window.isEmpty() && window.peekFirst().ts < cur.ts - deltaT) {
                window.pollFirst();
            }
        }

        private boolean emitPair(int vid1, int vid2, int loc, int slot, Context context)
                throws IOException, InterruptedException {
            if (vid1 == vid2) {
                return false;
            }
            outKey.set(vid1, vid2);
            outValue.set(loc, slot);
            context.write(outKey, outValue);
            context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.PAIRS_EMITTED.name()).increment(1L);
            return true;
        }

        private void rebuildTailBuffer(List<SeenRecord> currentRecords) {
            tailBuffer.clear();
            if (currentRecords.isEmpty()) {
                return;
            }
            int maxTs = currentRecords.get(currentRecords.size() - 1).ts;
            int minTailTs = maxTs - deltaT;
            for (SeenRecord record : currentRecords) {
                if (record.ts >= minTailTs) {
                    tailBuffer.add(record);
                }
            }
        }
    }

    private static final class SeenRecord {
        private final int vid;
        private final int ts;

        private SeenRecord(int vid, int ts) {
            this.vid = vid;
            this.ts = ts;
        }
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new Stage1Job(), args);
        System.exit(code);
    }
}
