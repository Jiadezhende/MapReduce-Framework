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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/** Stage 1: generate pair witnesses from filtered vehicle records. */
public class Stage1Job extends AbstractCompanionJob {

    private static final String PASS_A = "j1a";
    private static final String PASS_B = "j1b";

    public enum Stage1Counter {
        INPUT_RECORDS,
        PAIRS_EMITTED,
        CROSS_SLOT_PAIRS,
        SKEW_DROP,
        HOT_LOCS_SLICED,
        DELTAT_LT_SLOTSIZE,
        MAX_GROUP_RECORDS,
        MAX_GROUP_DISTINCT_VIDS,
        CROSS_SLOT_SKIPPED_BY_OVERLAP
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
        job.setNumReduceTasks(CompanionConf.stage1Reducers(conf));

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

    /**
     * Reducer that aggregates records of a (loc, slot) group into a vid → ts-list
     * map, then enumerates distinct vid pairs and emits a witness whenever any
     * ts pair falls within deltaT. Does not maintain a cross-emit dedup set:
     * Stage2's reducer absorbs duplicate (pair, loc, slot) tuples via its
     * {@code HashSet<Long> exactWitnesses}, so any duplicates here only cost
     * shuffle volume, not correctness.
     */
    public static class Stage1Reducer
            extends Reducer<CompositeKey, RecordWritable, PairKey, LocSlotWritable> {

        private final Int2ObjectOpenHashMap<IntArrayList> currentVidToTs = new Int2ObjectOpenHashMap<>();
        private final Int2ObjectOpenHashMap<IntArrayList> tailVidToTs = new Int2ObjectOpenHashMap<>();
        private final PairKey outKey = new PairKey();
        private final LocSlotWritable outValue = new LocSlotWritable();

        private int deltaT;
        private int slotSize;
        private int locSkewCap;
        private int slotOffset;
        private boolean emitWithinSlot;
        private int lastLoc = Integer.MIN_VALUE;
        private int lastSlot = Integer.MIN_VALUE;

        // Per-reducer max counters; Hadoop counters only support increment, so we
        // track local maxima and emit once in cleanup via setValue.
        private long maxGroupRecords = 0L;
        private long maxGroupDistinctVids = 0L;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            deltaT = CompanionConf.deltaT(conf);
            slotSize = CompanionConf.slotSize(conf);
            locSkewCap = CompanionConf.locSkewCap(conf);
            slotOffset = Math.floorMod(CompanionConf.stage1SlotOffset(conf), 2);
            emitWithinSlot = CompanionConf.stage1EmitWithinSlot(conf);

            if (deltaT < slotSize) {
                // Invariant assumed by the cross-slot overlap-skip below is violated.
                // We don't fail — Stage2 still dedupes — but flag so we can spot it.
                context.getCounter(COUNTER_GROUP_STAGE1,
                        Stage1Counter.DELTAT_LT_SLOTSIZE.name()).increment(1L);
            }
        }

        @Override
        protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context context)
                throws IOException, InterruptedException {
            int loc = key.getLoc();
            int slot = key.getSlot();
            boolean canUseTail = loc == lastLoc && slot == lastSlot + 1
                    && Math.floorMod(lastSlot + slotOffset, 2) == 0;
            if (!canUseTail) {
                tailVidToTs.clear();
            }
            currentVidToTs.clear();

            // Pass 1: bucket records by vid. Inputs arrive sorted by tNorm
            // (CompositeKey.FullKeyComparator), so each per-vid IntArrayList is
            // appended in ascending order — the two-pointer scan below requires this.
            int totalRecords = 0;
            boolean hotCounterEmitted = false;
            for (RecordWritable value : values) {
                if (totalRecords >= locSkewCap) {
                    context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.SKEW_DROP.name()).increment(1L);
                    if (!hotCounterEmitted) {
                        context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.HOT_LOCS_SLICED.name()).increment(1L);
                        hotCounterEmitted = true;
                    }
                    continue;
                }
                int vid = value.getVid();
                int tNorm = value.getTNorm();
                IntArrayList tsList = currentVidToTs.get(vid);
                if (tsList == null) {
                    tsList = new IntArrayList(2);
                    currentVidToTs.put(vid, tsList);
                }
                tsList.add(tNorm);
                totalRecords++;
            }

            if (totalRecords > maxGroupRecords) {
                maxGroupRecords = totalRecords;
            }
            if (currentVidToTs.size() > maxGroupDistinctVids) {
                maxGroupDistinctVids = currentVidToTs.size();
            }

            if (emitWithinSlot) {
                emitWithinSlotPairs(loc, slot, context);
            }
            if (canUseTail) {
                emitCrossSlotPairs(loc, slot, context);
            }

            rebuildTailBuffer();
            lastLoc = loc;
            lastSlot = slot;
        }

        @Override
        protected void cleanup(Context context) {
            context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.MAX_GROUP_RECORDS.name())
                    .setValue(maxGroupRecords);
            context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.MAX_GROUP_DISTINCT_VIDS.name())
                    .setValue(maxGroupDistinctVids);
        }

        private void emitWithinSlotPairs(int loc, int slot, Context context)
                throws IOException, InterruptedException {
            // Symmetric enumeration over distinct vids in the current group.
            // Order doesn't matter — emitPair normalizes vidA<vidB internally.
            int[] vids = currentVidToTs.keySet().toIntArray();
            for (int i = 0; i < vids.length; i++) {
                IntArrayList tsA = currentVidToTs.get(vids[i]);
                for (int j = i + 1; j < vids.length; j++) {
                    IntArrayList tsB = currentVidToTs.get(vids[j]);
                    if (anyTsWithinDeltaT(tsA, tsB)) {
                        emitPair(vids[i], vids[j], loc, slot, context);
                    }
                }
            }
        }

        private void emitCrossSlotPairs(int loc, int slot, Context context)
                throws IOException, InterruptedException {
            // Iterate tail × current. Tail ts < current ts (different slots, sorted
            // input), so anyTsWithinDeltaT with abs() is equivalent to the original
            // single-sided cur.ts - prev.ts <= deltaT check.
            ObjectIterator<Int2ObjectMap.Entry<IntArrayList>> tailIt =
                    tailVidToTs.int2ObjectEntrySet().fastIterator();
            while (tailIt.hasNext()) {
                Int2ObjectMap.Entry<IntArrayList> tailEntry = tailIt.next();
                int vidTail = tailEntry.getIntKey();

                // Invariant: companion.delta.t >= companion.slot.size. Two records in
                // the same slot are at most slotSize apart, so when vidTail also lives
                // in currentVidToTs, the within-slot pass above will have already
                // emitted every (vidTail, vidCur) pair that could match. Skipping here
                // avoids a guaranteed duplicate. If the invariant is violated, the
                // DELTAT_LT_SLOTSIZE counter fires in setup; Stage2's HashSet still
                // absorbs duplicates correctly.
                if (currentVidToTs.containsKey(vidTail)) {
                    context.getCounter(COUNTER_GROUP_STAGE1,
                            Stage1Counter.CROSS_SLOT_SKIPPED_BY_OVERLAP.name()).increment(1L);
                    continue;
                }

                IntArrayList tsTail = tailEntry.getValue();
                ObjectIterator<Int2ObjectMap.Entry<IntArrayList>> curIt =
                        currentVidToTs.int2ObjectEntrySet().fastIterator();
                while (curIt.hasNext()) {
                    Int2ObjectMap.Entry<IntArrayList> curEntry = curIt.next();
                    if (anyTsWithinDeltaT(tsTail, curEntry.getValue())) {
                        emitPair(vidTail, curEntry.getIntKey(), loc, slot, context);
                        context.getCounter(COUNTER_GROUP_STAGE1,
                                Stage1Counter.CROSS_SLOT_PAIRS.name()).increment(1L);
                    }
                }
            }
        }

        /**
         * Two-pointer scan over two ascending ts lists. Returns true on the first
         * pair within deltaT. Worst case O(|a| + |b|).
         */
        private boolean anyTsWithinDeltaT(IntArrayList a, IntArrayList b) {
            int sizeA = a.size();
            int sizeB = b.size();
            int i = 0;
            int j = 0;
            while (i < sizeA && j < sizeB) {
                int diff = a.getInt(i) - b.getInt(j);
                if (Math.abs(diff) <= deltaT) {
                    return true;
                }
                if (diff < 0) {
                    i++;
                } else {
                    j++;
                }
            }
            return false;
        }

        private void emitPair(int vid1, int vid2, int loc, int slot, Context context)
                throws IOException, InterruptedException {
            if (vid1 == vid2) {
                return;
            }
            outKey.set(vid1, vid2);
            outValue.set(loc, slot);
            context.write(outKey, outValue);
            context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.PAIRS_EMITTED.name()).increment(1L);
        }

        /**
         * Move records with ts ≥ maxTs - deltaT from currentVidToTs into tailVidToTs.
         * With the typical deltaT >= slotSize config, this keeps the whole current
         * group as the tail for the next slot.
         */
        private void rebuildTailBuffer() {
            tailVidToTs.clear();
            if (currentVidToTs.isEmpty()) {
                return;
            }
            int maxTs = Integer.MIN_VALUE;
            ObjectIterator<Int2ObjectMap.Entry<IntArrayList>> scan =
                    currentVidToTs.int2ObjectEntrySet().fastIterator();
            while (scan.hasNext()) {
                IntArrayList ts = scan.next().getValue();
                int last = ts.getInt(ts.size() - 1);
                if (last > maxTs) {
                    maxTs = last;
                }
            }
            int minTailTs = maxTs - deltaT;

            ObjectIterator<Int2ObjectMap.Entry<IntArrayList>> it =
                    currentVidToTs.int2ObjectEntrySet().fastIterator();
            while (it.hasNext()) {
                Int2ObjectMap.Entry<IntArrayList> e = it.next();
                IntArrayList src = e.getValue();
                IntArrayList kept = null;
                int sz = src.size();
                for (int idx = 0; idx < sz; idx++) {
                    int t = src.getInt(idx);
                    if (t >= minTailTs) {
                        if (kept == null) {
                            kept = new IntArrayList(sz - idx);
                        }
                        kept.add(t);
                    }
                }
                if (kept != null) {
                    tailVidToTs.put(e.getIntKey(), kept);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new Stage1Job(), args);
        System.exit(code);
    }
}
