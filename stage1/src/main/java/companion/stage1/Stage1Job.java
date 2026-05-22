package companion.stage1;

import companion.conf.CompanionConf;
import companion.io.CompositeKey;
import companion.io.LocSlotWritable;
import companion.io.PairKey;
import companion.io.RecordWritable;
import companion.job.AbstractCompanionJob;
import companion.util.HashUtil;
import org.apache.hadoop.conf.Configuration;
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

    /** Partitions by (loc, slot/2), keeping slot 2k and 2k+1 together. */
    public static class SkewAwarePartitioner
            extends Partitioner<CompositeKey, RecordWritable> {
        @Override
        public int getPartition(CompositeKey key, RecordWritable value, int numPartitions) {
            if (numPartitions <= 0) {
                return 0;
            }
            return Math.floorMod(HashUtil.mix(key.getLoc(), key.getSlot() / 2), numPartitions);
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
        private int lastLoc = Integer.MIN_VALUE;
        private int lastSlot = Integer.MIN_VALUE;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            deltaT = CompanionConf.deltaT(conf);
            locSkewCap = CompanionConf.locSkewCap(conf);
        }

        @Override
        protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context context)
                throws IOException, InterruptedException {
            int loc = key.getLoc();
            int slot = key.getSlot();
            boolean canUseTail = loc == lastLoc && slot == lastSlot + 1 && lastSlot % 2 == 0;
            if (!canUseTail) {
                tailBuffer.clear();
            }

            window.clear();
            boolean hotCounterEmitted = false;
            List<SeenRecord> currentRecords = new ArrayList<>();

            for (RecordWritable value : values) {
                SeenRecord cur = new SeenRecord(value.getVid(), value.getTNorm());
                emitCrossSlotPairs(loc, slot, cur, canUseTail, context);
                emitWithinSlotPairs(loc, slot, cur, context);

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
            while (!window.isEmpty() && window.peekFirst().ts < cur.ts - deltaT) {
                window.pollFirst();
            }
            for (SeenRecord prev : window) {
                emitPair(prev.vid, cur.vid, loc, slot, context);
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
