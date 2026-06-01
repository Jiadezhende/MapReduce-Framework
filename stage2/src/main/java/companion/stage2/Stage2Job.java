package companion.stage2;

import companion.conf.CompanionConf;
import companion.io.LocSlotWritable;
import companion.io.PairKey;
import companion.job.AbstractCompanionJob;
import companion.util.HashUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** Stage 2: count distinct (loc, slot) witnesses per vehicle pair. */
public class Stage2Job extends AbstractCompanionJob {

    private static final String HLL_SIDE_OUTPUT = "hllPairs";
    private static final String HLL_SIDE_OUTPUT_PATH = "_hll_pairs/part";
    private static final int HLL_PRECISION = 14;

    public enum Stage2Counter {
        PAIRS_INPUT,
        PAIRS_OUTPUT,
        HLL_FALLBACK_COUNT
    }

    @Override
    protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(Stage2Job.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, in);

        job.setMapperClass(Stage2Mapper.class);
        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(LocSlotWritable.class);

        job.setPartitionerClass(PairPartitioner.class);

        job.setReducerClass(Stage2Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(CompanionConf.stage2Reducers(conf));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);
        MultipleOutputs.addNamedOutput(job, HLL_SIDE_OUTPUT, TextOutputFormat.class,
                NullWritable.class, Text.class);

        return job;
    }

    public static class Stage2Mapper
            extends Mapper<PairKey, LocSlotWritable, PairKey, LocSlotWritable> {
        // Pair-hash sharding: when rounds > 1, this sub-job emits only the pairs
        // assigned to its round, so per-node shuffle peak is ~1/rounds. A pair's
        // every witness shares (vidA, vidB), so mix() is constant → all witnesses
        // of a pair land in exactly one round (rounds form a disjoint partition).
        private int rounds;
        private int round;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            rounds = CompanionConf.stage2Rounds(conf);
            round = CompanionConf.stage2Round(conf);
        }

        @Override
        protected void map(PairKey key, LocSlotWritable value, Context context)
                throws IOException, InterruptedException {
            if (rounds > 1
                    && Math.floorMod(HashUtil.mix(key.getVidA(), key.getVidB()), rounds) != round) {
                return;
            }
            context.write(key, value);
            context.getCounter(COUNTER_GROUP_STAGE2,
                    Stage2Counter.PAIRS_INPUT.name()).increment(1L);
        }
    }

    /** Single-stage partitioning: every witness for the same pair reaches one reducer. */
    public static class PairPartitioner
            extends Partitioner<PairKey, LocSlotWritable> {
        @Override
        public int getPartition(PairKey key, LocSlotWritable value, int numPartitions) {
            if (numPartitions <= 0) {
                return 0;
            }
            return Math.floorMod(HashUtil.mix(key.getVidA(), key.getVidB()), numPartitions);
        }
    }

    public static class Stage2Reducer
            extends Reducer<PairKey, LocSlotWritable, NullWritable, Text> {

        private final Text outText = new Text();
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        private int kMin;
        private int hllThreshold;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            kMin = CompanionConf.kMin(conf);
            hllThreshold = CompanionConf.hllThreshold(conf);
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(PairKey key, Iterable<LocSlotWritable> values, Context context)
                throws IOException, InterruptedException {
            Set<Long> exactWitnesses = new HashSet<>();
            HyperLogLog hll = null;

            for (LocSlotWritable value : values) {
                long witness = encodeWitness(value.getLoc(), value.getSlot());
                if (hll == null) {
                    exactWitnesses.add(witness);
                    if (shouldSwitchToHll(exactWitnesses.size())) {
                        hll = new HyperLogLog(HLL_PRECISION);
                        for (Long exactWitness : exactWitnesses) {
                            hll.add(exactWitness);
                        }
                        exactWitnesses.clear();
                        context.getCounter(COUNTER_GROUP_STAGE2,
                                Stage2Counter.HLL_FALLBACK_COUNT.name()).increment(1L);
                    }
                } else {
                    hll.add(witness);
                }
            }

            long count = hll == null ? exactWitnesses.size() : hll.estimate();
            if (hll != null) {
                writeHllPair(key, count);
            }
            if (count >= kMin) {
                outText.set(key.getVidA() + "," + key.getVidB() + "," + count);
                context.write(NullWritable.get(), outText);
                context.getCounter(COUNTER_GROUP_STAGE2,
                        Stage2Counter.PAIRS_OUTPUT.name()).increment(1L);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        private boolean shouldSwitchToHll(int exactSize) {
            return hllThreshold >= 0 && exactSize > hllThreshold;
        }

        private void writeHllPair(PairKey key, long count) throws IOException, InterruptedException {
            outText.set(key.getVidA() + "," + key.getVidB() + "," + count);
            multipleOutputs.write(HLL_SIDE_OUTPUT, NullWritable.get(), outText,
                    HLL_SIDE_OUTPUT_PATH);
        }
    }

    private static long encodeWitness(int loc, int slot) {
        return ((long) loc << 32) ^ (slot & 0xffffffffL);
    }

    private static int decodeLoc(long witness) {
        return (int) (witness >> 32);
    }

    private static int decodeSlot(long witness) {
        return (int) witness;
    }

    private static final class HyperLogLog {
        private final byte[] registers;
        private final int precision;
        private final int registerCount;
        private final double alphaMM;

        private HyperLogLog(int precision) {
            if (precision < 4 || precision > 16) {
                throw new IllegalArgumentException("precision must be in [4, 16]");
            }
            this.precision = precision;
            this.registerCount = 1 << precision;
            this.registers = new byte[registerCount];
            this.alphaMM = alpha(registerCount) * registerCount * registerCount;
        }

        private void add(long value) {
            long hash = mix64(value);
            int index = (int) (hash >>> (64 - precision));
            long remaining = hash << precision;
            int rank = Long.numberOfLeadingZeros(remaining) + 1;
            int maxRank = 64 - precision + 1;
            if (rank > maxRank) {
                rank = maxRank;
            }
            if (registers[index] < rank) {
                registers[index] = (byte) rank;
            }
        }

        private long estimate() {
            double sum = 0.0d;
            int zeros = 0;
            for (byte register : registers) {
                int value = register & 0xff;
                sum += Math.scalb(1.0d, -value);
                if (value == 0) {
                    zeros++;
                }
            }

            double estimate = alphaMM / sum;
            if (estimate <= 2.5d * registerCount && zeros > 0) {
                estimate = registerCount * Math.log((double) registerCount / zeros);
            }
            return Math.max(0L, Math.round(estimate));
        }

        private static double alpha(int m) {
            switch (m) {
                case 16:
                    return 0.673d;
                case 32:
                    return 0.697d;
                case 64:
                    return 0.709d;
                default:
                    return 0.7213d / (1.0d + 1.079d / m);
            }
        }

        private static long mix64(long value) {
            long x = value + 0x9E3779B97F4A7C15L;
            x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
            x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
            return x ^ (x >>> 31);
        }
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new Stage2Job(), args);
        System.exit(code);
    }
}
