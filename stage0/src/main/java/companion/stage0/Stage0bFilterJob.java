package companion.stage0;

import companion.conf.CompanionConf;
import companion.io.RecordWritable;
import companion.job.AbstractCompanionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;
import java.net.URI;

public class Stage0bFilterJob extends AbstractCompanionJob {
    public static final String KEY_VID_FREQ_PATH = "companion.vid_freq.path";

    @Override
    public int run(String[] args) throws Exception {
        Stage0Bloom.applyTrailingDefines(getConf(), args);
        return super.run(args);
    }

    @Override
    protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
        String vidFreqPath = vidFreqPath(conf);
        if (vidFreqPath == null || vidFreqPath.trim().isEmpty()) {
            throw new IllegalArgumentException(KEY_VID_FREQ_PATH + " is required");
        }

        Job job = Job.getInstance(conf, jobName());
        job.setJarByClass(Stage0bFilterJob.class);

        Path freq = new Path(vidFreqPath);
        FileSystem fs = freq.getFileSystem(conf);
        int cacheFiles = 0;
        for (FileStatus status : fs.listStatus(freq)) {
            if (status.getPath().getName().startsWith("part-")) {
                job.addCacheFile(status.getPath().toUri());
                cacheFiles++;
            }
        }
        if (cacheFiles == 0) {
            throw new IllegalArgumentException(KEY_VID_FREQ_PATH + " has no part-* files: " + vidFreqPath);
        }

        job.setMapperClass(FilterMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(RecordWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(RecordWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
        return job;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Stage0bFilterJob(), args));
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, NullWritable, RecordWritable> {
        private final RecordWritable out = new RecordWritable();
        private final Key bloomKey = new Key();
        private final byte[] bloomKeyBytes = new byte[4];
        private BloomFilter keptVids;
        private int t0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            t0 = CompanionConf.t0(conf);
            Path[] localCacheFiles = context.getLocalCacheFiles();
            if (localCacheFiles != null && localCacheFiles.length > 0) {
                for (Path path : localCacheFiles) {
                    loadBloomFile(conf, path);
                }
            }
            if (keptVids == null) {
                URI[] cacheFiles = context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    for (URI uri : cacheFiles) {
                        loadBloomFile(conf, new Path(uri));
                    }
                }
            }
            if (keptVids == null) {
                throw new IOException("No BloomFilter files found in DistributedCache");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Stage0CsvParser.ParseResult parsed = Stage0CsvParser.parseRecord(value, t0);
            if (!parsed.isOk()) {
                context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.PARSE_FAIL.name()).increment(1);
                return;
            }
            Stage0Bloom.setVid(bloomKey, bloomKeyBytes, parsed.vid());
            if (!keptVids.membershipTest(bloomKey)) {
                return;
            }
            out.set(parsed.vid(), parsed.loc(), parsed.tNorm());
            context.write(NullWritable.get(), out);
            context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.KEPT_RECORDS.name()).increment(1);
        }

        private void loadBloomFile(Configuration conf, Path path) throws IOException {
            if (!path.getName().startsWith("part-")) {
                return;
            }
            FileSystem fs = path.toUri().getScheme() == null
                    ? FileSystem.getLocal(conf)
                    : path.getFileSystem(conf);
            Path readPath = path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
            try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(readPath))) {
                NullWritable key = NullWritable.get();
                BloomFilter value = new BloomFilter();
                while (reader.next(key, value)) {
                    if (keptVids == null) {
                        keptVids = value;
                        value = new BloomFilter();
                    } else {
                        keptVids.or(value);
                    }
                }
            }
        }
    }

    private static String vidFreqPath(Configuration conf) {
        return conf.get(KEY_VID_FREQ_PATH, "");
    }
}
