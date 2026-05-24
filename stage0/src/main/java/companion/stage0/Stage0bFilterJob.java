package companion.stage0;

import companion.conf.CompanionConf;
import companion.io.RecordWritable;
import companion.job.AbstractCompanionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class Stage0bFilterJob extends AbstractCompanionJob {
    public static final String KEY_VID_FREQ_PATH = "companion.vid_freq.path";
    public static final String KEY_STAGE0_SNAPPY_ENABLED = "companion.stage0.snappy.enabled";
    private static final boolean STAGE0_SNAPPY_ENABLED_DEFAULT = true;

    @Override
    public int run(String[] args) throws Exception {
        applyTrailingDefines(getConf(), args);
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
        for (FileStatus status : fs.listStatus(freq)) {
            if (status.getPath().getName().startsWith("part-")) {
                job.addCacheFile(status.getPath().toUri());
            }
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
        if (stage0SnappyEnabled(conf)) {
            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        }
        return job;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Stage0bFilterJob(), args));
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, NullWritable, RecordWritable> {
        private final Set<Integer> keptVids = new HashSet<>();
        private final RecordWritable out = new RecordWritable();
        private int t0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            t0 = CompanionConf.t0(conf);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri);
                    if (path.getName().startsWith("part-")) {
                        loadVidFile(conf, path);
                    }
                }
            }
            if (keptVids.isEmpty()) {
                String freqPath = vidFreqPath(conf);
                if (freqPath == null || freqPath.trim().isEmpty()) {
                    throw new IOException(KEY_VID_FREQ_PATH + " is required");
                }
                loadVidPath(conf, new Path(freqPath));
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
            if (!keptVids.contains(parsed.vid())) {
                return;
            }
            out.set(parsed.vid(), parsed.loc(), parsed.tNorm());
            context.write(NullWritable.get(), out);
            context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.KEPT_RECORDS.name()).increment(1);
        }

        private void loadVidPath(Configuration conf, Path path) throws IOException {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.isDirectory(path)) {
                for (FileStatus status : fs.listStatus(path)) {
                    if (status.getPath().getName().startsWith("part-")) {
                        loadVidFile(conf, status.getPath());
                    }
                }
            } else {
                loadVidFile(conf, path);
            }
        }

        private void loadVidFile(Configuration conf, Path path) throws IOException {
            FileSystem fs = path.getFileSystem(conf);
            try (FSDataInputStream in = fs.open(path);
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    int vid = firstInt(line);
                    if (vid >= 0) {
                        keptVids.add(vid);
                    }
                }
            }
        }

        private int firstInt(String line) {
            int end = 0;
            while (end < line.length() && !Character.isWhitespace(line.charAt(end))) {
                end++;
            }
            if (end == 0) {
                return -1;
            }
            long value = 0L;
            for (int i = 0; i < end; i++) {
                char c = line.charAt(i);
                if (c < '0' || c > '9') {
                    return -1;
                }
                value = value * 10L + (c - '0');
                if (value > Integer.MAX_VALUE) {
                    return -1;
                }
            }
            return (int) value;
        }
    }

    private static String vidFreqPath(Configuration conf) {
        return conf.get(KEY_VID_FREQ_PATH, "");
    }

    private static boolean stage0SnappyEnabled(Configuration conf) {
        return conf.getBoolean(KEY_STAGE0_SNAPPY_ENABLED, STAGE0_SNAPPY_ENABLED_DEFAULT);
    }

    private static void applyTrailingDefines(Configuration conf, String[] args) {
        if (conf == null) {
            return;
        }
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("-D")) {
                continue;
            }
            String define = arg.substring(2).trim();
            if (define.isEmpty() && i + 1 < args.length) {
                define = args[++i].trim();
            }
            int eq = define.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = define.substring(0, eq).trim();
            String value = define.substring(eq + 1).trim();
            if (!key.isEmpty()) {
                conf.set(key, value);
            }
        }
    }
}
