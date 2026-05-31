package companion.stage3;

import companion.conf.CompanionConf;
import companion.job.AbstractCompanionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Stage 3: globally sort pairs, emit TopN, and write metrics. */
public class Stage3SortJob extends AbstractCompanionJob {

    public enum Stage3Counter {
        TOPN_EMITTED
    }

    private static final String SORTED_DIR = "companions.csv";
    private static final String TOPN_FILE = "top_n.csv";
    private static final String TOPN_TMP_DIR = "_topn_tmp";
    private static final String METRICS_FILE = "_metrics.json";
    private static final Path PARTITION_PATH = new Path("hdfs:///companion/_stage3/_partition.lst");
    private static final String HLL_DIR = "_hll_pairs";

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

        Job job = buildJob(conf, in, out);
        job.setJobName(jobName());

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            return 1;
        }

        Path sortedDir = new Path(out, SORTED_DIR);
        Path topNPath = new Path(out, TOPN_FILE);
        Path metricsPath = new Path(out, METRICS_FILE);
        int topN = CompanionConf.topN(conf);
        String phase = phaseFromPath(out);

        long hllPairCount = countLinesInDir(conf, new Path(in, HLL_DIR));
        long topNEmitted = runTopNJob(conf, sortedDir, topNPath, topN);
        ScanResult scan = scanSortedOutput(conf, sortedDir);
        // Merge YARN history counters and wall-clock times when available.
        HistoryResult history = parseYarnHistory(conf);
        // Prefer counters from history but ensure STAGE3.TOPN_EMITTED is present.
        Map<String, Long> counters = history.counters;
        if (!counters.containsKey("STAGE3.TOPN_EMITTED")) {
            counters.put("STAGE3.TOPN_EMITTED", topNEmitted);
        }
        Metrics metrics = new Metrics(phase, scan.pairTotal, scan.histogram, hllPairCount, counters, history.wallClockMs);
        writeMetrics(conf, metricsPath, metrics);
        return 0;
    }

    @Override
    protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
        Path sortedDir = new Path(out, SORTED_DIR);

        Job job = Job.getInstance(conf, jobName());
        job.setJarByClass(Stage3SortJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Stage3Key.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, sortedDir);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        int reducers = CompanionConf.stage3Reducers(conf);
        job.setNumReduceTasks(reducers);

        if (reducers > 1) {
            job.setPartitionerClass(TotalOrderPartitioner.class);
            configurePartitionFile(job);
        }

        return job;
    }

    private static void configurePartitionFile(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = PARTITION_PATH.getFileSystem(conf);
        Path parent = PARTITION_PATH.getParent();
        if (parent != null) {
            fs.mkdirs(parent);
        }
        if (fs.exists(PARTITION_PATH)) {
            fs.delete(PARTITION_PATH, false);
        }
        TotalOrderPartitioner.setPartitionFile(conf, PARTITION_PATH);

        int numReducers = job.getNumReduceTasks();
        // 目标每分区 ~200 条样本，下限 10_000 兜底
        int numSamples = Math.max(numReducers * 200, 10_000);
        int maxFiles = 10;

        List<Stage3Key> samples = sampleStage3Keys(job, numSamples, maxFiles);
        if (samples.isEmpty()) {
            throw new IOException("No samples collected for Stage3 partition file");
        }
        Collections.sort(samples);

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(PARTITION_PATH),
                SequenceFile.Writer.keyClass(Stage3Key.class),
                SequenceFile.Writer.valueClass(NullWritable.class))) {
            for (int i = 1; i < numReducers; i++) {
                int idx = (int) ((long) i * samples.size() / numReducers);
                if (idx >= samples.size()) {
                    idx = samples.size() - 1;
                }
                writer.append(samples.get(idx), NullWritable.get());
            }
        }
    }

    private static List<Stage3Key> sampleStage3Keys(Job job, int numSamples, int maxFiles)
            throws IOException {
        Configuration conf = job.getConfiguration();
        Path[] inputPaths = TextInputFormat.getInputPaths(job);
        List<FileStatus> dataFiles = new ArrayList<>();
        for (Path p : inputPaths) {
            FileSystem fs = p.getFileSystem(conf);
            FileStatus root;
            try {
                root = fs.getFileStatus(p);
            } catch (FileNotFoundException e) {
                continue;
            }
            if (root.isDirectory()) {
                collectInputFiles(fs, p, dataFiles);
            } else if (root.getLen() > 0) {
                dataFiles.add(root);
            }
        }
        if (dataFiles.isEmpty()) {
            return new ArrayList<>();
        }
        Collections.shuffle(dataFiles, new Random(0L));
        int filesToRead = Math.min(maxFiles, dataFiles.size());
        int perFile = Math.max(1, numSamples / filesToRead);

        List<Stage3Key> samples = new ArrayList<>(numSamples);
        for (int i = 0; i < filesToRead; i++) {
            FileStatus fst = dataFiles.get(i);
            FileSystem fs = fst.getPath().getFileSystem(conf);
            try (FSDataInputStream in = fs.open(fst.getPath());
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                int read = 0;
                while (read < perFile && (line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    try {
                        ParsedLine pl = parseLine(line);
                        samples.add(new Stage3Key(pl.count, pl.vidA, pl.vidB));
                        read++;
                    } catch (IOException ignore) {
                        // 跳过格式异常的行
                    }
                }
            }
        }
        return samples;
    }

    private static void collectInputFiles(FileSystem fs, Path dir, List<FileStatus> out)
            throws IOException {
        FileStatus[] children = fs.listStatus(dir);
        if (children == null) {
            return;
        }
        for (FileStatus c : children) {
            String name = c.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            if (c.isDirectory()) {
                collectInputFiles(fs, c.getPath(), out);
            } else if (c.getLen() > 0) {
                out.add(c);
            }
        }
    }

    public static class SortMapper extends Mapper<Object, Text, Stage3Key, Text> {
        private final Stage3Key outKey = new Stage3Key();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }
            ParsedLine parsed = parseLine(line);
            outKey.set(parsed.count, parsed.vidA, parsed.vidB);
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }

    public static class SortReducer extends Reducer<Stage3Key, Text, NullWritable, Text> {
        @Override
        protected void reduce(Stage3Key key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    private static long parseCount(String line) throws IOException {
        int first = line.indexOf(',');
        if (first <= 0) {
            throw new IOException("bad line: " + line);
        }
        int second = line.indexOf(',', first + 1);
        if (second <= first + 1 || second >= line.length() - 1) {
            throw new IOException("bad line: " + line);
        }
        String tail = line.substring(second + 1).trim();
        return Long.parseLong(tail);
    }

    private static ParsedLine parseLine(String line) throws IOException {
        int first = line.indexOf(',');
        if (first <= 0) {
            throw new IOException("bad line: " + line);
        }
        int second = line.indexOf(',', first + 1);
        if (second <= first + 1 || second >= line.length() - 1) {
            throw new IOException("bad line: " + line);
        }
        int vidA = Integer.parseInt(line.substring(0, first).trim());
        int vidB = Integer.parseInt(line.substring(first + 1, second).trim());
        long count = Long.parseLong(line.substring(second + 1).trim());
        return new ParsedLine(vidA, vidB, count);
    }

    private static ScanResult scanSortedOutput(Configuration conf,
                                               Path sortedDir) throws IOException {
        FileSystem fs = sortedDir.getFileSystem(conf);

        long pairTotal = 0L;
        Histogram histogram = new Histogram();
        for (Path part : listPartFiles(fs, sortedDir)) {
            try (FSDataInputStream in = fs.open(part);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    long count = parseCount(line);
                    pairTotal++;
                    histogram.add(count);
                }
            }
        }

        return new ScanResult(pairTotal, histogram);
    }

    private static long runTopNJob(Configuration conf, Path sortedDir, Path topNPath, int topN)
            throws Exception {
        Configuration jobConf = new Configuration(conf);
        Job job = Job.getInstance(jobConf, "Stage3TopNJob");
        job.setJarByClass(Stage3SortJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, sortedDir);

        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(Stage3Key.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        Path tmpDir = new Path(topNPath.getParent(), TOPN_TMP_DIR);
        FileSystem fs = tmpDir.getFileSystem(jobConf);
        if (fs.exists(tmpDir)) {
            fs.delete(tmpDir, true);
        }

        jobConf.setInt(CompanionConf.KEY_TOP_N, topN);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, tmpDir);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new IOException("TopN job failed");
        }

        Path partFile = new Path(tmpDir, "part-r-00000");
        FileSystem outFs = topNPath.getFileSystem(jobConf);
        if (outFs.exists(topNPath)) {
            outFs.delete(topNPath, false);
        }
        outFs.mkdirs(topNPath.getParent());
        if (!outFs.rename(partFile, topNPath)) {
            throw new IOException("Failed to move TopN output to " + topNPath);
        }
        outFs.delete(tmpDir, true);

        return job.getCounters()
                .findCounter(COUNTER_GROUP_STAGE3, Stage3Counter.TOPN_EMITTED.name())
                .getValue();
    }

    public static class TopNMapper extends Mapper<Object, Text, Stage3Key, Text> {
        private final Stage3Key outKey = new Stage3Key();
        private final Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.isEmpty()) {
                return;
            }
            ParsedLine parsed = parseLine(line);
            outKey.set(parsed.count, parsed.vidA, parsed.vidB);
            outValue.set(line);
            context.write(outKey, outValue);
        }
    }

    public static class TopNReducer extends Reducer<Stage3Key, Text, NullWritable, Text> {
        private int topN;
        private long emitted;

        @Override
        protected void setup(Context context) {
            topN = CompanionConf.topN(context.getConfiguration());
            emitted = 0L;
        }

        @Override
        protected void reduce(Stage3Key key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (emitted >= topN) {
                return;
            }
            for (Text value : values) {
                if (emitted >= topN) {
                    return;
                }
                context.write(NullWritable.get(), value);
                context.getCounter(COUNTER_GROUP_STAGE3, Stage3Counter.TOPN_EMITTED.name()).increment(1L);
                emitted++;
            }
        }
    }

    private static List<Path> listPartFiles(FileSystem fs, Path dir) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir, path -> path.getName().startsWith("part-"));
        if (statuses == null || statuses.length == 0) {
            return new ArrayList<>();
        }
        Arrays.sort(statuses, Comparator.comparing(s -> s.getPath().getName()));
        List<Path> files = new ArrayList<>(statuses.length);
        for (FileStatus status : statuses) {
            files.add(status.getPath());
        }
        return files;
    }

    private static long countLinesInDir(Configuration conf, Path dir) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        FileStatus dirStatus;
        try {
            dirStatus = fs.getFileStatus(dir);
        } catch (FileNotFoundException e) {
            return 0L;
        }
        if (!dirStatus.isDirectory()) {
            return countLinesInFile(fs, dir);
        }
        long total = 0L;
        FileStatus[] statuses = fs.listStatus(dir, path -> path.getName().startsWith("part-"));
        if (statuses == null) {
            return 0L;
        }
        for (FileStatus status : statuses) {
            total += countLinesInFile(fs, status.getPath());
        }
        return total;
    }

    private static long countLinesInFile(FileSystem fs, Path file) throws IOException {
        long total = 0L;
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            while (reader.readLine() != null) {
                total++;
            }
        }
        return total;
    }

    static HistoryResult parseYarnHistory(Configuration conf) throws IOException {
        Map<String, Long> counters = new HashMap<>();
        Map<String, Long> wallClock = new HashMap<>();
        wallClock.put("STAGE0", 0L);
        wallClock.put("STAGE1", 0L);
        wallClock.put("STAGE2", 0L);
        wallClock.put("STAGE3", 0L);

        String historyPath = CompanionConf.historyPath(conf);
        if (historyPath == null || historyPath.isEmpty()) {
            return new HistoryResult(counters, wallClock);
        }

        Path p = new Path(historyPath);
        FileSystem fs = p.getFileSystem(conf);
        FileStatus pStatus;
        try {
            pStatus = fs.getFileStatus(p);
        } catch (FileNotFoundException e) {
            return new HistoryResult(counters, wallClock);
        }

        List<Path> files = new ArrayList<>();
        if (pStatus.isDirectory()) {
            FileStatus[] statuses = fs.listStatus(p);
            if (statuses != null) {
                for (FileStatus s : statuses) {
                    String name = s.getPath().getName();
                    if (name.endsWith(".jhist") || name.endsWith(".json") || name.startsWith("job")) {
                        files.add(s.getPath());
                    }
                }
            }
        } else {
            files.add(p);
        }

        String[] wantedCounters = new String[] {
                "STAGE0.RAW_RECORDS",
                "STAGE0.KEPT_RECORDS",
                "STAGE1.PAIRS_EMITTED",
                "STAGE2.PAIRS_OUTPUT",
                "STAGE2.HLL_FALLBACK_COUNT",
                "STAGE3.TOPN_EMITTED"
        };

        Pattern valuePattern = Pattern.compile("\"value\"\\s*:\\s*(\\d+)");
        Pattern jobNamePattern = Pattern.compile("\"jobName\"\\s*:\\s*\"([^\"]+)\"|\"name\"\\s*:\\s*\"([^\"]+)\"");
        Pattern startPattern = Pattern.compile("\"startTime\"\\s*:\\s*(\\d+)");
        Pattern finishPattern = Pattern.compile("\"finishTime\"\\s*:\\s*(\\d+)");

        for (Path file : files) {
            StringBuilder sb = new StringBuilder(8192);
            try (FSDataInputStream in = fs.open(file);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
            String content = sb.toString();

            // extract counters by key
            for (String key : wantedCounters) {
                if (counters.containsKey(key)) {
                    continue;
                }
                int pos = content.indexOf(key);
                if (pos >= 0) {
                    int end = Math.min(content.length(), pos + 1024);
                    Matcher m = valuePattern.matcher(content.substring(pos, end));
                    if (m.find()) {
                        long v = Long.parseLong(m.group(1));
                        counters.put(key, v);
                    }
                }
            }

            // extract job times and attribute to stages by job name
            Matcher jm = jobNamePattern.matcher(content);
            while (jm.find()) {
                String jobName = jm.group(1) != null ? jm.group(1) : jm.group(2);
                int idx = jm.end();
                int end = Math.min(content.length(), idx + 2000);
                String window = content.substring(idx, end);
                Matcher ms = startPattern.matcher(window);
                Matcher mf = finishPattern.matcher(window);
                if (ms.find() && mf.find()) {
                    long start = Long.parseLong(ms.group(1));
                    long finish = Long.parseLong(mf.group(1));
                    long dur = Math.max(0L, finish - start);
                    if (jobName.contains("Stage0")) {
                        wallClock.put("STAGE0", wallClock.getOrDefault("STAGE0", 0L) + dur);
                    } else if (jobName.contains("Stage1")) {
                        wallClock.put("STAGE1", wallClock.getOrDefault("STAGE1", 0L) + dur);
                    } else if (jobName.contains("Stage2")) {
                        wallClock.put("STAGE2", wallClock.getOrDefault("STAGE2", 0L) + dur);
                    } else if (jobName.contains("Stage3")) {
                        wallClock.put("STAGE3", wallClock.getOrDefault("STAGE3", 0L) + dur);
                    }
                }
            }
        }

        return new HistoryResult(counters, wallClock);
    }

    private static void writeMetrics(Configuration conf, Path path, Metrics metrics) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, false);
        }
        try (FSDataOutputStream out = fs.create(path, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            writer.write(metrics.toJson());
            writer.write('\n');
        }
    }

    private static String phaseFromPath(Path out) {
        String name = out.getName();
        if ("1d".equals(name) || "7d".equals(name) || "31d".equals(name)) {
            return name;
        }
        return name.isEmpty() ? "unknown" : name;
    }

    private static final class Histogram {
        private long c3;
        private long c4;
        private long c5to9;
        private long c10to99;
        private long c100to999;
        private long c1000plus;

        private void add(long count) {
            if (count == 3) {
                c3++;
            } else if (count == 4) {
                c4++;
            } else if (count <= 9) {
                c5to9++;
            } else if (count <= 99) {
                c10to99++;
            } else if (count <= 999) {
                c100to999++;
            } else {
                c1000plus++;
            }
        }
    }

    private static final class ParsedLine {
        private final int vidA;
        private final int vidB;
        private final long count;

        private ParsedLine(int vidA, int vidB, long count) {
            this.vidA = vidA;
            this.vidB = vidB;
            this.count = count;
        }
    }

    private static final class Metrics {
        private final String phase;
        private final long pairTotal;
        private final Histogram histogram;
        private final long hllPairCount;
        private final Map<String, Long> counters;
        private final Map<String, Long> wallClockMs;

        private Metrics(String phase, long pairTotal, Histogram histogram, long hllPairCount,
                        Map<String, Long> counters, Map<String, Long> wallClockMs) {
            this.phase = phase;
            this.pairTotal = pairTotal;
            this.histogram = histogram;
            this.hllPairCount = hllPairCount;
            this.counters = counters;
            this.wallClockMs = wallClockMs;
        }

        private String toJson() {
            StringBuilder out = new StringBuilder(512);
            out.append('{');
            out.append("\"phase\":\"").append(phase).append("\",");
            out.append("\"pair_total\":").append(pairTotal).append(',');
            out.append("\"count_histogram\":{");
            out.append("\"3\":").append(histogram.c3).append(',');
            out.append("\"4\":").append(histogram.c4).append(',');
            out.append("\"5-9\":").append(histogram.c5to9).append(',');
            out.append("\"10-99\":").append(histogram.c10to99).append(',');
            out.append("\"100-999\":").append(histogram.c100to999).append(',');
            out.append("\"1000+\":").append(histogram.c1000plus).append('}');
            out.append(',');
            out.append("\"hll_pair_count\":").append(hllPairCount);
            out.append(',');
            out.append("\"counters\":{");
            boolean first = true;
            for (Map.Entry<String, Long> e : counters.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append('"').append(e.getKey()).append('"').append(':').append(e.getValue());
            }
            out.append('}');
            out.append(',');
            out.append("\"wall_clock_ms\":{");
            first = true;
            for (Map.Entry<String, Long> e : wallClockMs.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append('"').append(e.getKey()).append('"').append(':').append(e.getValue());
            }
            out.append('}');
            out.append('}');
            return out.toString();
        }
    }

    static final class HistoryResult {
        final Map<String, Long> counters;
        final Map<String, Long> wallClockMs;

        HistoryResult(Map<String, Long> counters, Map<String, Long> wallClockMs) {
            this.counters = counters;
            this.wallClockMs = wallClockMs;
        }
    }

    private static final class ScanResult {
        private final long pairTotal;
        private final Histogram histogram;

        private ScanResult(long pairTotal, Histogram histogram) {
            this.pairTotal = pairTotal;
            this.histogram = histogram;
        }
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new Stage3SortJob(), args);
        System.exit(code);
    }
}
