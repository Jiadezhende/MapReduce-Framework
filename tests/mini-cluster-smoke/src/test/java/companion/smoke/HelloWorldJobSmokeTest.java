package companion.smoke;

import companion.job.AbstractCompanionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Smoke test: a Hello-World word-count Job submitted through
 * {@link AbstractCompanionJob}, running against an in-process LocalJobRunner.
 *
 * Purpose: prove that
 *  - the base class wires Configured/Tool correctly;
 *  - companion-conf.xml loads from the classpath;
 *  - a Job with our common dependencies actually completes.
 */
public class HelloWorldJobSmokeTest {

    private static FileSystem fs;
    private static Path workDir;

    @BeforeClass
    public static void setUp() throws IOException {
        Assume.assumeFalse("Hadoop LocalJobRunner on Windows requires winutils/HADOOP_HOME",
                isWindowsWithoutHadoopHome());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        fs = FileSystem.get(conf);

        workDir = new Path(System.getProperty("java.io.tmpdir"),
                "companion-smoke-" + System.nanoTime());
        fs.mkdirs(workDir);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (fs != null && workDir != null) {
            fs.delete(workDir, true);
        }
    }

    @Test
    public void wordCountRunsViaAbstractCompanionJob() throws Exception {
        Path input = new Path(workDir, "input");
        Path output = new Path(workDir, "output");
        fs.mkdirs(input);

        try (org.apache.hadoop.fs.FSDataOutputStream os = fs.create(new Path(input, "text"))) {
            os.write("hello world hello\n".getBytes(StandardCharsets.UTF_8));
            os.write("companion world\n".getBytes(StandardCharsets.UTF_8));
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        int rc = ToolRunner.run(conf, new WordCountJob(),
                new String[]{input.toString(), output.toString()});
        assertEquals(0, rc);

        Map<String, Integer> counts = new HashMap<>();
        for (org.apache.hadoop.fs.FileStatus st : fs.listStatus(output)) {
            if (st.getPath().getName().startsWith("part-")) {
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(fs.open(st.getPath()), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        String[] tok = line.split("\\s+");
                        counts.put(tok[0], Integer.parseInt(tok[1]));
                    }
                }
            }
        }

        assertEquals(Integer.valueOf(2), counts.get("hello"));
        assertEquals(Integer.valueOf(2), counts.get("world"));
        assertEquals(Integer.valueOf(1), counts.get("companion"));
    }

    /** Trivial AbstractCompanionJob subclass used to validate the wiring. */
    public static class WordCountJob extends AbstractCompanionJob {
        @Override
        protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
            Job job = Job.getInstance(conf, jobName());
            job.setJarByClass(WordCountJob.class);
            job.setMapperClass(TokenMapper.class);
            job.setCombinerClass(SumReducer.class);
            job.setReducerClass(SumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextInputFormat.addInputPath(job, in);
            TextOutputFormat.setOutputPath(job, out);
            return job;
        }
    }

    public static class TokenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text word = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            for (String t : value.toString().split("\\s+")) {
                if (t.isEmpty()) continue;
                word.set(t);
                ctx.write(word, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int s = 0;
            for (IntWritable v : values) s += v.get();
            out.set(s);
            ctx.write(key, out);
        }
    }

    private static boolean isWindowsWithoutHadoopHome() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (!osName.contains("win")) {
            return false;
        }
        String hadoopHomeProp = System.getProperty("hadoop.home.dir");
        String hadoopHomeEnv = System.getenv("HADOOP_HOME");
        return isBlank(hadoopHomeProp) && isBlank(hadoopHomeEnv);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
