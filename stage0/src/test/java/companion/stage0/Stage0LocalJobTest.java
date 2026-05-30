package companion.stage0;

import companion.io.RecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage0LocalJobTest {

    private Configuration conf;
    private FileSystem fs;
    private Path workDir;

    @Before
    public void setUp() throws IOException {
        Assume.assumeFalse("Hadoop LocalJobRunner on Windows requires winutils/HADOOP_HOME",
                isWindowsWithoutHadoopHome());
        conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        // Stage0 uses a BloomFilter to approximate "vid count >= 2"; FixtureGenerator uses
        // precise counting. So production output is always a (multiset) SUPERSET of the fixture:
        // production may keep extra rows whose vid was bloom-FP-promoted. We assert superset +
        // bounded delta below rather than byte-equal. See docs/reference-semantics.md §7.
        conf.setLong(Stage0Bloom.KEY_EXPECTED_ENTRIES, 200_000L);
        conf.setFloat(Stage0Bloom.KEY_FALSE_POSITIVE_RATE, 1.0e-9f);
        fs = FileSystem.get(conf);
        workDir = new Path(System.getProperty("java.io.tmpdir"),
                "companion-stage0-" + System.nanoTime());
        fs.mkdirs(workDir);
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null && workDir != null) {
            fs.delete(workDir, true);
        }
    }

    @Test
    public void stage0JobsProduceGoldenFilteredFixtureFromMini() throws Exception {
        Path input = new Path(workDir, "mini.csv");
        Path freq = new Path(workDir, "vid_freq");
        Path filtered = new Path(workDir, "filtered");
        // Fixture is generated from the full mini.csv (FixtureGenerator dropped its LINES limit),
        // so this test must feed the same full input. mini.csv is 100k lines.
        writeMiniPrefix(input, 100_000);

        assertEquals(0, ToolRunner.run(conf, new Stage0aFreqJob(),
                new String[]{input.toString(), freq.toString()}));

        assertEquals(0, ToolRunner.run(new Configuration(conf), new Stage0bFilterJob(),
                new String[]{input.toString(), filtered.toString(),
                        "-D", Stage0bFilterJob.KEY_VID_FREQ_PATH + "=" + freq.toString()}));

        List<int[]> actual = readRecords(filtered);
        List<int[]> expected = readRecords(new Path(fixture("filtered.seq").toURI()));

        // Superset assertion: every fixture record must appear in the production output with at
        // least the same multiplicity. Production may emit extra rows due to Bloom FP; the delta
        // is bounded by BLOOM_FP_DELTA_BOUND. Under the test's 200k/1e-9 bloom, expected FP count
        // on mini.csv is ~1e-5 — the bound is set far above that as a sanity ceiling rather than
        // a tight statistical limit.
        final int BLOOM_FP_DELTA_BOUND = 100;
        Map<String, Integer> expectedMs = toMultiset(expected);
        Map<String, Integer> actualMs = toMultiset(actual);
        for (Map.Entry<String, Integer> e : expectedMs.entrySet()) {
            Integer got = actualMs.get(e.getKey());
            assertTrue("missing fixture record in production output: " + e.getKey(), got != null);
            assertTrue("multiplicity below fixture for " + e.getKey() + ": got=" + got
                            + " expected>=" + e.getValue(),
                    got >= e.getValue());
        }
        int delta = actual.size() - expected.size();
        assertTrue("delta must be non-negative (production output should be a superset): " + delta,
                delta >= 0);
        assertTrue("Bloom FP delta exceeds bound: " + delta + " > " + BLOOM_FP_DELTA_BOUND
                        + " — either bloom params shrank or mini.csv grew; revisit the bound",
                delta <= BLOOM_FP_DELTA_BOUND);
    }

    private static Map<String, Integer> toMultiset(List<int[]> rows) {
        Map<String, Integer> ms = new HashMap<>(rows.size() * 2);
        for (int[] r : rows) {
            String k = r[0] + "," + r[1] + "," + r[2];
            ms.merge(k, 1, Integer::sum);
        }
        return ms;
    }

    @Test
    public void stage0aWritesOnlyNonSingletonVids() throws Exception {
        Path input = new Path(workDir, "tiny.csv");
        Path freq = new Path(workDir, "tiny_freq");
        writeLines(input, asLines(
                "1,10,1420041600",
                "2,10,1420041601",
                "1,11,1420041602",
                "bad,line",
                "3,12,1420041603",
                "3,x,1420041604"
        ));

        assertEquals(0, ToolRunner.run(conf, new Stage0aFreqJob(),
                new String[]{input.toString(), freq.toString()}));

        BloomFilter bloom = readBloom(freq);
        assertTrue(mightContain(bloom, 1));
        assertTrue(!mightContain(bloom, 2));
        assertTrue(!mightContain(bloom, 3));
    }

    private void writeMiniPrefix(Path output, int lines) throws IOException {
        File source = new File(repoRoot(), "tests/data/mini.csv");
        List<String> prefix = new ArrayList<>(lines);
        try (BufferedReader reader = Files.newBufferedReader(source.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while (prefix.size() < lines && (line = reader.readLine()) != null) {
                prefix.add(line);
            }
        }
        assertEquals(lines, prefix.size());
        writeLines(output, prefix);
    }

    private List<int[]> readRecords(Path path) throws IOException {
        List<int[]> records = new ArrayList<>();
        if (fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                if (status.getPath().getName().startsWith("part-")) {
                    records.addAll(readRecords(status.getPath()));
                }
            }
            return records;
        }

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path))) {
            NullWritable key = NullWritable.get();
            RecordWritable value = new RecordWritable();
            while (reader.next(key, value)) {
                records.add(new int[]{value.getVid(), value.getLoc(), value.getTNorm()});
            }
        }
        return records;
    }

    private BloomFilter readBloom(Path path) throws IOException {
        BloomFilter bloom = null;
        if (fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                if (status.getPath().getName().startsWith("part-")) {
                    BloomFilter part = readBloom(status.getPath());
                    if (bloom == null) {
                        bloom = part;
                    } else {
                        bloom.or(part);
                    }
                }
            }
            return bloom;
        }

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(path))) {
            NullWritable key = NullWritable.get();
            BloomFilter value = new BloomFilter();
            while (reader.next(key, value)) {
                if (bloom == null) {
                    bloom = value;
                    value = new BloomFilter();
                } else {
                    bloom.or(value);
                }
            }
        }
        return bloom;
    }

    private boolean mightContain(BloomFilter bloom, int vid) {
        Key key = new Key();
        byte[] bytes = new byte[4];
        Stage0Bloom.setVid(key, bytes, vid);
        return bloom.membershipTest(key);
    }

    private static List<String> asLines(String... lines) {
        List<String> out = new ArrayList<>();
        for (String line : lines) {
            out.add(line);
        }
        return out;
    }

    private void writeLines(Path output, List<String> lines) throws IOException {
        try (FSDataOutputStream out = fs.create(output, true);
             OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            for (String line : lines) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    private static File fixture(String name) throws IOException {
        return new File(repoRoot(), "tests/data/fixtures/" + name);
    }

    private static File repoRoot() throws IOException {
        File cur = new File(".").getCanonicalFile();
        while (cur != null) {
            if (new File(cur, "tests/data/mini.csv").isFile()) {
                return cur;
            }
            cur = cur.getParentFile();
        }
        throw new IllegalStateException("repo root not found");
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
