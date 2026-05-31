package companion.stage3;

import companion.conf.CompanionConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage3SortJobTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void localSortWritesTopNAndMetrics() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt(CompanionConf.KEY_TOP_N, 2);
        conf.setInt(CompanionConf.KEY_STAGE3_REDUCERS, 1);

        FileSystem fs = FileSystem.getLocal(conf);
        Path in = new Path(tmp.newFolder("in").toURI());
        Path out = new Path(tmp.newFolder("out").toURI());
        fs.delete(out, true);

        writeLines(fs, new Path(in, "part-00000"),
                "1,2,5",
                "2,3,10",
                "1,3,3",
            "2,4,7",
            "1,4,7");

        int code = ToolRunner.run(conf, new Stage3SortJob(),
                new String[]{in.toString(), out.toString()});
        assertEquals(0, code);

        List<String> sorted = readLines(fs, new Path(out, "companions.csv"));
        List<String> expectedSorted = Arrays.asList(
                "2,3,10",
            "1,4,7",
            "2,4,7",
                "1,2,5",
                "1,3,3"
        );
        assertEquals(expectedSorted, sorted);

        List<String> topN = readLines(fs, new Path(out, "top_n.csv"));
        List<String> expectedTop = Arrays.asList(
                "2,3,10",
            "1,4,7"
        );
        assertEquals(expectedTop, topN);

        String metrics = readAll(fs, new Path(out, "_metrics.json"));
        assertTrue(metrics.contains("\"pair_total\":5"));
        assertTrue(metrics.contains("\"3\":1"));
        assertTrue(metrics.contains("\"4\":0"));
        assertTrue(metrics.contains("\"5-9\":3"));
        assertTrue(metrics.contains("\"10-99\":1"));
        assertTrue(metrics.contains("\"hll_pair_count\":0"));
        assertTrue(metrics.contains("\"STAGE3.TOPN_EMITTED\":2"));
    }

    private static void writeLines(FileSystem fs, Path file, String... lines) throws Exception {
        try (FSDataOutputStream out = fs.create(file, true);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.write(line);
                writer.write('\n');
            }
        }
    }

    private static List<String> readLines(FileSystem fs, Path path) throws Exception {
        List<String> lines = new ArrayList<>();
        if (fs.getFileStatus(path).isDirectory()) {
            FileStatus[] statuses = fs.listStatus(path, p -> p.getName().startsWith("part-"));
            if (statuses == null) {
                return lines;
            }
            Arrays.sort(statuses, Comparator.comparing(s -> s.getPath().getName()));
            for (FileStatus status : statuses) {
                lines.addAll(readLines(fs, status.getPath()));
            }
            return lines;
        }

        try (FSDataInputStream in = fs.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }

    private static String readAll(FileSystem fs, Path path) throws Exception {
        StringBuilder out = new StringBuilder();
        try (FSDataInputStream in = fs.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }
        }
        return out.toString();
    }
}
