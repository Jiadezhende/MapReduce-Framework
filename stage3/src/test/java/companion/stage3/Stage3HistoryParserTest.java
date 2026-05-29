package companion.stage3;

import companion.conf.CompanionConf;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage3HistoryParserTest {

    @Test
    public void testParseSimpleHistoryFile() throws Exception {
        // create temporary history file with counters and job times
        File tmp = File.createTempFile("fake-history", ".jhist");
        tmp.deleteOnExit();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(tmp))) {
            w.write("{\n");
            w.write(" \"jobName\": \"Stage3SortJob\",\n");
            w.write(" \"startTime\": 1000,\n");
            w.write(" \"finishTime\": 3000,\n");
            w.write(" \"counters\": [\n");
            w.write("   {\"name\": \"STAGE3.TOPN_EMITTED\", \"value\": 123},\n");
            w.write("   {\"name\": \"STAGE1.PAIRS_EMITTED\", \"value\": 7}\n");
            w.write(" ]\n");
            w.write("}\n");
            w.flush();
        }

        Configuration conf = new Configuration();
        CompanionConf.applyDefaults(conf);
        conf.set(CompanionConf.KEY_HISTORY_PATH, tmp.getAbsolutePath());

        Stage3SortJob.HistoryResult res = Stage3SortJob.parseYarnHistory(conf);
        Map<String, Long> counters = res.counters;
        Map<String, Long> wall = res.wallClockMs;

        assertTrue("should contain STAGE3.TOPN_EMITTED", counters.containsKey("STAGE3.TOPN_EMITTED"));
        assertEquals(123L, (long) counters.get("STAGE3.TOPN_EMITTED"));
        assertTrue("should contain STAGE1.PAIRS_EMITTED", counters.containsKey("STAGE1.PAIRS_EMITTED"));
        assertEquals(7L, (long) counters.get("STAGE1.PAIRS_EMITTED"));

        // wall-clock for Stage3 should be finish - start = 2000
        assertTrue(wall.containsKey("STAGE3"));
        assertEquals(2000L, (long) wall.get("STAGE3"));
    }
}
