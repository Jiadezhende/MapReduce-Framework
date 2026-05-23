package companion.job;

import companion.conf.CompanionConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractCompanionJobTest {

    @Test
    public void trailingDefinesAreAppliedBeforeBuildingJob() throws Exception {
        CapturingJob job = new CapturingJob();

        try {
            ToolRunner.run(new Configuration(), job,
                    new String[]{"input", "output", "-D " + CompanionConf.KEY_T0 + "=123"});
            fail("expected buildJob to stop after capturing configuration");
        } catch (StopAfterCapture expected) {
            // Expected: this test only needs to inspect the prepared configuration.
        }

        assertEquals(123L, job.capturedT0);
    }

    private static final class CapturingJob extends AbstractCompanionJob {
        private long capturedT0;

        @Override
        protected Job buildJob(Configuration conf, Path input, Path output) {
            capturedT0 = CompanionConf.t0(conf);
            throw new StopAfterCapture();
        }
    }

    private static final class StopAfterCapture extends RuntimeException {
    }
}
