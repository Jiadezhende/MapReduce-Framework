package companion.job;

import companion.conf.CompanionConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for every Job in the pipeline. Subclasses implement
 * {@link #buildJob(Configuration, Path, Path)} to assemble Mapper /
 * Reducer / Partitioner / Comparators; the run() wrapper handles common
 * concerns (conf loading, args parsing, exit code).
 *
 * Convention for run() args:
 *   $0 = input path
 *   $1 = output path
 *   $2+ = -Dkey=value overrides (forwarded via {@link Configured})
 */
public abstract class AbstractCompanionJob extends Configured implements Tool {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** Subclass entry point: build the configured Job. */
    protected abstract Job buildJob(Configuration conf, Path in, Path out) throws Exception;

    /** Human-readable job name surfaced to YARN UI. */
    protected String jobName() {
        return getClass().getSimpleName();
    }

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

        log.info("Submitting {}: in={} out={}", jobName(), in, out);
        Job job = buildJob(conf, in, out);
        String tag = CompanionConf.runTag(conf);
        job.setJobName(tag.isEmpty() ? jobName() : jobName() + " [" + tag + "]");

        boolean ok = job.waitForCompletion(true);
        return ok ? 0 : 1;
    }

    /** Counter group prefix shared across all stages, e.g. "STAGE1". */
    public static final String COUNTER_GROUP_STAGE0 = "STAGE0";
    public static final String COUNTER_GROUP_STAGE1 = "STAGE1";
    public static final String COUNTER_GROUP_STAGE2 = "STAGE2";
    public static final String COUNTER_GROUP_STAGE3 = "STAGE3";
}
