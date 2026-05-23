package companion.stage0;

import companion.conf.CompanionConf;
import companion.job.AbstractCompanionJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Stage0aFreqJob extends AbstractCompanionJob {

    @Override
    protected Job buildJob(Configuration conf, Path in, Path out) throws Exception {
        Job job = Job.getInstance(conf, jobName());
        job.setJarByClass(Stage0aFreqJob.class);

        job.setMapperClass(FreqMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(FreqReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        return job;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Stage0aFreqJob(), args));
    }

    public static class FreqMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final IntWritable vid = new IntWritable();
        private final IntWritable one = new IntWritable(1);
        private int t0;

        @Override
        protected void setup(Context context) {
            t0 = CompanionConf.t0(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.RAW_RECORDS.name()).increment(1);
            Stage0CsvParser.ParseResult parsed = Stage0CsvParser.parseRecord(value, t0);
            if (!parsed.isOk()) {
                context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.PARSE_FAIL.name()).increment(1);
                return;
            }
            vid.set(parsed.vid());
            context.write(vid, one);
        }
    }

    public static class SumCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable out = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            out.set(sum);
            context.write(key, out);
        }
    }

    public static class FreqReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            if (sum >= 2) {
                context.write(key, NullWritable.get());
            } else {
                context.getCounter(COUNTER_GROUP_STAGE0, Stage0Counters.SINGLETON_VIDS.name()).increment(1);
            }
        }
    }
}
