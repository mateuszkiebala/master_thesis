package prefix_app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sampling {
    static final Log LOG = LogFactory.getLog(Sampling.class);

    public static class SamplerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final Random random = new Random();
        private int ratioForRandom;

        @Override
        public void setup(Context ctx) throws IOException, InterruptedException {
            super.setup(ctx);
            ratioForRandom = ctx.getConfiguration().getInt(Utils.RATIO_KEY, -1);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (random.nextInt(ratioForRandom) == 0) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static class ComputeBoundsForSortingReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
        private int noOfSplitPoints;

        @Override
        public void setup(Context ctx) throws IOException, InterruptedException {
            super.setup(ctx);
            Configuration conf = ctx.getConfiguration();
            noOfSplitPoints = conf.getInt(Utils.SPLIT_POINTS_KEY, 0) - 1;
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<FourInts> result = new ArrayList<>();
            for (Text value : values) {
                result.add(new FourInts(value));
            }

            java.util.Collections.sort(result, FourInts.cmp);
            int step = result.size() / (noOfSplitPoints+1);
            for (int i = 1; i <= noOfSplitPoints; i++) {
                context.write(result.get(i * step).toText(), NullWritable.get());
            }
        }
    }

    public static int run(Path input, Path output, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        LOG.info("Starting phase sampling");

        Job job = Job.getInstance(conf, "JOB: Phase one sampling");
        job.setJarByClass(Sampling.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(SamplerMapper.class);
        job.setReducerClass(ComputeBoundsForSortingReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        LOG.info("Waiting for phase Sampling");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase Sampling, processed " + total + " key/value pairs");

        return ret;
    }
}
