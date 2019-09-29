package prefix_app;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Sorting {

    static final Log LOG = LogFactory.getLog(Sorting.class);

    public static final String SAMPLING_SPLIT_POINTS_CACHE = "sampling_split_points.cache";
    public static final String SORTED_COUNTS = "sortedCounts";
    public static final String SORTED_DATA = "sortedData";
    public static final String SORTED_DATA_PATTERN = SORTED_DATA + "-r-*";

    public static class PartitioningMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private FourInts[] splitPoints;

        @Override
        public void setup(Context ctx) {
            ArrayList<String> words = Utils.readFromCache(new Path(SAMPLING_SPLIT_POINTS_CACHE));
            splitPoints = new FourInts[words.size()];
            for (int i = 0; i < words.size(); i++) {
              splitPoints[i] = new FourInts(words.get(i));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int dummy = java.util.Arrays.binarySearch(splitPoints, new FourInts(value.toString()), FourInts.cmp);
            context.write(new IntWritable(dummy >= 0 ? dummy : -dummy - 1), value);
        }
    }

    public static class SortingReducer extends Reducer<IntWritable, Text, WritableComparable, Writable> {
        private MultipleOutputs mos;

        @Override
        public void setup(Context ctx) {
          mos = new MultipleOutputs(ctx);
        }

        public void cleanup(Context ctx) throws IOException {
          try {
            mos.close();
          } catch (InterruptedException ex) {
            Logger.getLogger(Sorting.class.getName()).log(Level.SEVERE, null, ex);
          }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<FourInts> result = new ArrayList<>();
            for (Text record : values) {
                result.add(new FourInts(record.toString()));
            }
            java.util.Collections.sort(result, FourInts.cmp);
            mos.write(SORTED_COUNTS, key, new LongWritable(result.size()));
            mos.write(SORTED_DATA, key, new Text(new MultipleFourInts(result).toString()));
        }
    }

    public static int run(Path input, Path samplingSuperdir, Path output, Configuration conf) throws Exception {
        LOG.info("Starting Phase Sorting Reducer");

        Job job = Job.getInstance(conf, "JOB: Phase Sorting Reducer");
        job.setJarByClass(Sorting.class);
        job.addCacheFile(new URI(samplingSuperdir + "/part-r-00000" + "#" + Sorting.SAMPLING_SPLIT_POINTS_CACHE));
        job.setNumReduceTasks(conf.getInt(Utils.REDUCERS_NO_KEY, 1));
        job.setMapperClass(PartitioningMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setReducerClass(SortingReducer.class);
        MultipleOutputs.addNamedOutput(job, SORTED_COUNTS, TextOutputFormat.class, IntWritable.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, SORTED_DATA, TextOutputFormat.class, IntWritable.class, Text.class);

        LOG.info("Waiting for sorting reducer");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase sorting reducer, processed " + total + " key/value pairs");

        return ret;
    }
}
