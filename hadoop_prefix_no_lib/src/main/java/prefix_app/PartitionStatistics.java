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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class PartitionStatistics {

  static final Log LOG = LogFactory.getLog(PartitionStatistics.class);

  public static class PartitionPrefixMapper extends Mapper<Text, Text, NullWritable, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      long result = 0;
      for (FourInts fourInts : new MultipleFourInts(value.toString()).getValues()) {
        result += fourInts.getValue();
      }
      context.write(NullWritable.get(), new IndexedStatistics(key.toString(), result).toText());
    }
  }

  public static class PartitionStatisticsReducer extends Reducer<NullWritable, Text, NullWritable, LongWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<IndexedStatistics> result = new ArrayList<>();
      for (Text value : values) {
        result.add(new IndexedStatistics(value));
      }
      java.util.Collections.sort(result, IndexedStatistics.cmp);

      Long[] prefixPartitionStatistics = new Long[result.size()];
      for (int i = 0; i < result.size(); i++) {
        long stat = i == 0 ? 0 : result.get(i-1).statistics + prefixPartitionStatistics[i-1];
        prefixPartitionStatistics[i] = stat;
        context.write(NullWritable.get(), new LongWritable(stat));
      }
    }
  }

  public static int run(Path input, Path output, Configuration conf) throws Exception {
    LOG.info("starting phase PartitionStatistics");
    Job job = Job.getInstance(conf, "JOB: Phase PartitionStatistics");
    job.setJarByClass(PartitionStatistics.class);
    job.setNumReduceTasks(1);
    job.setMapperClass(PartitionPrefixMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, input + "/" + Sorting.SORTED_DATA_PATTERN);
    FileOutputFormat.setOutputPath(job, output);

    job.setReducerClass(PartitionStatisticsReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    LOG.info("Waiting for phase PartitionStatistics");
    int ret = job.waitForCompletion(true) ? 0 : 1;

    Counters counters = job.getCounters();
    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    LOG.info("Finished phase PartitionStatistics, processed " + total + " key/value pairs");

    return ret;
  }
}
