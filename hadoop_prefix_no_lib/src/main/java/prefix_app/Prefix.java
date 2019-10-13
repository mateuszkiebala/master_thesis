package prefix_app;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
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

public class Prefix {

  static final Log LOG = LogFactory.getLog(Prefix.class);
  static final String PREFIX_PART_STATS_CACHE = "prefix_part_stats.cache";

  public static class PrefixMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class PrefixReducer extends Reducer<Text, Text, LongWritable, Text> {
    List<Long> machinePrefixStats;

    @Override
    public void setup(Context ctx) {
      ArrayList<String> words = Utils.readFromCache(new Path(PREFIX_PART_STATS_CACHE));
      machinePrefixStats = words.stream().map(w -> Long.parseLong(w)).collect(Collectors.toList());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<FourInts> fourIntsList = new ArrayList<>();
      for (Text value : values) {
        for (FourInts fourInts : new MultipleFourInts(value).getValues()) {
          fourIntsList.add(fourInts);
        }
      }
      java.util.Collections.sort(fourIntsList, FourInts.cmp);

      int partIndex = Integer.parseInt(key.toString());
      long prevPartStats = machinePrefixStats.get(partIndex);

      Long[] result = new Long[fourIntsList.size()];
      for (int i = 0; i < fourIntsList.size(); i++) {
        long stat = i == 0 ? prevPartStats : fourIntsList.get(i-1).getValue() + result[i-1];
        result[i] = stat;
        context.write(new LongWritable(stat), fourIntsList.get(i).toText());
      }
    }
  }

  public static int run(Path input, Path samplingSuperdir, Path output, Configuration conf) throws Exception {
    LOG.info("starting phase Prefix");
    Job job = Job.getInstance(conf, "JOB: Phase Prefix");
    job.setJarByClass(Prefix.class);
    job.setNumReduceTasks(conf.getInt(Utils.REDUCERS_NO_KEY, 1));
    job.setMapperClass(PrefixMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.addCacheFile(new URI(samplingSuperdir + "/part-r-00000" + "#" + PREFIX_PART_STATS_CACHE));
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setReducerClass(PrefixReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    LOG.info("Waiting for phase Prefix");
    int ret = job.waitForCompletion(true) ? 0 : 1;

    Counters counters = job.getCounters();
    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    LOG.info("Finished phase Prefix, processed " + total + " key/value pairs");

    return ret;
  }
}
