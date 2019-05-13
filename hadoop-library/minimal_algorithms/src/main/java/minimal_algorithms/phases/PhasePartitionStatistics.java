package minimal_algorithms.phases;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.statistics.*;
import minimal_algorithms.sending.AvroSender;
import minimal_algorithms.utils.*;
import minimal_algorithms.config.StatisticsConfig;

public class PhasePartitionStatistics {

    static final Log LOG = LogFactory.getLog(PhasePartitionStatistics.class);
    static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.BASE_SCHEMA_KEY);
        MultipleBaseRecords.setSchema(baseSchema);
    }

    public static class PartitionPrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleBaseRecords>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private Configuration conf;
        private StatisticsUtils statsUtiler;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY));
            sender = new AvroSender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleBaseRecords> value, Context context) throws IOException, InterruptedException {
            sender.send(key, statsUtiler.foldLeftRecords(value.datum().getRecords()));
        }
    }

    public static class PartitionStatisticsReducer extends Reducer<AvroKey<Integer>, AvroValue<StatisticsAggregator>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            sender = new AvroSender(ctx);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<StatisticsAggregator>> values, Context context) throws IOException, InterruptedException {
            for (AvroValue<StatisticsAggregator> av : values) {
                sender.send(key, av.datum());
                break;
            }
        }
    }

    public static int run(Path input, Path output, StatisticsConfig statsConfig) throws Exception {
        LOG.info("starting phase PartitionStatistics");
        Configuration conf = statsConfig.getConf();
        setSchemas(conf);
        Schema statisticsAggregatorSchema = statsConfig.getStatisticsAggregatorSchema();

        Job job = Job.getInstance(conf, "JOB: Phase PartitionStatistics");
        job.setJarByClass(PhasePartitionStatistics.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(PartitionPrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + StatisticsConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleBaseRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, statisticsAggregatorSchema);

        job.setReducerClass(PartitionStatisticsReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, statisticsAggregatorSchema);

        LOG.info("Waiting for phase PartitionStatistics");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase PartitionStatistics, processed " + total + " key/value pairs");

        return ret;
    }
}
