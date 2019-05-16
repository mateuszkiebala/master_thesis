package minimal_algorithms.phases;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.statistics.*;
import minimal_algorithms.utils.*;
import minimal_algorithms.sending.*;
import minimal_algorithms.config.StatisticsConfig;

public class PhasePrefix {

    static final Log LOG = LogFactory.getLog(PhasePrefix.class);

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.BASE_SCHEMA_KEY);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY);
        StatisticsRecord.setSchema(statisticsAggregatorSchema, baseSchema);
        MultipleRecords.setSchema(baseSchema);
        MultipleStatisticRecords.setSchema(StatisticsRecord.getClassSchema());
        SendWrapper.setSchema(baseSchema, statisticsAggregatorSchema);
    }

    public static class PrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleRecords>, AvroKey<Integer>, AvroValue<SendWrapper>> {

        private Configuration conf;
        private AvroSender sender;
        private StatisticsUtils statsUtiler;

        @Override
        public void setup(Context ctx) {
            conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY));
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleRecords> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator partitionStatistics = statsUtiler.foldLeftRecords(value.datum().getRecords(), null);
            sender.sendToAllHigherMachines(new SendWrapper(null, partitionStatistics), key.datum());
            for (GenericRecord record : value.datum().getRecords()) {
                sender.send(key, new SendWrapper(record, null));
            }
        }
    }

    public static class PrefixReducer extends Reducer<AvroKey<Integer>, AvroValue<SendWrapper>, AvroKey<Integer>, AvroValue<MultipleStatisticRecords>> {

        private AvroSender sender;
        private Configuration conf;
        private StatisticsUtils statsUtiler;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY));
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<SendWrapper>> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<GenericRecord>> groupedRecords = SendingUtils.partitionRecords(values);
            StatisticsAggregator partitionStatistics = statsUtiler.foldLeftAggregators(groupedRecords.get(2));
            List<StatisticsAggregator> statistics = statsUtiler.scanLeftRecords(groupedRecords.get(1), partitionStatistics);
            List<StatisticsRecord> statsRecords = statsUtiler.zip(statistics, groupedRecords.get(1));
            sender.send(key, new MultipleStatisticRecords(statsRecords));
        }
    }

    public static int run(Path input, Path output, StatisticsConfig statsConfig) throws Exception {
        LOG.info("Starting Phase Prefix");
        Configuration conf = statsConfig.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase Prefix");
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + StatisticsConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SendWrapper.getClassSchema());

        job.setReducerClass(PrefixReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleStatisticRecords.getClassSchema());

        LOG.info("Waiting for Phase Prefix");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished Phase Prefix, processed " + total + " key/value pairs");

        return ret;
    }
}
