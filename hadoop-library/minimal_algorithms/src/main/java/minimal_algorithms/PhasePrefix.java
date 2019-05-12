package minimal_algorithms;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.terasort.*;
import minimal_algorithms.avro_types.utils.*;
import minimal_algorithms.sending.SendingUtils;
import minimal_algorithms.sending.AvroSender;
import minimal_algorithms.statistics.StatisticsUtils;
import minimal_algorithms.config.StatisticsConfig;

public class PhasePrefix {

    static final Log LOG = LogFactory.getLog(PhasePrefix.class);
    public static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.BASE_SCHEMA);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA);
        StatisticsRecord.setSchema(statisticsAggregatorSchema, baseSchema);
        MultipleBaseRecords.setSchema(baseSchema);
        MultipleStatisticRecords.setSchema(StatisticsRecord.getClassSchema());
        SendWrapper.setSchema(baseSchema, statisticsAggregatorSchema);
    }

    public static class PrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleBaseRecords>, AvroKey<Integer>, AvroValue<SendWrapper>> {

        private Configuration conf;
        private StatisticsAggregator[] partitionPrefixedStatistics;
        private int machinesCount;
        private AvroSender sender;
        private StatisticsUtils statsUtiler;

        @Override
        public void setup(Context ctx) {
            conf = ctx.getConfiguration();
            setSchemas(conf);
            machinesCount = conf.getInt(StatisticsConfig.NO_OF_STRIPS_KEY, 0);
            sender = new AvroSender(ctx);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA));
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleBaseRecords> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator partitionStatistics = statsUtiler.foldLeftRecords(value.datum().getRecords(), null);
            SendWrapper wrapperedPartitionStatistics = new SendWrapper();
            wrapperedPartitionStatistics.setRecord2(partitionStatistics);
            sender.sendToRangeMachines(wrapperedPartitionStatistics, key.datum() + 1, machinesCount);

            List<SendWrapper> toSend = new ArrayList<>();
            for (GenericRecord record : value.datum().getRecords()) {
                SendWrapper sw = new SendWrapper();
                sw.setRecord1(record);
                sender.send(key, sw);
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
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA));
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<SendWrapper>> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<GenericRecord>> groupedRecords = SendingUtils.partitionRecords(values);
            System.out.println(groupedRecords);
            StatisticsAggregator partitionStatistics = statsUtiler.foldLeftAggregators(groupedRecords.get(2));
            List<StatisticsAggregator> statistics = statsUtiler.scanLeftRecords(groupedRecords.get(1), partitionStatistics);
            System.out.println(statistics);
            List<StatisticsRecord> statsRecords = statsUtiler.zip(statistics, groupedRecords.get(1));
            System.out.println(statsRecords);
            sender.send(key, new MultipleStatisticRecords(statsRecords));
        }
    }

    public static int run(Path input, Path output, StatisticsConfig statsConfig) throws Exception {
        LOG.info("starting prefix");
        Configuration conf = statsConfig.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase prefix");
        job.setJarByClass(PhasePrefix.class);
        //job.setNumReduceTasks(0);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + StatisticsConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleBaseRecords.getClassSchema());

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

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
