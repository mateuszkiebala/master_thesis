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

public class PhasePrefix {

    static final Log LOG = LogFactory.getLog(PhasePrefix.class);
    public static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);
        StatisticsRecord.setSchema(statisticsAggregatorSchema, mainObjectSchema);
        MultipleMainObjects.setSchema(mainObjectSchema);
        MultipleStatisticRecords.setSchema(StatisticsRecord.getClassSchema());
        SendWrapper.setSchema(mainObjectSchema, statisticsAggregatorSchema);
    }

    public static class PrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<SendWrapper>> {

        private Configuration conf;
        private Schema mainObjectSchema;
        private Schema statisticsAggregatorSchema;
        private StatisticsAggregator[] partitionPrefixedStatistics;
        private int machinesCount;
        private final AvroValue<SendWrapper> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        private StatisticsAggregator getPartitionStatistics(MultipleMainObjects value) throws IOException, InterruptedException {
            StatisticsAggregator statsMerger = null;
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                for (GenericRecord record : value.getRecords()) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                }
            } catch (Exception e) {
                System.err.println("Cannot create partition statistics: " + e.toString());
            }
            return statsMerger;
        }

        @Override
        public void setup(Context ctx) {
            conf = ctx.getConfiguration();
            setSchemas(conf);
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);
            machinesCount = conf.getInt(PhaseSampling.NO_OF_STRIPS_KEY, 0);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator partitionStatistics = getPartitionStatistics(value.datum());
            SendWrapper wrapperedPartitionStatistics = new SendWrapper();
            wrapperedPartitionStatistics.setRecord2(partitionStatistics);

            for (int i = key.datum() + 1; i < machinesCount; i++) {
                avKey.datum(i);
                avVal.datum(wrapperedPartitionStatistics);
                context.write(avKey, avVal);
            }

            List<SendWrapper> toSend = new ArrayList<>();
            for (GenericRecord record : value.datum().getRecords()) {
                SendWrapper sw = new SendWrapper();
                sw.setRecord1(record);
                avVal.datum(sw);
                context.write(key, avVal);
            }
        }
    }

    public static class PrefixReducer extends Reducer<AvroKey<Integer>, AvroValue<SendWrapper>, AvroKey<Integer>, AvroValue<MultipleStatisticRecords>> {

        private final AvroValue<MultipleStatisticRecords> avVal = new AvroValue<>();
        private Configuration conf;
        private Schema statisticsAggregatorSchema;

        private StatisticsAggregator getPartitionStatisticsValue(List<GenericRecord> records) {
            StatisticsAggregator result = null;
            try {
                for (GenericRecord record : records) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) SpecificData.get().deepCopy(statisticsAggregatorSchema, record);
                    result = result == null ? statisticsAggregator : result.merge(statisticsAggregator);
                }
            } catch (Exception e) {
                System.err.println("Cannot create partition statistics value: " + e.toString());
            }
            return result;
        }

        private List<StatisticsRecord> getPrefixes(List<GenericRecord> records, StatisticsAggregator partitionStatistics) {
            List<StatisticsRecord> result = new ArrayList<>();
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                StatisticsAggregator statsMerger = null;
                for (GenericRecord record : records) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                    StatisticsAggregator prefixResult = partitionStatistics == null ? statsMerger : statsMerger.merge(partitionStatistics);
                    result.add(new StatisticsRecord(SpecificData.get().deepCopy(statisticsAggregatorSchema, prefixResult), record));
                }
            } catch (Exception e) {
                System.err.println("Cannot create prefix statistics: " + e.toString());
            }
            return result;
        }

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<SendWrapper>> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<GenericRecord>> groupedRecords = SendingUtils.partitionRecords(values);
            StatisticsAggregator partitionStatistics = getPartitionStatisticsValue(groupedRecords.get(2));
            avVal.datum(new MultipleStatisticRecords(getPrefixes(groupedRecords.get(1), partitionStatistics)));
            context.write(key, avVal);
        }
    }

    public static int run(Path input, Path output, URI partitionStatisticsURI, Configuration conf) throws Exception {
        LOG.info("starting prefix");
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase prefix");
        job.addCacheFile(partitionStatisticsURI);
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

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
