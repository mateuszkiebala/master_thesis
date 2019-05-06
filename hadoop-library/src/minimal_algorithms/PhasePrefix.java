package minimal_algorithms;

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
        MultipleSendWrappers.setSchema(SendWrapper.getClassSchema());
    }

    public static class PrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<MultipleSendWrappers>> {

        private Configuration conf;
        private Schema mainObjectSchema;
        private Schema statisticsAggregatorSchema;
        private StatisticsAggregator[] partitionPrefixedStatistics;
        private final AvroValue<MultipleSendWrappers> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        /*private void initPartitionPrefixedStatistics() {
            Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), statisticsAggregatorSchema);
            GenericRecord[] partitionStatistics = Utils.readRecordsFromLocalFileAvro(conf, PhasePartitionStatistics.PARTITION_STATISTICS_CACHE, keyValueSchema);
            partitionPrefixedStatistics = new StatisticsAggregator[partitionStatistics.length];

            for (GenericRecord ps : partitionStatistics) {
                int paritionIndex = (Integer) ps.get("key");
                StatisticsAggregator paritionStatisticsAggregator = (StatisticsAggregator) SpecificData.get().deepCopy(statisticsAggregatorSchema, ps.get("value"));
                partitionPrefixedStatistics[paritionIndex] = paritionStatisticsAggregator;
            }

            for (int i = 1; i < partitionPrefixedStatistics.length; i++) {
                partitionPrefixedStatistics[i] = partitionPrefixedStatistics[i-1].merge(partitionPrefixedStatistics[i]);
            }
        }*/

        private StatisticsAggregator getPartitionStatistics(MultipleMainObjects value) throws IOException, InterruptedException {
            StatisticsAggregator statsMerger = null;
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                for (GenericRecord record : value.getRecords()) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    if (statsMerger == null) {
                        statsMerger = statisticsAggregator;
                    } else {
                        statsMerger = statsMerger.merge(statisticsAggregator);
                    }
                }
            } catch (Exception e) {
                System.err.println("Cannot create partition statistics: " + e.toString());
            }
            return statsMerger;
        }

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator partitionStatistics = getPartitionStatistics(value.datum());
            SendWrapper sendWrapperPartitionStatistics = new SendWrapper();
            sendWrapperPartitionStatistics.setRecord2(partitionStatistics);

            List<SendWrapper> toSend = new ArrayList<>();
            toSend.add(sendWrapperPartitionStatistics);
            for (GenericRecord record : value.datum().getRecords()) {
                SendWrapper sw = new SendWrapper();
                sw.setRecord1(record);
                toSend.add(sw);
            }

            avVal.datum(new MultipleSendWrappers(toSend));
            context.write(key, avVal);
        }
    }

    public static class PrefixReducer extends Reducer<AvroKey<Integer>, AvroValue<MultipleSendWrappers>, AvroKey<Integer>, AvroValue<MultipleStatisticRecords>> {

        private final AvroValue<MultipleStatisticRecords> avVal = new AvroValue<>();
        private Configuration conf;
        private Schema statisticsAggregatorSchema;

        private StatisticsAggregator getPartitionStatisticsValue(List<GenericRecord> records) {
            StatisticsAggregator result = null;
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                for (GenericRecord record : records) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    if (result == null) {
                        result = statisticsAggregator;
                    } else {
                        result = result.merge(statisticsAggregator);
                    }
                }
            } catch (Exception e) {
                System.err.println("Cannot create partition statistics value: " + e.toString());
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
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<MultipleSendWrappers>> values, Context context) throws IOException, InterruptedException {
            List<GenericRecord> records1 = new ArrayList<>();
            List<GenericRecord> records2 = new ArrayList<>();
            for (AvroValue<MultipleSendWrappers> value : values) {
                MultipleSendWrappers sendWrappers = SpecificData.get().deepCopy(MultipleSendWrappers.getClassSchema(), value.datum());
                records1.addAll(sendWrappers.select1Records());
                records2.addAll(sendWrappers.select2Records());
            }

            StatisticsAggregator partitionStatistics = getPartitionStatisticsValue(records1);
        }
    }

    /*
    List<SendWrapper> toSend = new ArrayList<>();
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                StatisticsAggregator partitionStatistics = key.datum() == 0 ? null : partitionPrefixedStatistics[key.datum()-1];
                StatisticsAggregator statsMerger = null;
                MultipleMainObjects mainObjects = SpecificData.get().deepCopy(MultipleMainObjects.getClassSchema(), value.datum());
                for (GenericRecord record : mainObjects.getRecords()) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    if (statsMerger == null) {
                        statsMerger = statisticsAggregator;
                    } else {
                        statsMerger = statsMerger.merge(statisticsAggregator);
                    }

                    StatisticsAggregator prefixResult;
                    if (partitionStatistics == null) {
                        prefixResult = statsMerger;
                    } else {
                        prefixResult = statsMerger.merge(partitionStatistics);
                    }
                    StatisticsAggregator statsAgg1 = SpecificData.get().deepCopy(statisticsAggregatorSchema, prefixResult);
                    SendWrapper sw = new SendWrapper();
                    sw.setRecord1(statsAgg1);
                    statsRecords.add(sw);
                }
            } catch (Exception e) {
                System.err.println("Cannot create prefix statistics: " + e.toString());
            }

            MultipleSendWrappers msw = new MultipleSendWrappers(statsRecords);
            System.out.println(msw);
            avVal.datum(msw);
            context.write(key, avVal);
     */

    public static int run(Path input, Path output, URI partitionStatisticsURI, Configuration conf) throws Exception {
        LOG.info("starting prefix");
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase prefix");
        job.addCacheFile(partitionStatisticsURI);
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(PrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, MultipleSendWrappers.getClassSchema());

        job.setReducerClass(PrefixReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleSendWrappers.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
