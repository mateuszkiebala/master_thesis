package sortavro;

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
import sortavro.avro_types.statistics.*;
import sortavro.avro_types.terasort.*;

public class PhasePrefix {

    static final Log LOG = LogFactory.getLog(PhasePrefix.class);
    public static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        Schema statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
        StatisticsRecord.setSchema(statisticerSchema, mainObjectSchema);
        MultipleMainObjects.setSchema(mainObjectSchema);
        MultipleStatisticRecords.setSchema(StatisticsRecord.getClassSchema());
    }

    public static class PartitionPrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<MultipleStatisticRecords>> {

        private Configuration conf;
        private Schema mainObjectSchema;
        private Schema statisticerSchema;
        private Statisticer[] partitionPrefixedStatistics;
        private final AvroValue<MultipleStatisticRecords> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        private void initPartitionPrefixedStatistics() {
            Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), statisticerSchema);
            GenericRecord[] partitionStatistics = Utils.readRecordsFromLocalFileAvro(conf, PhasePartitionStatistics.PARTITION_STATISTICS_CACHE, keyValueSchema);
            partitionPrefixedStatistics = new Statisticer[partitionStatistics.length];

            for (GenericRecord ps : partitionStatistics) {
                int paritionIndex = (Integer) ps.get("key");
                Statisticer paritionStatisticer = (Statisticer) SpecificData.get().deepCopy(statisticerSchema, ps.get("value"));
                partitionPrefixedStatistics[paritionIndex] = paritionStatisticer;
            }

            for (int i = 1; i < partitionPrefixedStatistics.length; i++) {
                partitionPrefixedStatistics[i] = partitionPrefixedStatistics[i-1].merge(partitionPrefixedStatistics[i]);
            }
        }

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
            statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
            initPartitionPrefixedStatistics();
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            List<StatisticsRecord> statsRecords = new ArrayList<>();
            try {
                Class statisticerClass = SpecificData.get().getClass(statisticerSchema);
                Statisticer partitionStatistics = key.datum() == 0 ? null : partitionPrefixedStatistics[key.datum()-1];
                Statisticer statsMerger = null;
                MultipleMainObjects mainObjects = SpecificData.get().deepCopy(MultipleMainObjects.getClassSchema(), value.datum());
                for (GenericRecord record : mainObjects.getRecords()) {
                    Statisticer statisticer = (Statisticer) statisticerClass.newInstance();
                    statisticer.init(record);
                    if (statsMerger == null) {
                        statsMerger = statisticer;
                    } else {
                        statsMerger = statsMerger.merge(statisticer);
                    }

                    Statisticer prefixResult;
                    if (partitionStatistics == null) {
                        prefixResult = statsMerger;
                    } else {
                        prefixResult = statsMerger.merge(partitionStatistics);
                    }
                    statsRecords.add(new StatisticsRecord(SpecificData.get().deepCopy(statisticerSchema, prefixResult), record));
                }
            } catch (Exception e) {
                System.err.println("Cannot create prefix statistics: " + e.toString());
            }

            avVal.datum(new MultipleStatisticRecords(statsRecords));
            context.write(key, avVal);
        }
    }

    public static int run(Path input, Path output, URI partitionStatisticsURI, Configuration conf) throws Exception {
        LOG.info("starting prefix");
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase prefix");
        job.addCacheFile(partitionStatisticsURI);
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(PartitionPrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, MultipleStatisticRecords.getClassSchema());

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleStatisticRecords.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
