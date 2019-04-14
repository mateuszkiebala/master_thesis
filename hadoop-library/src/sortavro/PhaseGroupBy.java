package sortavro;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
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
import sortavro.avro_types.group_by.*;
import sortavro.avro_types.utils.KeyRecord;

/**
 *
 * @author mateuszkiebala
 */
public class PhaseGroupBy {

    static final Log LOG = LogFactory.getLog(PhaseGroupBy.class);

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        Schema statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
        Schema keyRecordSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.GROUP_BY_KEY_SCHEMA);

        GroupByRecordSchemaCreator.setSchema(statisticerSchema, keyRecordSchema);
        MultipleGroupByRecordsSchemaCreator.setSchema(GroupByRecord.getClassSchema());
        MultipleMainObjectsSchemaCreator.setMainObjectSchema(mainObjectSchema);
    }

    public static class GroupByMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<MultipleGroupByRecords>> {

        private Configuration conf;
        private Schema statisticerSchema;
        private Schema keyRecordSchema;
        private final AvroValue<MultipleGroupByRecords> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
            keyRecordSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.GROUP_BY_KEY_SCHEMA);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            List<GroupByRecord> masterResult = new ArrayList<>();
            List<GroupByRecord> thisResult = new ArrayList<>();
            try {
                Class statisticerClass = SpecificData.get().getClass(statisticerSchema);
                Class keyRecordClass = SpecificData.get().getClass(keyRecordSchema);

                List<GroupByRecord> groupByRecords = new ArrayList<>();
                MultipleMainObjects mainObjects = SpecificData.get().deepCopy(MultipleMainObjects.getClassSchema(), value.datum());
                for (GenericRecord record : mainObjects.getRecords()) {
                    Statisticer statisticer = (Statisticer) statisticerClass.newInstance();
                    statisticer.init(record);

                    KeyRecord keyRecord = (KeyRecord) keyRecordClass.newInstance();
                    keyRecord.create(record);
                    groupByRecords.add(new GroupByRecord(statisticer, keyRecord));
                }

                KeyRecord minKey = null;
                KeyRecord maxKey = null;
                Map<KeyRecord, Statisticer> grouped = new HashMap<>();
                for (GroupByRecord record : groupByRecords) {
                    KeyRecord keyRecord = record.getKey();
                    if (grouped.containsKey(keyRecord)) {
                        Statisticer mapStatisticer = grouped.get(keyRecord);
                        grouped.put(keyRecord, mapStatisticer.merge(record.getStatisticer()));
                    } else {
                        grouped.put(keyRecord, record.getStatisticer());
                    }

                    minKey = minKey == null ? keyRecord : KeyRecord.min(minKey, keyRecord);
                    maxKey = maxKey == null ? keyRecord : KeyRecord.max(maxKey, keyRecord);
                }

                for (Map.Entry<KeyRecord, Statisticer> entry : grouped.entrySet()) {
                    GroupByRecord record = new GroupByRecord(entry.getValue(), entry.getKey());
                    if (entry.getKey().equals(minKey) || entry.getKey().equals(maxKey)) {
                        masterResult.add(record);
                    } else {
                        thisResult.add(record);
                    }
                }
            } catch (Exception e) {
                System.err.println("Cannot run group_by: " + e.toString());
            }

            avVal.datum(new MultipleGroupByRecords(thisResult));
            context.write(key, avVal);

            Integer masterIndex = 0;
            avVal.datum(new MultipleGroupByRecords(masterResult));
            avKey.datum(masterIndex);
            context.write(avKey, avVal);
        }
    }
/*
    public static class GroupByReducer extends Reducer<AvroKey<Integer>, AvroValue<MultipleGroupByRecords>, AvroKey<Integer>, AvroValue<MultipleGroupByRecords>> {

        private Integer[] prefixedPartitionSizes;
        private Configuration conf;
        private final AvroKey<Integer> avKey = new AvroKey<>();
        private final AvroValue<MultipleRankWrappers> avVal = new AvroValue<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            prefixedPartitionSizes = readPartitionSizes(conf);
            for (int i = 1; i < prefixedPartitionSizes.length; i++) {
                prefixedPartitionSizes[i] = prefixedPartitionSizes[i - 1] + prefixedPartitionSizes[i];
            }
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<MultipleMainObjects>> values, Context context) throws IOException, InterruptedException {
            int partitionIndex = key.datum();
            ArrayList<RankWrapper> result = new ArrayList<>();
            for (AvroValue<MultipleMainObjects> o : values) {
                MultipleMainObjects mainObjects = SpecificData.get().deepCopy(MultipleMainObjects.getClassSchema(), o.datum());
                int i = 0;
                for (GenericRecord record : mainObjects.getRecords()) {
                    int rank = partitionIndex == 0 ? i : prefixedPartitionSizes[partitionIndex-1] + i;
                    result.add(new RankWrapper(rank, record));
                    i++;
                }
            }
            avKey.datum(key.datum());
            avVal.datum(new MultipleRankWrappers(result));
            context.write(avKey, avVal);
        }
    }
*/
    public static int run(Path input, Path output, Configuration conf) throws Exception {
        LOG.info("starting group_by");
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: PhaseGroupBy");
        job.setJarByClass(PhasePrefix.class);
        //job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setNumReduceTasks(0);
        job.setMapperClass(GroupByMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, MultipleGroupByRecords.getClassSchema());

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleGroupByRecords.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
