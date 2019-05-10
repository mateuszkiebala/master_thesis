package minimal_algorithms;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
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
import minimal_algorithms.avro_types.group_by.*;
import minimal_algorithms.avro_types.utils.KeyRecord;
import minimal_algorithms.sending.Sender;
import minimal_algorithms.statistics.StatisticsUtils;
import minimal_algorithms.config.GroupByConfig;

public class PhaseGroupBy {

    static final Log LOG = LogFactory.getLog(PhaseGroupBy.class);
    static final Integer MASTER_MACHINE_INDEX = 0;

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.BASE_SCHEMA);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.STATISTICS_AGGREGATOR_SCHEMA);
        Schema keyRecordSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.GROUP_BY_KEY_SCHEMA);

        GroupByRecord.setSchema(statisticsAggregatorSchema, keyRecordSchema);
        MultipleGroupByRecords.setSchema(GroupByRecord.getClassSchema());
        MultipleMainObjects.setSchema(mainObjectSchema);
    }

    public static class GroupByMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<GroupByRecord>> {

        private Configuration conf;
        private Schema statisticsAggregatorSchema;
        private Schema keyRecordSchema;
        private Comparator<GenericRecord> cmp;
        private Sender<GroupByRecord> sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.STATISTICS_AGGREGATOR_SCHEMA);
            keyRecordSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.GROUP_BY_KEY_SCHEMA);
            sender = new Sender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            try {
                List<GroupByRecord> groupByRecords = new ArrayList<>();
                MultipleMainObjects mainObjects = SpecificData.get().deepCopy(MultipleMainObjects.getClassSchema(), value.datum());
                for (GenericRecord record : mainObjects.getRecords()) {
                    StatisticsAggregator statisticsAggregator = StatisticsAggregator.create(statisticsAggregatorSchema, record);
                    KeyRecord keyRecord = KeyRecord.create(keyRecordSchema, record);
                    groupByRecords.add(new GroupByRecord(statisticsAggregator, keyRecord));
                }

                KeyRecord minKey = null;
                KeyRecord maxKey = null;
                Map<KeyRecord, StatisticsAggregator> grouped = new HashMap<>();
                for (GroupByRecord record : groupByRecords) {
                    KeyRecord keyRecord = record.getKey();
                    if (grouped.containsKey(keyRecord)) {
                        StatisticsAggregator mapStatisticsAggregator = grouped.get(keyRecord);
                        grouped.put(keyRecord, mapStatisticsAggregator.merge(record.getStatisticsAggregator()));
                    } else {
                        grouped.put(keyRecord, record.getStatisticsAggregator());
                    }

                    minKey = minKey == null ? keyRecord : KeyRecord.min(minKey, keyRecord);
                    maxKey = maxKey == null ? keyRecord : KeyRecord.max(maxKey, keyRecord);
                }

                for (Map.Entry<KeyRecord, StatisticsAggregator> entry : grouped.entrySet()) {
                    GroupByRecord record = new GroupByRecord(entry.getValue(), entry.getKey());
                    int dstMachineIndex = entry.getKey().equals(minKey) || entry.getKey().equals(maxKey) ? MASTER_MACHINE_INDEX : key.datum();
                    sender.sendToMachine(record, dstMachineIndex);
                }
            } catch (Exception e) {
                System.err.println("Cannot run group_by: " + e.toString());
            }
        }
    }

    public static class GroupByReducer extends Reducer<AvroKey<Integer>, AvroValue<GroupByRecord>, AvroKey<Integer>, AvroValue<MultipleGroupByRecords>> {

        private Configuration conf;
        private Sender<MultipleGroupByRecords> sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new Sender(ctx);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GroupByRecord>> values, Context context) throws IOException, InterruptedException {
            List<GroupByRecord> result = new ArrayList<>();
            if (key.datum().equals(MASTER_MACHINE_INDEX)) {
                Map<KeyRecord, StatisticsAggregator> grouped = new HashMap<>();

                for (AvroValue<GroupByRecord> value : values) {
                    GroupByRecord record = SpecificData.get().deepCopy(GroupByRecord.getClassSchema(), value.datum());
                    KeyRecord keyRecord = record.getKey();
                    if (grouped.containsKey(keyRecord)) {
                        StatisticsAggregator mapStatisticsAggregator = grouped.get(keyRecord);
                        grouped.put(keyRecord, mapStatisticsAggregator.merge(record.getStatisticsAggregator()));
                    } else {
                        grouped.put(keyRecord, record.getStatisticsAggregator());
                    }
                }

                for (Map.Entry<KeyRecord, StatisticsAggregator> entry : grouped.entrySet()) {
                    result.add(new GroupByRecord(entry.getValue(), entry.getKey()));
                }
            } else {
                for (AvroValue<GroupByRecord> value : values) {
                    result.add(SpecificData.get().deepCopy(GroupByRecord.getClassSchema(), value.datum()));
                }
            }
            sender.sendToMachine(new MultipleGroupByRecords(result), key);
        }
    }

    public static int run(Path input, Path output, GroupByConfig groupByConfig) throws Exception {
        LOG.info("starting group_by");
        Configuration conf = groupByConfig.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: PhaseGroupBy");
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(GroupByMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, GroupByRecord.getClassSchema());

        job.setReducerClass(GroupByReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleGroupByRecords.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
