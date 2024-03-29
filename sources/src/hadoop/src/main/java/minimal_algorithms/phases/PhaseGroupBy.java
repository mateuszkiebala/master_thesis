package minimal_algorithms.hadoop.phases;

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
import minimal_algorithms.hadoop.statistics.*;
import minimal_algorithms.hadoop.group_by.*;
import minimal_algorithms.hadoop.utils.*;
import minimal_algorithms.hadoop.sending.AvroSender;
import minimal_algorithms.hadoop.config.GroupByConfig;

public class PhaseGroupBy {

    static final Log LOG = LogFactory.getLog(PhaseGroupBy.class);
    static final Integer MASTER_MACHINE_INDEX = 0;

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.BASE_SCHEMA_KEY);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY);
        Schema keyRecordSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.KEY_RECORD_SCHEMA_KEY);

        GroupByRecord.setSchema(statisticsAggregatorSchema, keyRecordSchema);
        MultipleGroupByRecords.setSchema(GroupByRecord.getClassSchema());
        MultipleRecords.setSchema(baseSchema);
    }

    public static class GroupByMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleRecords>, AvroKey<Integer>, AvroValue<GroupByRecord>> {

        private Configuration conf;
        private Schema statisticsAggregatorSchema;
        private Schema keyRecordSchema;
        private Comparator<GenericRecord> cmp;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY);
            keyRecordSchema = Utils.retrieveSchemaFromConf(conf, GroupByConfig.KEY_RECORD_SCHEMA_KEY);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleRecords> value, Context context) throws IOException, InterruptedException {
            try {
                List<GroupByRecord> groupByRecords = new ArrayList<>();
                for (GenericRecord record : MultipleRecords.deepCopy(value.datum()).getRecords()) {
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
                        grouped.put(keyRecord, StatisticsAggregator.safeMerge(grouped.get(keyRecord), record.getStatisticsAggregator()));
                    } else {
                        grouped.put(keyRecord, record.getStatisticsAggregator());
                    }

                    minKey = minKey == null ? keyRecord : KeyRecord.min(minKey, keyRecord);
                    maxKey = maxKey == null ? keyRecord : KeyRecord.max(maxKey, keyRecord);
                }

                for (Map.Entry<KeyRecord, StatisticsAggregator> entry : grouped.entrySet()) {
                    GroupByRecord record = new GroupByRecord(entry.getValue(), entry.getKey());
                    int dstMachineIndex = entry.getKey().equals(minKey) || entry.getKey().equals(maxKey) ? MASTER_MACHINE_INDEX : key.datum();
                    sender.send(dstMachineIndex, record);
                }
            } catch (Exception e) {
                System.err.println("Cannot run group_by: " + e.toString());
            }
        }
    }

    public static class GroupByReducer extends Reducer<AvroKey<Integer>, AvroValue<GroupByRecord>, AvroKey<Integer>, AvroValue<MultipleGroupByRecords>> {

        private Configuration conf;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GroupByRecord>> values, Context context) throws IOException, InterruptedException {
            List<GroupByRecord> result = new ArrayList<>();
            if (key.datum().equals(MASTER_MACHINE_INDEX)) {
                Map<KeyRecord, StatisticsAggregator> grouped = new HashMap<>();

                for (AvroValue<GroupByRecord> value : values) {
                    GroupByRecord record = GroupByRecord.deepCopy(value.datum());
                    KeyRecord keyRecord = record.getKey();
                    if (grouped.containsKey(keyRecord)) {
                        grouped.put(keyRecord, StatisticsAggregator.safeMerge(grouped.get(keyRecord), record.getStatisticsAggregator()));
                    } else {
                        grouped.put(keyRecord, record.getStatisticsAggregator());
                    }
                }

                for (Map.Entry<KeyRecord, StatisticsAggregator> entry : grouped.entrySet()) {
                    result.add(new GroupByRecord(entry.getValue(), entry.getKey()));
                }
            } else {
                for (AvroValue<GroupByRecord> value : values) {
                    result.add(GroupByRecord.deepCopy(value.datum()));
                }
            }
            sender.send(key, new MultipleGroupByRecords(result));
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

        FileInputFormat.setInputPaths(job, input + "/" + GroupByConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRecords.getClassSchema());

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

        LOG.info("Waiting for phase GroupBy");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase GroupBy, processed " + total + " key/value pairs");

        return ret;
    }
}
