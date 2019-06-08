package minimal_algorithms.phases;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.utils.*;
import minimal_algorithms.sending.*;
import minimal_algorithms.semi_join.*;
import minimal_algorithms.config.SemiJoinConfig;

public class PhaseSemiJoin {

    static final Log LOG = LogFactory.getLog(PhaseSemiJoin.class);

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, SemiJoinConfig.BASE_SCHEMA_KEY);
        Schema keyRecordSchema = Utils.retrieveSchemaFromConf(conf, SemiJoinConfig.KEY_RECORD_SCHEMA_KEY);
        MultipleRecords.setSchema(baseSchema);
        SendWrapper.setSchema(keyRecordSchema, baseSchema);
    }

    public static class TBoundsMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleRecords>, AvroKey<Integer>, AvroValue<SendWrapper>> {

        private Configuration conf;
        private Schema keyRecordSchema;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            keyRecordSchema = Utils.retrieveSchemaFromConf(conf, SemiJoinConfig.KEY_RECORD_SCHEMA_KEY);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleRecords> value, Context context) throws IOException, InterruptedException {
            KeyRecord minTKey = null;
            KeyRecord maxTKey = null;
            for (GenericRecord record : value.datum().getRecords()) {
                SemiJoinRecord semiJoinRecord = (SemiJoinRecord) record;
                if (semiJoinRecord.isT()) {
                    KeyRecord keyRecord = KeyRecord.create(keyRecordSchema, semiJoinRecord);
                    minTKey = minTKey == null ? keyRecord : KeyRecord.min(minTKey, keyRecord);
                    maxTKey = maxTKey == null ? keyRecord : KeyRecord.max(maxTKey, keyRecord);
                }
                sender.send(key, new SendWrapper(null, record));
            }

            if (minTKey != null && maxTKey != null) {
                sender.sendToAllMachines(new SendWrapper(minTKey, null));
                sender.sendToAllMachines(new SendWrapper(maxTKey, null));
            }
        }
    }

    public static class SemiJoinReducer extends Reducer<AvroKey<Integer>, AvroValue<SendWrapper>, AvroKey<Integer>, AvroValue<MultipleRecords>> {

        private Configuration conf;
        private Schema keyRecordSchema;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            keyRecordSchema = Utils.retrieveSchemaFromConf(conf, SemiJoinConfig.KEY_RECORD_SCHEMA_KEY);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<SendWrapper>> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<GenericRecord>> records = SendingUtils.partitionRecords(values);
            HashSet<KeyRecord> tKeys = new HashSet<>();
            for (GenericRecord record : records.get(2)) {
                SemiJoinRecord semiJoinRecord = (SemiJoinRecord) record;
                if (semiJoinRecord.isT()) {
                    tKeys.add(KeyRecord.create(keyRecordSchema, semiJoinRecord));
                }
            }

            for (GenericRecord record : records.get(1)) {
                tKeys.add((KeyRecord) record);
            }

            List<GenericRecord> result = new ArrayList<>();
            for (GenericRecord record : records.get(2)) {
                SemiJoinRecord semiJoinRecord = (SemiJoinRecord) record;
                if (semiJoinRecord.isR() && tKeys.contains(KeyRecord.create(keyRecordSchema, semiJoinRecord))) {
                    result.add(record);
                }
            }
            sender.send(key, new MultipleRecords(result));
        }
    }

    public static int run(Path input, Path output, SemiJoinConfig semiJoinConfig) throws Exception {
        LOG.info("Starting Phase SemiJoin");
        Configuration conf = semiJoinConfig.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase SemiJoin");
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(TBoundsMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + SemiJoinConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SendWrapper.getClassSchema());

        job.setReducerClass(SemiJoinReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleRecords.getClassSchema());

        LOG.info("Waiting for phase SemiJoin");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase SemiJoin, processed " + total + " key/value pairs");

        return ret;
    }
}
