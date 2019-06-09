package minimal_algorithms.hadoop.phases;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.hadoop.utils.*;
import minimal_algorithms.hadoop.ranking.*;
import minimal_algorithms.hadoop.config.BaseConfig;
import minimal_algorithms.hadoop.sending.AvroSender;

public class PhasePerfectSort {

    static final Log LOG = LogFactory.getLog(PhasePerfectSort.class);

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA_KEY);
        MultipleRecords.setSchema(baseSchema);
        RankRecord.setSchema(baseSchema);
        MultipleRankRecords.setSchema(RankRecord.getClassSchema());
    }

    public static class PerfectBalanceMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleRankRecords>, AvroKey<Integer>, AvroValue<RankRecord>> {

        private Configuration conf;
        private AvroSender sender;
        private long itemsNoByMachines;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            itemsNoByMachines = Utils.getItemsNoByMachines(conf);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleRankRecords> value, Context context) throws IOException, InterruptedException {
            MultipleRankRecords baseRecords = MultipleRankRecords.deepCopy(value.datum());
            for (GenericRecord record : baseRecords.getRecords()) {
                long rank = ((RankRecord) record).getRank();
                sender.send(rank / itemsNoByMachines, record);
            }
        }
    }

    public static class SortingReducer extends Reducer<AvroKey<Integer>, AvroValue<RankRecord>, AvroKey<Integer>, AvroValue<MultipleRecords>> {

        private Configuration conf;
        private Comparator<GenericRecord> cmp;
        private AvroMultipleOutputs amos;
        private Schema baseSchema;
        private final AvroValue<Integer> aInt = new AvroValue<>();
        private final AvroValue<MultipleRecords> avValueMultRecords = new AvroValue<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            amos = new AvroMultipleOutputs(ctx);
            cmp = Utils.retrieveComparatorFromConf(conf);
            baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA_KEY);
        }

        public void cleanup(Context ctx) throws IOException {
            try {
                amos.close();
            } catch (InterruptedException ex) {
                Logger.getLogger(PhasePerfectSort.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        protected void reduce(AvroKey<Integer> avKey, Iterable<AvroValue<RankRecord>> values, Context context) throws IOException, InterruptedException {
            ArrayList<GenericRecord> objects = new ArrayList<>();
            for (AvroValue<RankRecord> record : values) {
                objects.add(Utils.deepCopy(baseSchema, record.datum().getRecord()));
            }
            java.util.Collections.sort(objects, cmp);

            aInt.datum(objects.size());
            amos.write(BaseConfig.SORTED_COUNTS_TAG, avKey, aInt);

            avValueMultRecords.datum(new MultipleRecords(objects));
            amos.write(BaseConfig.SORTED_DATA_TAG, avKey, avValueMultRecords);
        }
    }

    public static int run(Path input, Path output, BaseConfig config) throws Exception {
        LOG.info("Starting Phase PerfectSort");
        Configuration conf = config.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase PerfectSort");
        job.setJarByClass(PhasePerfectSort.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PerfectBalanceMapper.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRankRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, RankRecord.getClassSchema());

        job.setReducerClass(SortingReducer.class);
        AvroMultipleOutputs.addNamedOutput(job, BaseConfig.SORTED_DATA_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), MultipleRecords.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, BaseConfig.SORTED_COUNTS_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));

        LOG.info("Waiting for phase PerfectSort");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase PerfectSort, processed " + total + " key/value pairs");

        return ret;
    }
}
