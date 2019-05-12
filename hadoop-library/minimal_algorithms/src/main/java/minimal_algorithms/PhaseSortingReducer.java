package minimal_algorithms;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.avro_types.terasort.*;
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.sending.AvroSender;

/**
 *
 * @author jsroka, mateuszkiebala
 */
public class PhaseSortingReducer {

    static final Log LOG = LogFactory.getLog(PhaseSortingReducer.class);

    public static final String SAMPLING_SPLIT_POINTS_CACHE = "sampling_split_points.cache";

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA);
        MultipleBaseRecords.setSchema(baseSchema);
    }

    public static class PartitioningMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>> {

        private GenericRecord[] splitPoints;
        private Configuration conf;
        private Comparator<GenericRecord> cmp;
        private Schema baseSchema;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            splitPoints = Utils.readRecordsFromCacheAvro(conf, PhaseSortingReducer.SAMPLING_SPLIT_POINTS_CACHE, BaseConfig.BASE_SCHEMA);
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA);
            sender = new AvroSender(ctx);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            int dummy = java.util.Arrays.binarySearch(splitPoints, Utils.deepCopy(baseSchema, key.datum()), cmp);
            sender.send(dummy >= 0 ? dummy : -dummy - 1, new AvroValue<GenericRecord>(key.datum()));
        }
    }

    public static class SortingReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<Integer>, AvroValue<MultipleBaseRecords>> {

        private Configuration conf;
        private Comparator<GenericRecord> cmp;
        private AvroMultipleOutputs amos;
        private Schema baseSchema;
        private final AvroValue<Integer> aInt = new AvroValue<>();
        private final AvroValue<MultipleBaseRecords> avValueMultRecords = new AvroValue<>();
        
        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            amos = new AvroMultipleOutputs(ctx);
            cmp = Utils.retrieveComparatorFromConf(conf);
            baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA);
        }

        public void cleanup(Context ctx) throws IOException {
            try {
                amos.close();
            } catch (InterruptedException ex) {
                Logger.getLogger(PhaseSortingReducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        protected void reduce(AvroKey<Integer> avKey, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            ArrayList<GenericRecord> result = new ArrayList<>();
            for (AvroValue<GenericRecord> record : values) {
                result.add(Utils.deepCopy(baseSchema, record.datum()));
            }
            java.util.Collections.sort(result, cmp);

            aInt.datum(result.size());
            amos.write(BaseConfig.SORTED_COUNTS_TAG, avKey, aInt);

            avValueMultRecords.datum(new MultipleBaseRecords(result));
            amos.write(BaseConfig.SORTED_DATA_TAG, avKey, avValueMultRecords);
        }
    }

    public static int run(Path input, Path samplingSuperdir, Path output, BaseConfig config) throws Exception {
        LOG.info("Starting Phase Sorting Reducer");
        Configuration conf = config.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase Sorting Reducer");
        job.setJarByClass(PhaseSortingReducer.class);
        job.addCacheFile(new URI(samplingSuperdir + "/part-r-00000.avro" + "#" + PhaseSortingReducer.SAMPLING_SPLIT_POINTS_CACHE));
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PartitioningMapper.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, config.getBaseSchema());
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, config.getBaseSchema());

        job.setReducerClass(SortingReducer.class);
        AvroMultipleOutputs.addNamedOutput(job, BaseConfig.SORTED_DATA_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), MultipleBaseRecords.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, BaseConfig.SORTED_COUNTS_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));

        LOG.info("Waiting for sorting reducer");
        int ret = (job.waitForCompletion(true) ? 0 : 1);

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase sorting reducer, processed " + total + " key/value pairs");

        return ret;
    }
}
