package minimal_algorithms.phases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configurable;
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
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.sending.Sender;
import minimal_algorithms.utils.Utils;

/**
 *
 * @author jsroka, mateuszkiebala
 */
public class PhaseSampling {
    static final Log LOG = LogFactory.getLog(PhaseSampling.class);

    public static class SamplerMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, AvroValue<GenericRecord>> implements Configurable {

        private Configuration conf;
        private final Random random = new Random();
        private Sender sender;
        private int ratioForRandom;

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            ratioForRandom = Utils.getRatioForRandomKey(conf);
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void setup(Context ctx) {
            sender = new Sender(ctx);
        }

        @Override
        public void map(AvroKey<GenericRecord> record, NullWritable nV, Context context) throws IOException, InterruptedException {
            if (random.nextInt(ratioForRandom) == 0) {
                sender.send(NullWritable.get(), new AvroValue<GenericRecord>(record.datum()));
            }
        }
    }

    public static class ComputeBoundsForSortingReducer extends Reducer<NullWritable, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

        private Schema baseSchema;
        private Comparator<GenericRecord> cmp;
        private Sender sender;
        private int noOfSplitPoints;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            super.setup(ctx);
            noOfSplitPoints = Utils.getStripsCount(ctx.getConfiguration()) - 1;
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            baseSchema = Utils.retrieveSchemaFromConf(ctx.getConfiguration(), BaseConfig.BASE_SCHEMA_KEY);
            sender = new Sender(ctx);
        }
        
        @Override
        protected void reduce(NullWritable nV, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            ArrayList<GenericRecord> result = new ArrayList<>();
            for (AvroValue<GenericRecord> record : values) {
                result.add(Utils.deepCopy(baseSchema, record.datum()));
            }

            java.util.Collections.sort(result, cmp);
            int step = result.size() / (noOfSplitPoints+1);

            AvroKey<GenericRecord> avKey = new AvroKey<>();
            for (int i = 1; i <= noOfSplitPoints; i++) {
                avKey.datum(result.get(i * step));
                sender.send(avKey, NullWritable.get());
            }
        }
    }

    public static int run(Path input, Path output, BaseConfig config) throws IOException, InterruptedException, ClassNotFoundException {
        LOG.info("Starting phase sampling");

        Job job = Job.getInstance(config.getConf(), "JOB: Phase one sampling");
        job.setJarByClass(PhaseSampling.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        job.setMapperClass(SamplerMapper.class);
        job.setReducerClass(ComputeBoundsForSortingReducer.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(AvroValue.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);

        Schema baseSchema = config.getBaseSchema();
        AvroJob.setInputKeySchema(job, baseSchema);
        AvroJob.setMapOutputValueSchema(job, baseSchema);
        AvroJob.setOutputKeySchema(job, baseSchema);

        LOG.info("Waiting for phase sampling");
        int ret = job.waitForCompletion(true) ? 0 : 1;
        
        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase sampling, processed " + total + " key/value pairs");

        return ret;
    }
}
