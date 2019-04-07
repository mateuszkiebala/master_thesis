package sortavro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jsroka
 */
public class PhaseSampling {
    public static final String NO_OF_VALUES_KEY = "sampling.noOfValues";
    public static final String NO_OF_STRIPS_KEY = "sampling.noOfSplits";
    public static final String RATIO_FOR_RANDOM_KEY = "sampling.ratioForRandom";
    public static final int RATIO_FOR_RANDOM_DEFAULT = -1;
    public static final int NO_OF_KEYS_DEFAULT = -1;
    public static final int NO_OF_VALUES_DEFAULT = -1;

    public static class SamplerMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, AvroValue<GenericRecord>> implements Configurable {

        private Configuration conf;
        private final Random random = new Random();
        private final AvroValue<GenericRecord> avVal = new AvroValue();
        private int ratioForRandom;

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            ratioForRandom = this.conf.getInt(RATIO_FOR_RANDOM_KEY, RATIO_FOR_RANDOM_DEFAULT);
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void map(AvroKey<GenericRecord> record, NullWritable nV, Context context) throws IOException, InterruptedException {
            if (random.nextInt(ratioForRandom) == 0) {
                //Utils.copyRecordAndOffset(record.datum(), offset.get(), rwo);
                //avVal.datum(rwo);
                avVal.datum(record.datum());//kopiowanie z klucza do wartosci, a czemu tu nie uzywamy wartosci?, bo nie ma takiej klasy, jest AvroKeyOutputFormat

                context.write(NullWritable.get(), avVal);
            }
        }
    }

    public static class ComputeBoundsForSortingReducer extends Reducer<NullWritable, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

        private Schema mainObjectSchema;
        private Comparator<GenericRecord> cmp;
        private int noOfSplitPoints;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            super.setup(ctx);
            noOfSplitPoints = Utils.getStripsCount(ctx.getConfiguration()) - 1;
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(ctx.getConfiguration());
        }
        
        @Override
        protected void reduce(NullWritable nV, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            ArrayList<GenericRecord> l = new ArrayList<>();
            //TODO tu sie moze dac posortowac przez secondary sort
            for (AvroValue<GenericRecord> t : values) {
                l.add(SpecificData.get().deepCopy(mainObjectSchema, t.datum()));//ten iterable zwraca za każdym razem ten sam obiekt tylko z podmienionymi wartościami; co więcej zanim się wszystkiego nie obejrzy nie wiadomo ile tego bedzie
            }

            java.util.Collections.sort(l, cmp);
            int step = l.size() / (noOfSplitPoints+1);

            AvroKey<GenericRecord> avKey = new AvroKey<>(null);
            for (int i = 1; i <= noOfSplitPoints; i++) {
                avKey.datum(l.get(i * step));
                context.write(avKey, NullWritable.get());
            }
        }
    }

    public static int runSampling(Path input, Path output, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        SortAvroRecord.LOG.info("starting phase 1 sampling");
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);

        Job job = Job.getInstance(conf, "JOB: Phase one sampling");
        job.setJarByClass(PhaseSampling.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        
        job.setMapperClass(SamplerMapper.class);
        job.setReducerClass(ComputeBoundsForSortingReducer.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(AvroValue.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);        
        job.setOutputKeyClass(AvroKey.class);
        //job.setOutputValueClass(NullWritable.class);

        AvroJob.setInputKeySchema(job, mainObjectSchema);
        AvroJob.setMapOutputValueSchema(job, mainObjectSchema);
        AvroJob.setOutputKeySchema(job, mainObjectSchema);//Schema.create(Schema.Type.STRING));

        SortAvroRecord.LOG.info("waiting for phase 1 sampling");
        int ret = (job.waitForCompletion(true) ? 0 : 1);
        
        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        SortAvroRecord.LOG.info("finished phase 1 sampling, processed "+ total + " key/value pairs");

        return ret;
    }

}
