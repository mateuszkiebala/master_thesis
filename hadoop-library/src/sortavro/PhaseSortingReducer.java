package sortavro;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.avro.reflect.ReflectData;
import sortavro.avro_types.terasort.*;

/**
 *
 * @author jsroka
 */
public class PhaseSortingReducer {

    public static final String COUNTS_TAG = "sortingCountsTag";
    public static final String DATA_TAG = "sortingDataTag";
    public static final String DATA_GLOB = "sortingDataTag-r-*.avro";
    public static final String COUNTS_MERGED_FILENAME = "sorting_counts_filename.txt";
    public static final String SAMPLING_SPLIT_POINTS_CACHE_FILENAME_ALIAS = "sampling_split_points.cache";

    public static class PartitioningMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<Integer>, AvroValue<GenericRecord>> {

        private GenericRecord[] splitPoints;
        private Configuration conf;
        private Comparator<GenericRecord> cmp;
        private Schema mainObjectSchema;
        private final AvroValue<GenericRecord> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            splitPoints = Utils.readMainObjectRecordsFromLocalFileAvro(conf, PhaseSortingReducer.SAMPLING_SPLIT_POINTS_CACHE_FILENAME_ALIAS);
            cmp = Utils.retrieveComparatorFromConf(ctx.getConfiguration());
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            int dummy = java.util.Arrays.binarySearch(splitPoints, SpecificData.get().deepCopy(mainObjectSchema, key.datum()), cmp);
            avKey.datum((dummy >= 0) ? dummy : -dummy - 1);
            avVal.datum(key.datum());
            context.write(avKey, avVal);
        }
    }

    public static class SortingReducer extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<Integer>, AvroValue<MultipleMainObjects>> {

        private Configuration conf;
        private Comparator<GenericRecord> cmp;
        private AvroMultipleOutputs amos;
        private Schema mainObjectSchema;
        private final AvroValue<Integer> aInt = new AvroValue<>();
        private final AvroValue<MultipleMainObjects> avValueMultRecords = new AvroValue<>(null);
        
        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            amos = new AvroMultipleOutputs(ctx);
            cmp = Utils.retrieveComparatorFromConf(conf);
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
            MultipleMainObjects.setSchema(mainObjectSchema);
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
            ArrayList<GenericRecord> l = new ArrayList<>();
            
            //TODO how to secondary sort in avro?
            for (AvroValue<GenericRecord> t : values) {
                //ten iterable zwraca za każdym razem ten sam obiekt tylko z podmienionymi wartościami; co więcej zanim się wszystkiego nie obejrzy nie wiadomo ile tego bedzie
                l.add(SpecificData.get().deepCopy(mainObjectSchema, t.datum()));
            }
            java.util.Collections.sort(l, cmp);
            
            //write size of the group
            aInt.datum(l.size());
            amos.write(COUNTS_TAG, avKey, aInt);

            //write group of values
            avValueMultRecords.datum(new MultipleMainObjects(l));
            amos.write(DATA_TAG, avKey, avValueMultRecords);            
        }
    }

    public static int runSorting(Path input, Path output, URI partitionsURI, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        SortAvroRecord.LOG.info("starting phase 2 sorting");
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        MultipleMainObjects.setSchema(mainObjectSchema);

        Job job = Job.getInstance(conf, "JOB: Phase two sorting");
        job.setJarByClass(PhaseSortingReducer.class);
        job.addCacheFile(partitionsURI);//now the file is accessible at location SortAvroRecordWithOffset.PARTITIONS_CACHE_FILENAME_ALIAS, since we specified it so after # in URI
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PartitioningMapper.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, mainObjectSchema);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, mainObjectSchema);

        job.setReducerClass(SortingReducer.class);
        //job.setOutputFormatClass(AvroKeyOutputFormat.class);//AvroKeyValueOutputFormat opakowuje klucz wartosc w pare - dziala dla AvroKey/Value oraz podstawowych Writable
        //AvroJob.setOutputKeySchema(job, RecordWithOffset.getClassSchema());//Schema.create(Schema.Type.STRING));
        //job.setOutputValueClass(NullWritable.class);

        AvroMultipleOutputs.addNamedOutput(job, PhaseSortingReducer.DATA_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), MultipleMainObjects.getClassSchema()); // if Schema is specified as null then the default output schema is used
        AvroMultipleOutputs.addNamedOutput(job, PhaseSortingReducer.COUNTS_TAG, AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));

        int ret = (job.waitForCompletion(true) ? 0 : 1);

        return ret;
    }

}
