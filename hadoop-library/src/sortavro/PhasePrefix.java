package sortavro;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.tool.ConcatTool;
import org.apache.avro.file.DataFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sortavro.avro_types.ranking.*;
import sortavro.avro_types.terasort.*;

/**
 *
 * @author mateuszkiebala
 */
public class PhasePrefix {

    static final Log LOG = LogFactory.getLog(PhasePrefix.class);
    public static final String PARTITION_SIZES_FILE = "partition_sizes.avro";
    public static final String PARTITION_SIZES_CACHE = "partition_sizes.cache";

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        Schema statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
        StatisticsRecordSchemaCreator.setSchema(statisticerSchema, mainObjectSchema);
        MultipleMainObjectsSchemaCreator.setMainObjectSchema(mainObjectSchema);
    }

    public static class PartitionPrefixMapper extends Mapper<AvroKey<MultipleMainObjects>, NullWritable, AvroKey<MultipleMainObjects>, NullWritable> {

        private Configuration conf;
        private Schema mainObjectSchema;
        private Schema statisticerSchema;
        private final AvroValue<GenericRecord> avVal = new AvroValue<>();
        private final AvroKey<Integer> avKey = new AvroKey<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
            statisticerSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICER_SCHEMA);
        }

        @Override
        protected void map(AvroKey<MultipleMainObjects> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Statisticer result = SpecificData.getClass(statisticerSchema).newInstance();
            for (GenericRecord record : key.getRecords()) {
                Method getStatisticer = SpecificData.getClass(statisticerSchema).getMethod("get", SpecificData.getClass(mainObjectSchema));
                Statisticer statisticer = getStatisticer.invoke(null, (Object) record);
                result.merge(statisticer);
            }
            // write to avro file but it must be distributed filesystem not local -> and then we need to merge them, maybe generic method?
            context.write(avKey, avVal);
        }
    }

    public static class PrefixReducer extends Reducer<AvroKey<MultipleMainObjects>, NullWritable, AvroKey<StatisticsRecord>, NullWritable> {

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
            System.out.println(MultipleMainObjectsSchemaForwarder.getSchema());
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

    public static int run(Path input, Path output, Configuration conf) throws Exception {
        LOG.info("starting prefix");
        mergePartitionSizes(input, conf);
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase prefix");
        job.setJarByClass(PhasePrefix.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(PartitionPrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, MultipleMainObjects.getClassSchema());
        AvroJob.setInputValueSchema(job, NullWritable.class);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, MultipleMainObjects.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, NullWritable.class);

        job.setReducerClass(PrefixReducer.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        AvroJob.setOutputKeySchema(job, StatisticsRecord.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
