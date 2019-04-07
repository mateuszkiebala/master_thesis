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
import sortavro.record.MultipleRecords4Float;
import sortavro.record.Record4Float;
import sortavro.record.RankedRecords4Float;
import sortavro.record.MultiRankedRecords4Float;

/**
 *
 * @author mateuszkiebala
 */
public class PhaseRanking {
/*
    static final Log LOG = LogFactory.getLog(PhaseRanking.class);
    public static final String PARTITION_SIZES_FILE = "partition_sizes.avro";
    public static final String PARTITION_SIZES_CACHE = "partition_sizes.cache";

    private static void mergePartitionSizes(Path input, Configuration conf) {
        try {
            List<String> countFiles = new ArrayList<>();
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] statusList = hdfs.listStatus(input);
            if (statusList != null) {
                for (FileStatus fileStatus : statusList) {
                    String filename = fileStatus.getPath().getName();
                    Pattern regex = Pattern.compile(PhaseSortingReducer.COUNTS_TAG + "-r-.*\\.avro");
                    Matcher matcher = regex.matcher(filename);

                    if (matcher.find()) {
                        countFiles.add(input.toString() + "/" + filename);
                    }
                }
                countFiles.add(input.toString() + "/" + PARTITION_SIZES_FILE);
                new ConcatTool().run(System.in, System.out, System.err, countFiles);
            }
        } catch (Exception e) {
            System.err.println("Cannot merge partition sizes: " + e.toString());
        }
    }

    private static Integer[] readPartitionSizes(Configuration conf) {
        int stripsCount = Utils.getStripsCount(conf);
        Integer[] records = new Integer[stripsCount];
        String fileName = PARTITION_SIZES_CACHE;
        File f = new File(fileName);

        GenericRecord datumKeyValuePair = null;
        Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));
        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(f, new GenericDatumReader<>(keyValueSchema))) {
            while (fileReader.hasNext()) {
                datumKeyValuePair = (GenericRecord) fileReader.next(datumKeyValuePair);
                records[(int) datumKeyValuePair.get(0)] = (int) datumKeyValuePair.get(1);
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileName, ie);
        }

        return records;
    }

    public static class RankingReducer extends Reducer<AvroKey<Integer>, AvroValue<MultiRecords4Float>, AvroKey<Integer>, AvroValue<MultiRanked>> {

        private Integer[] prefixedPartitionSizes;
        private Configuration conf;
        private final AvroKey<Integer> avKey = new AvroKey<>();
        private final AvroValue<MultiRanked> avVal = new AvroValue<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            prefixedPartitionSizes = readPartitionSizes(conf);
            for (int i = 1; i < prefixedPartitionSizes.length; i++) {
                prefixedPartitionSizes[i] = prefixedPartitionSizes[i - 1] + prefixedPartitionSizes[i];
            }
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<MultiRecords4Float>> values, Context context) throws IOException, InterruptedException {
            int partitionIndex = key.datum();
            ArrayList<Ranked> result = new ArrayList<>();
            for (AvroValue<MultiRecords4Float> o : values) {
                MultiRecords4Float multiRecords = SpecificData.get().deepCopy(MultiRecords4Float.getClassSchema(), o.datum());
                int i = 0;
                for (Record4Float record : multiRecords.getArrayOfRecords()) {
                    int rank = partitionIndex == 0 ? i : prefixedPartitionSizes[partitionIndex-1] + i;
                    result.add(new Ranked(rank, record));
                    i++;
                }
            }

            avKey.datum(key.datum());
            avVal.datum(new MultiRanked(result));
            context.write(avKey, avVal);
        }
    }

    public static int run(Path input, Path output, Configuration conf) throws Exception {
        LOG.info("starting ranking");
        mergePartitionSizes(input, conf);
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        Schema mutliRecord4FloatSchema = new MultiRecords4Float(mainObjectSchema).getSchema();

        Job job = Job.getInstance(conf, "JOB: Phase ranking");
        URI partitionCountsCache = new URI(input + "/" + PARTITION_SIZES_FILE + "#" + PARTITION_SIZES_CACHE);
        job.setJarByClass(PhaseRanking.class);
        job.addCacheFile(partitionCountsCache);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, mutliRecord4FloatSchema);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, mutliRecord4FloatSchema);

        job.setReducerClass(RankingReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultiRanked.getClassSchema());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }*/
}
