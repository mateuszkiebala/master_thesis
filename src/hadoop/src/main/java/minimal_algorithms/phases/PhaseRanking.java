package minimal_algorithms.phases;

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
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.ranking.*;
import minimal_algorithms.utils.*;
import minimal_algorithms.sending.AvroSender;
import minimal_algorithms.config.BaseConfig;

public class PhaseRanking {

    static final Log LOG = LogFactory.getLog(PhaseRanking.class);
    public static final String PARTITION_SIZES_FILE = "partition_sizes.avro";
    public static final String PARTITION_SIZES_CACHE = "partition_sizes.cache";

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, BaseConfig.BASE_SCHEMA_KEY);
        MultipleRecords.setSchema(baseSchema);
        RankRecord.setSchema(baseSchema);
        MultipleRankRecords.setSchema(RankRecord.getClassSchema());
    }

    public static class RankingReducer extends Reducer<AvroKey<Integer>, AvroValue<MultipleRecords>, AvroKey<Integer>, AvroValue<MultipleRankRecords>> {

        private Long[] prefixedPartitionSizes;
        private Configuration conf;
        private AvroSender sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
            prefixedPartitionSizes = Utils.readAvroSortingCountsFromCache(conf, PARTITION_SIZES_CACHE);
            for (int i = 1; i < prefixedPartitionSizes.length; i++) {
                prefixedPartitionSizes[i] = prefixedPartitionSizes[i - 1] + prefixedPartitionSizes[i];
            }
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<MultipleRecords>> values, Context context) throws IOException, InterruptedException {
            int partitionIndex = key.datum();
            ArrayList<RankRecord> result = new ArrayList<>();
            for (AvroValue<MultipleRecords> o : values) {
                MultipleRecords baseRecords = MultipleRecords.deepCopy(o.datum());
                int i = 0;
                for (GenericRecord record : baseRecords.getRecords()) {
                    long rank = partitionIndex == 0 ? i : prefixedPartitionSizes[partitionIndex-1] + i;
                    result.add(new RankRecord(rank, record));
                    i++;
                }
            }
            sender.send(key, new MultipleRankRecords(result));
        }
    }

    public static int run(Path input, Path output, BaseConfig config) throws Exception {
        LOG.info("Starting phase Ranking");
        Configuration conf = config.getConf();
        Utils.mergeHDFSAvro(conf, input, BaseConfig.SORTED_COUNTS_TAG + "-r-.*\\.avro", PARTITION_SIZES_FILE);
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase Ranking");
        job.setJarByClass(PhaseRanking.class);
        job.addCacheFile(new URI(input + "/" + PARTITION_SIZES_FILE + "#" + PARTITION_SIZES_CACHE));
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));

        FileInputFormat.setInputPaths(job, input + "/" + BaseConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, MultipleRecords.getClassSchema());

        job.setReducerClass(RankingReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleRankRecords.getClassSchema());

        LOG.info("Waiting for phase Ranking");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished phase Ranking, processed " + total + " key/value pairs");

        return ret;
    }
}
