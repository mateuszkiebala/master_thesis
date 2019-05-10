package minimal_algorithms;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.terasort.*;
import minimal_algorithms.sending.Sender;
import minimal_algorithms.statistics.StatisticsUtils;
import minimal_algorithms.config.StatisticsConfig;

public class PhasePartitionStatistics {

    static final Log LOG = LogFactory.getLog(PhasePartitionStatistics.class);
    static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveSchemaFromConf(conf, StatisticsConfig.BASE_SCHEMA);
        MultipleMainObjects.setSchema(mainObjectSchema);
    }

    public static class PartitionPrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private Configuration conf;
        private StatisticsUtils statsUtiler;
        private Sender<StatisticsAggregator> sender;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, StatisticsConfig.STATISTICS_AGGREGATOR_SCHEMA));
            sender = new Sender(ctx);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            sender.sendToMachine(statsUtiler.foldLeftRecords(value.datum().getRecords()), key);
        }
    }

    public static class PartitionStatisticsReducer extends Reducer<AvroKey<Integer>, AvroValue<StatisticsAggregator>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private Sender<StatisticsAggregator> sender;

        @Override
        public void setup(Context ctx) {
            sender = new Sender(ctx);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<StatisticsAggregator>> values, Context context) throws IOException, InterruptedException {
            for (AvroValue<StatisticsAggregator> av : values) {
                sender.sendToMachine(av.datum(), key);
                break;
            }
        }
    }

    public static int run(Path input, Path output, StatisticsConfig statsConfig) throws Exception {
        LOG.info("starting partition statistics");
        Configuration conf = statsConfig.getConf();
        setSchemas(conf);
        Schema statisticsAggregatorSchema = statsConfig.getStatisticsAggregatorSchema();

        Job job = Job.getInstance(conf, "JOB: Phase partition statistics");
        job.setJarByClass(PhasePartitionStatistics.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(PartitionPrefixMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + PhaseSortingReducer.DATA_GLOB);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleMainObjects.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, statisticsAggregatorSchema);

        job.setReducerClass(PartitionStatisticsReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, statisticsAggregatorSchema);

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        return ret;
    }
}
