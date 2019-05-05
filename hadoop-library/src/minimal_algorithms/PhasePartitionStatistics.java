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

public class PhasePartitionStatistics {

    static final Log LOG = LogFactory.getLog(PhasePartitionStatistics.class);
    static final String PARTITION_STATISTICS_CACHE = "partition_statistics.cache";

    private static void setSchemas(Configuration conf) {
        Schema mainObjectSchema = Utils.retrieveMainObjectSchemaFromConf(conf);
        MultipleMainObjects.setSchema(mainObjectSchema);
    }

    public static class PartitionPrefixMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleMainObjects>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private Configuration conf;
        private Schema statisticsAggregatorSchema;
        private final AvroValue<StatisticsAggregator> avVal = new AvroValue<>();

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleMainObjects> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator statsMerger = null;
            try {
                Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
                for (GenericRecord record : value.datum().getRecords()) {
                    StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                    statisticsAggregator.create(record);
                    if (statsMerger == null) {
                        statsMerger = statisticsAggregator;
                    } else {
                        statsMerger = statsMerger.merge(statisticsAggregator);
                    }
                }
            } catch (Exception e) {
                System.err.println("Cannot create partition statistics: " + e.toString());
            }

            avVal.datum(statsMerger);
            context.write(key, avVal);
        }
    }

    public static class PartitionStatisticsReducer extends Reducer<AvroKey<Integer>, AvroValue<StatisticsAggregator>, AvroKey<Integer>, AvroValue<StatisticsAggregator>> {

        private final AvroValue<StatisticsAggregator> avVal = new AvroValue<>();

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<StatisticsAggregator>> values, Context context) throws IOException, InterruptedException {
            int size = 0;
            for (AvroValue<StatisticsAggregator> av : values) {
                avVal.datum(av.datum());
                size++;
            }

            if (size != 1) {
                throw new InterruptedException("Too many AvroValues<StatisticsAggregator> size = " + size);
            }

            context.write(key, avVal);
        }
    }

    public static int run(Path input, Path output, Configuration conf) throws Exception {
        LOG.info("starting partition statistics");
        setSchemas(conf);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SortAvroRecord.STATISTICS_AGGREGATOR_SCHEMA);

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
