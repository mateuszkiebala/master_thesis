package minimal_algorithms.phases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import javafx.util.Pair;
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
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import minimal_algorithms.statistics.*;
import minimal_algorithms.utils.*;
import minimal_algorithms.sending.*;
import minimal_algorithms.ranking.*;
import minimal_algorithms.RangeTree;
import minimal_algorithms.config.SlidingAggregationConfig;

public class PhaseSlidingAggregation {

    static final Log LOG = LogFactory.getLog(PhaseSlidingAggregation.class);

    private static void setSchemas(Configuration conf) {
        Schema baseSchema = Utils.retrieveSchemaFromConf(conf, SlidingAggregationConfig.BASE_SCHEMA_KEY);
        Schema statisticsAggregatorSchema = Utils.retrieveSchemaFromConf(conf, SlidingAggregationConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY);
        RankRecord.setSchema(baseSchema);
        IndexedStatisticsRecord.setSchema(statisticsAggregatorSchema);
        MultipleRankRecords.setSchema(RankRecord.getClassSchema());
        SendWrapper.setSchema(RankRecord.getClassSchema(), IndexedStatisticsRecord.getClassSchema());
        StatisticsRecord.setSchema(statisticsAggregatorSchema, baseSchema);
        MultipleStatisticRecords.setSchema(StatisticsRecord.getClassSchema());
    }

    public static class RemotelyRelevantMapper extends Mapper<AvroKey<Integer>, AvroValue<MultipleRankRecords>, AvroKey<Integer>, AvroValue<SendWrapper>> {

        private Configuration conf;
        private AvroSender sender;
        private StatisticsUtils statsUtiler;
        private long itemsNoByMachine;
        private int machinesNo;
        private long windowLength;

        @Override
        public void setup(Context ctx) {
            conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, SlidingAggregationConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY));
            itemsNoByMachine = Utils.getItemsNoByMachines(conf);
            machinesNo = Utils.getMachinesNo(conf);
            windowLength = conf.getLong(SlidingAggregationConfig.WINDOW_LENGTH_KEY, 0);
        }

        @Override
        protected void map(AvroKey<Integer> key, AvroValue<MultipleRankRecords> value, Context context) throws IOException, InterruptedException {
            StatisticsAggregator partitionStatistics = statsUtiler.foldLeftRecords(value.datum().getBaseRecords(), null);
            sender.sendToAllMachines(new SendWrapper(null, new IndexedStatisticsRecord(new Long(key.datum()), partitionStatistics)));
            distributeDataToRemotelyRelevantPartitions(key.datum(), value.datum().getRecords());
        }

        private void distributeDataToRemotelyRelevantPartitions(int machineIndex, List<RankRecord> records) throws IOException, InterruptedException {
            for (RankRecord record : records) {
                SendWrapper sw = new SendWrapper(record, null);
                if (windowLength <= itemsNoByMachine) {
                    sender.sendBounded(machineIndex + 1, sw);
                } else {
                    int remRelMachine = (int) ((windowLength - 1) / itemsNoByMachine);
                    sender.sendBounded(machineIndex + remRelMachine, sw);
                    sender.sendBounded(machineIndex + remRelMachine + 1, sw);
                }
                sender.sendBounded(machineIndex, sw);
            }
        }
    }

    public static class WindowReducer extends Reducer<AvroKey<Integer>, AvroValue<SendWrapper>, AvroKey<Integer>, AvroValue<MultipleStatisticRecords>> {

        private AvroSender sender;
        private Configuration conf;
        private StatisticsUtils statsUtiler;
        private long itemsNoByMachine;
        private long windowLength;

        @Override
        public void setup(Context ctx) {
            this.conf = ctx.getConfiguration();
            setSchemas(conf);
            sender = new AvroSender(ctx);
            statsUtiler = new StatisticsUtils(Utils.retrieveSchemaFromConf(conf, SlidingAggregationConfig.STATISTICS_AGGREGATOR_SCHEMA_KEY));
            itemsNoByMachine = Utils.getItemsNoByMachines(conf);
            windowLength = conf.getLong(SlidingAggregationConfig.WINDOW_LENGTH_KEY, 0);
        }

        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<SendWrapper>> values, Context context) throws IOException, InterruptedException {
            Map<Integer, List<GenericRecord>> groupedRecords = SendingUtils.partitionRecords(values);
            RangeTree partitionsTree = getPartitionsTree(groupedRecords.get(2));
            computeWindowValues(key.datum(), groupedRecords.get(1), partitionsTree);
        }

        private RangeTree getPartitionsTree(List<GenericRecord> partitionStatisticsRecords) {
            RangeTree tree = new RangeTree(partitionStatisticsRecords.size());
            for (GenericRecord record : partitionStatisticsRecords) {
                IndexedStatisticsRecord isr = (IndexedStatisticsRecord) record;
                int machineIndex = isr.getIndex().intValue();
                StatisticsAggregator baseStatistics = statsUtiler.createStatisticsAggregator(isr.getRecord());
                tree.insert(baseStatistics, machineIndex);
            }
            return tree;
        }

        private void computeWindowValues(int machineIndex, List<GenericRecord> records, RangeTree partitionsTree) throws IOException, InterruptedException {
            java.util.Collections.sort(records, RankRecord.genericCmp);
            HashMap<Long, Integer> rankToIndex = new HashMap<>();
            RangeTree tree = new RangeTree(records.size());
            for (int i = 0; i < records.size(); i++) {
                RankRecord rankRecord = (RankRecord) records.get(i);
                rankToIndex.put(rankRecord.getRank(), i);
                tree.insert(statsUtiler.createStatisticsAggregator(rankRecord.getRecord()), i);
            }

            List<StatisticsRecord> slidingResult = new ArrayList<>();
            long baseLowerBound = machineIndex * itemsNoByMachine;
            long baseUpperBound = (machineIndex+1) * itemsNoByMachine - 1;
            for (int i = 0; i < records.size(); i++) {
                RankRecord rankRecord = (RankRecord) records.get(i);
                long rank = rankRecord.getRank();
                if (rank >= baseLowerBound && rank <= baseUpperBound) {
                    long windowStart = (rank - windowLength + 1) < 0 ? 0 : rank - windowLength + 1;
                    int a = (int) (((rank + 1 - windowLength + itemsNoByMachine) / itemsNoByMachine) - 1);
                    int alpha = a >= 0 ? a : -1;
                    StatisticsAggregator result;
                    if (machineIndex > alpha + 1) {
                        StatisticsAggregator w1 = alpha < 0 ? null : tree.query(rankToIndex.get(windowStart), rankToIndex.get((alpha + 1) * itemsNoByMachine - 1));
                        StatisticsAggregator w2 = partitionsTree.query(alpha + 1, machineIndex - 1);
                        StatisticsAggregator w3 = tree.query(rankToIndex.get(baseLowerBound), rankToIndex.get(rank));
                        result = StatisticsAggregator.safeMerge(StatisticsAggregator.safeMerge(w1, w2), w3);
                    } else {
                        result = tree.query(rankToIndex.get(windowStart), rankToIndex.get(rank));
                    }
                    slidingResult.add(new StatisticsRecord(result, rankRecord.getRecord()));
                }
            }
            sender.send(machineIndex, new MultipleStatisticRecords(slidingResult));
        }
    }

    public static int run(Path input, Path output, SlidingAggregationConfig config) throws Exception {
        LOG.info("starting Phase SlidingAggregation");
        Configuration conf = config.getConf();
        setSchemas(conf);

        Job job = Job.getInstance(conf, "JOB: Phase SlidingAggregation");
        job.setJarByClass(PhaseSlidingAggregation.class);
        job.setNumReduceTasks(Utils.getReduceTasksCount(conf));
        job.setMapperClass(RemotelyRelevantMapper.class);

        FileInputFormat.setInputPaths(job, input + "/" + SlidingAggregationConfig.SORTED_DATA_PATTERN);
        FileOutputFormat.setOutputPath(job, output);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputValueSchema(job, MultipleRankRecords.getClassSchema());

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(AvroValue.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, SendWrapper.getClassSchema());

        job.setReducerClass(WindowReducer.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setOutputValueSchema(job, MultipleStatisticRecords.getClassSchema());

        LOG.info("Waiting for Phase SlidingAggregation");
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        LOG.info("Finished Phase SlidingAggregation, processed " + total + " key/value pairs");

        return ret;
    }
}
