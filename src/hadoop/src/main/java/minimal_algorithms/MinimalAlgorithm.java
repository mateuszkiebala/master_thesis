package minimal_algorithms.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import minimal_algorithms.hadoop.config.*;
import minimal_algorithms.hadoop.utils.*;
import minimal_algorithms.hadoop.phases.*;

public class MinimalAlgorithm {

    static final Log LOG = LogFactory.getLog(MinimalAlgorithm.class);

    private Configuration conf;
    private Config config;
    private int valuesNo;
    private int stripsNo;
    private int reduceTasksNo;

    private final String SAMPLING_DIR = "/sampling_output";
    private final String SORTING_DIR = "/sorting_output";
    private final String RANKING_DIR = "/ranking_output";
    private final String PERFECT_SORT_WITH_RANKS_DIR = "/perfect_sort_with_ranks_output";

    public MinimalAlgorithm(Configuration conf, int valuesNo, int stripsNo, int reduceTasksNo) {
        this.valuesNo = valuesNo;
        this.stripsNo = stripsNo;
        this.reduceTasksNo = reduceTasksNo;
        this.conf = conf;
        config = new Config(this.conf, valuesNo, stripsNo, reduceTasksNo);
        this.conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    }

    public int teraSort(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        return teraSort(homeDir, input, output, baseConfig);
    }

    public int teraSort(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        validateArgs();
        Path samplingSuperdir = new Path(homeDir + "/tmp" + SAMPLING_DIR);
        Utils.deleteDirFromHDFS(conf, samplingSuperdir, true);
        //-------------------------------SAMPLING-------------------------------
        //input: avro file with RecordWithCount4
        //       containing whole input
        //mapper: (AK<RecordWithCount4>, NW) -> (NW, AV<RecordWithCount4>)
        //        chooses each record with 1/PhaseSampling.RATIO_FOR_RANDOM_KEY probability
        //reducer: (NW, AV<RecordWithCount4>*) -> (AvroKey<RecordWithCount4>, NW)
        //         computes noOfSplitPoints=PhaseSampling.NO_OF_MACHINES_KEY-1 split points
        //         so that they divide the sample in such a way: ..., sp1, ..., sp2, ..., sp_noOfSplitPoints, ...
        //output: avro file with RecordWithCount4
        //        containing split points
        int ret = PhaseSampling.run(input, samplingSuperdir, baseConfig);
        ret = ret == 0 ? PhaseSortingReducer.run(input, samplingSuperdir, output, baseConfig) : ret;
        //-------------------------------SORTING--------------------------------
        //input: avro file with RecordWithCount4
        //       containing whole input
        //       and the path to the output of PhaseSampling which is cached in the cluster in the location after #
        //mapper: (AK<RecordWithCount4>, NW) -> (AK<Integer>, AV<RecordWithCount4>)
        //        send each record to reducer numbered from 0 to PhaseSampling.NO_OF_MACHINES_KEY
        //        the shares are more the less equal
        //reducer: (AK<Integer>, AV<RecordWithCount4>*) -> (AK<Integer>, AV<MultipleRecordsWithCoun4>)
        //         sorts the values it got, produces multiple outputs
        //output:  avro files with pairs in AvroKeyValueOutputFormat
        //         PhaseSortingReducer.COUNTS_TAG - count of values in this group
        //         PhaseSortingReducer.DATA_TAG - all the values in MultipleRecordsWithCoun4 object with a list inside
        Utils.deleteDirFromHDFS(conf, samplingSuperdir, true);
        return ret;
    }

    public int ranking(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        return ranking(homeDir, input, output, baseConfig);
    }

    public int ranking(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        int ret = teraSort(homeDir, input, sortingDir, baseConfig);
        ret = ret == 0 ? PhaseRanking.run(sortingDir, output, baseConfig) : ret;
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
        return ret;
    }

    public int perfectSort(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        return perfectSort(homeDir, input, output, baseConfig);
    }

    public int perfectSort(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path rankingDir = new Path(homeDir + "/tmp" + RANKING_DIR);
        int ret = ranking(homeDir, input, rankingDir, baseConfig);
        ret = ret == 0 ? PhasePerfectSort.run(rankingDir, output, baseConfig) : ret;
        Utils.deleteDirFromHDFS(conf, rankingDir, true);
        return ret;
    }

    public int perfectSortWithRanks(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        return perfectSortWithRanks(homeDir, input, output, baseConfig);
    }

    public int perfectSortWithRanks(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path rankingDir = new Path(homeDir + "/tmp" + RANKING_DIR);
        int ret = ranking(homeDir, input, rankingDir, baseConfig);
        ret = ret == 0 ? PhasePerfectSortWithRanks.run(rankingDir, output, baseConfig) : ret;
        Utils.deleteDirFromHDFS(conf, rankingDir, true);
        return ret;
    }

    public int partitionStatistics(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) throws Exception {
        StatisticsConfig statisticsConfig = new StatisticsConfig(config, cmp, baseSchema, statisticsAggregatorSchema);
        return partitionStatistics(homeDir, input, output, statisticsConfig);
    }

    public int partitionStatistics(Path homeDir, Path input, Path output, StatisticsConfig statisticsConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        int ret = teraSort(homeDir, input, sortingDir, statisticsConfig);
        ret = ret == 0 ? PhasePartitionStatistics.run(sortingDir, output, statisticsConfig) : ret;
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
        return ret;
    }

    public int prefix(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) throws Exception {
        StatisticsConfig statisticsConfig = new StatisticsConfig(config, cmp, baseSchema, statisticsAggregatorSchema);
        return prefix(homeDir, input, output, statisticsConfig);
    }

    public int prefix(Path homeDir, Path input, Path output, StatisticsConfig statisticsConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        int ret = teraSort(homeDir, input, sortingDir, statisticsConfig);
        ret = ret == 0 ? PhasePrefix.run(sortingDir, output, statisticsConfig) : ret;
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
        return ret;
    }

    public int group(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema, Schema keyRecordSchema) throws Exception {
        GroupByConfig groupByConfig = new GroupByConfig(config, cmp, baseSchema, statisticsAggregatorSchema, keyRecordSchema);
        return group(homeDir, input, output, groupByConfig);
    }

    public int group(Path homeDir, Path input, Path output, GroupByConfig groupByConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        int ret = teraSort(homeDir, input, sortingDir, groupByConfig);
        ret = ret == 0 ? PhaseGroupBy.run(sortingDir, output, groupByConfig) : ret;
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
        return ret;
    }

    public int semiJoin(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema keyRecordSchema) throws Exception {
        SemiJoinConfig semiJoinConfig = new SemiJoinConfig(config, cmp, baseSchema, keyRecordSchema);
        return semiJoin(homeDir, input, output, semiJoinConfig);
    }

    public int semiJoin(Path homeDir, Path input, Path output, SemiJoinConfig semiJoinConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        int ret = teraSort(homeDir, input, sortingDir, semiJoinConfig);
        ret = ret == 0 ? PhaseSemiJoin.run(sortingDir, output, semiJoinConfig) : ret;
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
        return ret;
    }

    public int slidingAggregation(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema,
                                  Schema statisticsAggregatorSchema, long windowLength) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        Path perfectSortWithRanksDir = new Path(homeDir + "/tmp" + PERFECT_SORT_WITH_RANKS_DIR);
        int ret = perfectSortWithRanks(homeDir, input, perfectSortWithRanksDir, baseConfig);
        SlidingAggregationConfig saConfig = new SlidingAggregationConfig(config, cmp, baseSchema, statisticsAggregatorSchema, windowLength);
        ret = ret == 0 ? PhaseSlidingAggregation.run(perfectSortWithRanksDir, output, saConfig) : ret;
        Utils.deleteDirFromHDFS(conf, perfectSortWithRanksDir, true);
        return ret;
    }

    public Configuration getConf() {
        return conf;
    }

    public Config getConfig() {
        return config;
    }

    private void validateArgs() {
        if (!Config.validateValuesPerStripNo(valuesNo, stripsNo)) {
            System.err.println("Too many values for one strip. Increase number of strips to avoid int overflow.");
            System.exit(-1);
        }
    }
}
