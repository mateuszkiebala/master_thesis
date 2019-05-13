package minimal_algorithms;

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
import minimal_algorithms.config.*;
import minimal_algorithms.utils.*;
import minimal_algorithms.phases.*;

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
    private final String PARTITION_STATISTICS_DIR = "/partition_statistics_output";
    private final String PREFIX_DIR = "/prefix_output";
    private final String GROUP_BY_DIR = "/group_by_output";

    public MinimalAlgorithm(Configuration conf, int valuesNo, int stripsNo, int reduceTasksNo) {
        this.valuesNo = valuesNo;
        this.stripsNo = stripsNo;
        this.reduceTasksNo = reduceTasksNo;
        this.conf = conf;
        config = new Config(this.conf, valuesNo, stripsNo, reduceTasksNo);
        this.conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    }

    public void teraSort(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        teraSort(homeDir, input, output, baseConfig);
    }

    public void teraSort(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
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
        PhaseSampling.run(input, samplingSuperdir, baseConfig);
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
        PhaseSortingReducer.run(input, samplingSuperdir, output, baseConfig);
        Utils.deleteDirFromHDFS(conf, samplingSuperdir, true);
    }

    public void ranking(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        ranking(homeDir, input, output, baseConfig);
    }

    public void ranking(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        teraSort(homeDir, input, sortingDir, baseConfig);
        PhaseRanking.run(sortingDir, output, baseConfig);
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
    }

    public void perfectSort(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        perfectSort(homeDir, input, output, baseConfig);
    }

    public void perfectSort(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path rankingDir = new Path(homeDir + "/tmp" + RANKING_DIR);
        ranking(homeDir, input, rankingDir, baseConfig);
        PhasePerfectSort.run(rankingDir, output, baseConfig);
        Utils.deleteDirFromHDFS(conf, rankingDir, true);
    }

    public void perfectSortWithRanks(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        perfectSortWithRanks(homeDir, input, output, baseConfig);
    }

    public void perfectSortWithRanks(Path homeDir, Path input, Path output, BaseConfig baseConfig) throws Exception {
        Path rankingDir = new Path(homeDir + "/tmp" + RANKING_DIR);
        ranking(homeDir, input, rankingDir, baseConfig);
        PhasePerfectSortWithRanks.run(rankingDir, output, baseConfig);
        Utils.deleteDirFromHDFS(conf, rankingDir, true);
    }

    public void partitionStatistics(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) throws Exception {
        StatisticsConfig statisticsConfig = new StatisticsConfig(config, cmp, baseSchema, statisticsAggregatorSchema);
        partitionStatistics(homeDir, input, output, statisticsConfig);
    }

    public void partitionStatistics(Path homeDir, Path input, Path output, StatisticsConfig statisticsConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        teraSort(homeDir, input, sortingDir, statisticsConfig);
        PhasePartitionStatistics.run(sortingDir, output, statisticsConfig);
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
    }

    public void prefix(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) throws Exception {
        StatisticsConfig statisticsConfig = new StatisticsConfig(config, cmp, baseSchema, statisticsAggregatorSchema);
        prefix(homeDir, input, output, statisticsConfig);
    }

    public void prefix(Path homeDir, Path input, Path output, StatisticsConfig statisticsConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        teraSort(homeDir, input, sortingDir, statisticsConfig);
        PhasePrefix.run(sortingDir, output, statisticsConfig);
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
    }

    public void group(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema, Schema keyRecordSchema) throws Exception {
        GroupByConfig groupByConfig = new GroupByConfig(config, cmp, baseSchema, statisticsAggregatorSchema, keyRecordSchema);
        group(homeDir, input, output, groupByConfig);
    }

    public void group(Path homeDir, Path input, Path output, GroupByConfig groupByConfig) throws Exception {
        Path sortingDir = new Path(homeDir + "/tmp" + SORTING_DIR);
        teraSort(homeDir, input, sortingDir, groupByConfig);
        PhaseGroupBy.run(sortingDir, output, groupByConfig);
        Utils.deleteDirFromHDFS(conf, sortingDir, true);
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
