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
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.group_by.*;
import minimal_algorithms.config.StatisticsConfig;
import minimal_algorithms.config.GroupByConfig;
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.config.Config;
import org.apache.avro.generic.GenericRecord;


public class MinimalAlgorithm {

    static final Log LOG = LogFactory.getLog(MinimalAlgorithm.class);

    private Configuration conf;
    private Config config;
    private int valuesNo;
    private int stripsNo;
    private int reduceTasksNo;

    public MinimalAlgorithm(Configuration conf, int valuesNo, int stripsNo, int reduceTasksNo) {
        this.valuesNo = valuesNo;
        this.stripsNo = stripsNo;
        this.reduceTasksNo = reduceTasksNo;
        this.conf = conf;
        config = new Config(this.conf, valuesNo, stripsNo, reduceTasksNo);
        this.conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    }

    public void teraSort(Path homeDir, Path input, Path output, Comparator cmp, Schema baseSchema) throws Exception {
        validateArgs();
        BaseConfig baseConfig = new BaseConfig(config, cmp, baseSchema);
        Path samplingSuperdir = new Path(homeDir + "/tmp/sampling_output");
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

    private void validateArgs() {
        if (!Config.validateValuesPerStripNo(valuesNo, stripsNo)) {
            System.err.println("Too many values for one strip. Increase number of strips to avoid int overflow.");
            System.exit(-1);
        }
    }
}
