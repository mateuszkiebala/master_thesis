package minimal_algorithms;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import minimal_algorithms.record.RWC4Cmps;
import minimal_algorithms.record.Record4Float;
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.group_by.*;
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.config.StatisticsConfig;
import minimal_algorithms.config.GroupByConfig;

/**
 *
 * @author jsroka
 */
public class SortAvroRecord extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(SortAvroRecord.class);

    public static final String SAMPLING_SUPERDIR = "/1_sampling_output";
    public static final String SORTING_SUPERDIR = "/2_sorting_output";
    public static final String RANKING_SUPERDIR = "/3_ranking_output";
    public static final String PARTITION_STATISTICS_SUPERDIR = "/4_partition_statistics_output";
    public static final String PREFIX_SUPERDIR = "/5_prefix_output";
    public static final String GROUP_BY_SUPERDIR = "/6_group_by_output";

    public static final String IS_LAST_DIMENSION_KEY = "is.last.dimension";
    public static final int NO_OF_DIMENSIONS = 4;

    private Path getSamplingSuperdir(String commonPrefix, String dimPrefix) {
        return new Path(commonPrefix + dimPrefix + SAMPLING_SUPERDIR);
    }

    private Path getSortingSuperdir(String commonPrefix, String dimPrefix) {
        return new Path(commonPrefix + dimPrefix + SORTING_SUPERDIR);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: SortAvro <input> <intermediate_prefix> <elements> <splits> <reduce_tasks>");
            return -1;
        }

        Path input = new Path(args[0]);
        int n = Integer.parseInt(args[2]);//no of inpput data values
        int t = Integer.parseInt(args[3]);//no of strips
        int m = 1 + n / t;//no of data values per strip
        if (m / 20 > Integer.MAX_VALUE) {
            System.err.println("Too many values for one strip. Increase number of strips to avoid int overflow.");
            System.exit(-1);
        }
        double rho = 1. / m * Math.log(((double) n) * t);
        int reverseRho = (int) (1 / rho);
        System.out.println("n="+n);
        System.out.println("t="+t);
        System.out.println("m="+m);
        System.out.println("rho="+rho);
        System.out.println("reverseRho="+reverseRho);
        System.out.println("for smaling keys aprox. = " + (int) (n*rho));

        Configuration conf = getConf();
        conf.setLong(PhaseSampling.NO_OF_VALUES_KEY, n);
        conf.setInt(PhaseSampling.NO_OF_STRIPS_KEY, t);
        conf.setInt(PhaseSampling.RATIO_FOR_RANDOM_KEY, reverseRho);
        conf.setInt(Utils.NO_OF_REDUCE_TASKS_KEY, Integer.parseInt(args[4]));

        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);//potrzebne zeby hadoop bral odpowiednie jary avro

        //set comparators used in this dimmension
        Utils.storeComparatorsInConf(conf, RWC4Cmps.firstCmp, RWC4Cmps.firstCmp, RWC4Cmps.secondCmp, RWC4Cmps.thirdCmp, RWC4Cmps.fourthCmp);
        //paths for files storing lo and hi borders; the last one was computed in this dimension; the missing ones were not yet computed
        Utils.storeInConfLoBoundsFilenamesComputedSoFar(conf);
        Utils.storeInConfHiBoundsFilenamesComputedSoFar(conf);
        Utils.storeSchemaInConf(conf, Record4Float.getClassSchema(), BaseConfig.BASE_SCHEMA);

        BaseConfig baseConfig = new BaseConfig(conf, Record4Float.getClassSchema());
        StatisticsConfig statsConfig = new StatisticsConfig(conf, Record4Float.getClassSchema(), SumStatisticsAggregator.getClassSchema());
        GroupByConfig groupByConfig = new GroupByConfig(conf, Record4Float.getClassSchema(), SumStatisticsAggregator.getClassSchema(), IntKeyRecord4Float.getClassSchema());

        Path samplingSuperdir = new Path(args[1] + SAMPLING_SUPERDIR);
        Path sortingSuperdir = new Path(args[1] + SORTING_SUPERDIR);
        Path rankingSuperdir = new Path(args[1] + RANKING_SUPERDIR);
        Path prefixSuperdir = new Path(args[1] + PREFIX_SUPERDIR);
        Path partitionStatisticsSuperdir = new Path(args[1] + PARTITION_STATISTICS_SUPERDIR);
        Path groupBySuperdir = new Path(args[1] + GROUP_BY_SUPERDIR);

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
        PhaseSampling.runSampling(input, samplingSuperdir, conf);

        //-------------------------------SORTING--------------------------------
        URI samplingBoundsURI = new URI(samplingSuperdir + "/part-r-00000.avro" + "#" + PhaseSortingReducer.SAMPLING_SPLIT_POINTS_CACHE_FILENAME_ALIAS);//po # jest nazwa pod ktora plik zostanie umieszczony w cache
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
        PhaseSortingReducer.runSorting(input, sortingSuperdir, samplingBoundsURI, conf);
        PhaseRanking.run(sortingSuperdir, rankingSuperdir, baseConfig);
        PhasePartitionStatistics.run(sortingSuperdir, partitionStatisticsSuperdir, statsConfig);
        PhasePrefix.run(sortingSuperdir, prefixSuperdir, statsConfig);
        PhaseGroupBy.run(sortingSuperdir, groupBySuperdir, groupByConfig);

        System.out.println("n="+n);
        System.out.println("t="+t);
        System.out.println("m="+m);
        System.out.println("rho="+rho);
        System.out.println("reverseRho="+reverseRho);
        System.out.println("for smaling keys aprox. = " + (int) (n*rho));

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SortAvroRecord(), args);//to configuration jest tu potrzebne zeby odczytac -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        System.exit(res);
    }
}
