package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class Config {

    public static final String BASE_SCHEMA = "base.schema";
    public static final String STATISTICS_AGGREGATOR_SCHEMA = "statisticer.schema";
    public static final String GROUP_BY_KEY_SCHEMA = "group.by.key.schema";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final String NO_OF_VALUES_KEY = "no.of.values";
    public static final String NO_OF_STRIPS_KEY = "no.of.splits";
    public static final String RATIO_FOR_RANDOM_KEY = "ratio.for.random";

    public static final String SORTED_COUNTS_TAG = "sortedCountsTag";
    public static final String SORTED_DATA_TAG = "sortedDataTag";
    public static final String SORTED_DATA_PATTERN = SORTED_DATA_TAG + "-r-*.avro";

    public static final int RATIO_FOR_RANDOM_DEFAULT = -1;
    public static final int NO_OF_KEYS_DEFAULT = -1;
    public static final int NO_OF_VALUES_DEFAULT = -1;
    public static final int NO_OF_REDUCE_TASKS_DEFAULT = 2;

    protected Configuration conf;

    public static boolean validateValuesPerStripNo(int valuesNo, int stripsNo) {
        int valuesPerStripNo = 1 + valuesNo / stripsNo;
        return valuesPerStripNo / 20 > Integer.MAX_VALUE ? false : true;
    }

    public static int computeValuesPerStripNo(int valuesNo, int stripsNo) {
        return 1 + valuesNo / stripsNo;
    }

    public static double computeRHO(int valuesNo, int stripsNo) {
        int valuesPerStripNo = computeValuesPerStripNo(valuesNo, stripsNo);
        return 1. / valuesPerStripNo * Math.log(((double) valuesNo) * stripsNo);
    }

    public static int computeReversedRHO(int valuesNo, int stripsNo) {
        return (int) (1 / computeRHO(valuesNo, stripsNo));
    }

    public Config(Configuration conf, int valuesNo, int stripsNo, int reduceTasksNo) {
        this.conf = conf;

        conf.setLong(NO_OF_VALUES_KEY, valuesNo);
        conf.setInt(NO_OF_STRIPS_KEY, stripsNo);
        conf.setInt(RATIO_FOR_RANDOM_KEY, computeReversedRHO(valuesNo, stripsNo));
        conf.setInt(NO_OF_REDUCE_TASKS_KEY, reduceTasksNo);
    }

    public Config(Config config) {
        this.conf = new Configuration(config.getConf());
    }

    public Configuration getConf() {
        return conf;
    }
}
