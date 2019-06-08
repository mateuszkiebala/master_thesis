package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class Config {

    public static final String BASE_SCHEMA_KEY = "base.schema.key";
    public static final String STATISTICS_AGGREGATOR_SCHEMA_KEY = "statistics.aggregator.schema.key";
    public static final String KEY_RECORD_SCHEMA_KEY = "key.record.schema.key";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final String NO_OF_VALUES_KEY = "no.of.values";
    public static final String NO_OF_STRIPS_KEY = "no.of.splits";
    public static final String NO_OF_ITEMS_BY_MACHINE = "no.of.items.by.machine";
    public static final String RATIO_FOR_RANDOM_KEY = "ratio.for.random";

    public static final String SORTED_COUNTS_TAG = "sortedCountsTag";
    public static final String SORTED_DATA_TAG = "sortedDataTag";
    public static final String SORTED_DATA_PATTERN = SORTED_DATA_TAG + "-r-*.avro";

    protected Configuration conf;

    public static boolean validateValuesPerStripNo(int valuesNo, int stripsNo) {
        if (valuesNo <= 0 || stripsNo <= 0) {
            return false;
        }
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

    public static long computeItemsNoByMachine(int valuesNo, int stripsNo) {
        return (valuesNo + stripsNo - 1) / stripsNo;
    }

    public Config(Configuration conf, int valuesNo, int stripsNo, int reduceTasksNo) {
        this.conf = conf;

        conf.setLong(NO_OF_VALUES_KEY, valuesNo);
        conf.setInt(NO_OF_STRIPS_KEY, stripsNo);
        conf.setInt(RATIO_FOR_RANDOM_KEY, computeReversedRHO(valuesNo, stripsNo));
        conf.setInt(NO_OF_REDUCE_TASKS_KEY, reduceTasksNo);
        conf.setLong(NO_OF_ITEMS_BY_MACHINE, computeItemsNoByMachine(valuesNo, stripsNo));
    }

    public Config(Config config) {
        this.conf = new Configuration(config.getConf());
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
