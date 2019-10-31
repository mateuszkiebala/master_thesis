package minimal_algorithms.hadoop.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.hadoop.utils.Utils;

public class Config {

    public static final String BASE_SCHEMA_KEY = "base.schema.key";
    public static final String STATISTICS_AGGREGATOR_SCHEMA_KEY = "statistics.aggregator.schema.key";
    public static final String KEY_RECORD_SCHEMA_KEY = "key.record.schema.key";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final String NO_OF_ITEMS_KEY = "no.of.items";
    public static final String NO_OF_PARTITIONS_KEY = "no.of.partitions";
    public static final String NO_OF_ITEMS_BY_MACHINE = "no.of.items.by.machine";
    public static final String RATIO_FOR_RANDOM_KEY = "ratio.for.random";

    public static final String SORTED_COUNTS_TAG = "sortedCountsTag";
    public static final String SORTED_DATA_TAG = "sortedDataTag";
    public static final String SORTED_DATA_PATTERN = SORTED_DATA_TAG + "-r-*.avro";

    protected Configuration conf;
    private int itemsNo;
    private int partitionsNo;
    private int reduceTasksNo;

    public static boolean validateItemsPerPartition(int itemsNo, int partitionsNo) {
        if (itemsNo <= 0 || partitionsNo <= 0) {
            return false;
        }
        int itemsPerPartition = 1 + itemsNo / partitionsNo;
        return itemsPerPartition / 20 > Integer.MAX_VALUE ? false : true;
    }

    public static int computeItemsPerPartition(int itemsNo, int partitionsNo) {
        return 1 + itemsNo / partitionsNo;
    }

    public static double computeRHO(int itemsNo, int partitionsNo) {
        int itemsPerPartition = computeItemsPerPartition(itemsNo, partitionsNo);
        return 1. / itemsPerPartition * Math.log(((double) itemsNo) * partitionsNo);
    }

    public static int computeReversedRHO(int itemsNo, int partitionsNo) {
        return (int) (1 / computeRHO(itemsNo, partitionsNo));
    }

    public static long computeItemsNoByMachine(int itemsNo, int partitionsNo) {
        return (itemsNo + partitionsNo - 1) / partitionsNo;
    }

    public Config(Configuration conf, int itemsNo, int partitionsNo, int reduceTasksNo) {
        this.conf = conf;
        this.itemsNo = itemsNo;
        this.partitionsNo = partitionsNo;
        this.reduceTasksNo = reduceTasksNo;

        conf.setLong(NO_OF_ITEMS_KEY, itemsNo);
        conf.setInt(NO_OF_PARTITIONS_KEY, partitionsNo);
        conf.setInt(RATIO_FOR_RANDOM_KEY, computeReversedRHO(itemsNo, partitionsNo));
        conf.setInt(NO_OF_REDUCE_TASKS_KEY, reduceTasksNo);
        conf.setLong(NO_OF_ITEMS_BY_MACHINE, computeItemsNoByMachine(itemsNo, partitionsNo));
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

    public int getReduceTasksNo() {
        return reduceTasksNo;
    }

    public int getPartitionsNo() {
        return partitionsNo;
    }

    public int getItemsNo() {
        return itemsNo;
    }

    public boolean validateItemsPerPartition() {
        return validateItemsPerPartition(itemsNo, partitionsNo);
    }
}
