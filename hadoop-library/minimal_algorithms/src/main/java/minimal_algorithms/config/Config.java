package minimal_algorithms.config;

import org.apache.hadoop.conf.Configuration;

public class Config {
    public static final String BASE_SCHEMA = "base.schema";
    public static final String STATISTICS_AGGREGATOR_SCHEMA = "statisticer.schema";
    public static final String GROUP_BY_KEY_SCHEMA = "group_by_key.schema";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final int NO_OF_REDUCE_TASKS_DEFAULT = 2;
}
