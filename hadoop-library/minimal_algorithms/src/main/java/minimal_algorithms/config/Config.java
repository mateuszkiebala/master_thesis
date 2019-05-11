package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class Config {
    public static final String BASE_SCHEMA = "base.schema";
    public static final String STATISTICS_AGGREGATOR_SCHEMA = "statisticer.schema";
    public static final String GROUP_BY_KEY_SCHEMA = "group_by_key.schema";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final int NO_OF_REDUCE_TASKS_DEFAULT = 2;

    protected Comparator cmp;
    protected Configuration conf;
    protected Schema baseSchema;

    public Config(Configuration conf, Comparator cmp, Schema baseSchema) {
        this.cmp = cmp;
        this.conf = conf;
        this.baseSchema = baseSchema;
        Utils.storeComparatorInConf(conf, cmp);
        Utils.storeSchemaInConf(conf, baseSchema, BASE_SCHEMA);
    }

    public Configuration getConf() {
        return conf;
    }

    public Comparator getCmp() {
        return cmp;
    }

    public Schema getBaseSchema() {
        return baseSchema;
    }
}
