package minimal_algorithms.config;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class StatisticsConfig extends Config {
    private Configuration conf;
    private Schema baseSchema;
    private Schema statisticsAggregatorSchema;

    public StatisticsConfig(Configuration conf, Schema baseSchema, Schema statisticsAggregatorSchema) {
        this.conf = conf;
        this.baseSchema = baseSchema;
        this.statisticsAggregatorSchema = statisticsAggregatorSchema;
        Utils.storeSchemaInConf(conf, baseSchema, BASE_SCHEMA);
        Utils.storeSchemaInConf(conf, statisticsAggregatorSchema, STATISTICS_AGGREGATOR_SCHEMA);
    }

    public Configuration getConf() {
        return conf;
    }

    public Schema getBaseSchema() {
        return baseSchema;
    }

    public Schema getStatisticsAggregatorSchema() {
        return statisticsAggregatorSchema;
    }
}
