package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class StatisticsConfig extends BaseConfig {
    protected Schema statisticsAggregatorSchema;

    public StatisticsConfig(Config config, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) {
        super(config, cmp, baseSchema);
        this.statisticsAggregatorSchema = statisticsAggregatorSchema;
        Utils.storeSchemaInConf(conf, statisticsAggregatorSchema, STATISTICS_AGGREGATOR_SCHEMA_KEY);
    }

    public Schema getStatisticsAggregatorSchema() {
        return statisticsAggregatorSchema;
    }
}
