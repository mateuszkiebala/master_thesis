package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class StatisticsConfig extends Config {
    protected Schema statisticsAggregatorSchema;

    public StatisticsConfig(Configuration conf, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema) {
        super(conf, cmp, baseSchema);
        this.statisticsAggregatorSchema = statisticsAggregatorSchema;
        Utils.storeSchemaInConf(conf, statisticsAggregatorSchema, STATISTICS_AGGREGATOR_SCHEMA);
    }

    public Schema getStatisticsAggregatorSchema() {
        return statisticsAggregatorSchema;
    }
}
