package minimal_algorithms.config;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class GroupByConfig extends Config {
    private Configuration conf;
    private Schema baseSchema;
    private Schema statisticsAggregatorSchema;
    private Schema keyRecordSchema;

    public GroupByConfig(Configuration conf, Schema baseSchema, Schema statisticsAggregatorSchema, Schema keyRecordSchema) {
        this.conf = conf;
        this.baseSchema = baseSchema;
        this.statisticsAggregatorSchema = statisticsAggregatorSchema;
        this.keyRecordSchema = keyRecordSchema;
        Utils.storeSchemaInConf(conf, baseSchema, BASE_SCHEMA);
        Utils.storeSchemaInConf(conf, statisticsAggregatorSchema, STATISTICS_AGGREGATOR_SCHEMA);
        Utils.storeSchemaInConf(conf, keyRecordSchema, GROUP_BY_KEY_SCHEMA);
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

    public Schema getKeyRecordSchema() {
        return keyRecordSchema;
    }
}
