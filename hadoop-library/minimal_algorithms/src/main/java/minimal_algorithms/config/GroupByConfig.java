package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class GroupByConfig extends StatisticsConfig {
    protected Schema keyRecordSchema;

    public GroupByConfig(Config config, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema, Schema keyRecordSchema) {
        super(config, cmp, baseSchema, statisticsAggregatorSchema);
        this.keyRecordSchema = keyRecordSchema;
        Utils.storeSchemaInConf(conf, keyRecordSchema, GROUP_BY_KEY_SCHEMA);
    }

    public Schema getKeyRecordSchema() {
        return keyRecordSchema;
    }
}
