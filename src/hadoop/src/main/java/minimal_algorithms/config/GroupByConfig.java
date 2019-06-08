package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class GroupByConfig extends StatisticsConfig {
    protected Schema keyRecordSchema;

    public GroupByConfig(Config config, Comparator cmp, Schema baseSchema, Schema statisticsAggregatorSchema, Schema keyRecordSchema) {
        super(config, cmp, baseSchema, statisticsAggregatorSchema);
        this.keyRecordSchema = keyRecordSchema;
        Utils.storeSchemaInConf(conf, keyRecordSchema, KEY_RECORD_SCHEMA_KEY);
    }

    public Schema getKeyRecordSchema() {
        return keyRecordSchema;
    }
}
