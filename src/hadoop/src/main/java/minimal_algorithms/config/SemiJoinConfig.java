package minimal_algorithms.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.utils.Utils;

public class SemiJoinConfig extends BaseConfig {
    protected Schema keyRecordSchema;

    public SemiJoinConfig(Config config, Comparator cmp, Schema baseSchema, Schema keyRecordSchema) {
        super(config, cmp, baseSchema);
        this.keyRecordSchema = keyRecordSchema;
        Utils.storeSchemaInConf(conf, keyRecordSchema, KEY_RECORD_SCHEMA_KEY);
    }

    public Schema getKeyRecordSchema() {
        return keyRecordSchema;
    }
}
