package minimal_algorithms.hadoop.config;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.hadoop.utils.Utils;

public class BaseConfig extends Config {

    protected Comparator cmp;
    protected Schema baseSchema;

    public BaseConfig(Config config, Comparator cmp, Schema baseSchema) {
        super(config);
        this.cmp = cmp;
        this.baseSchema = baseSchema;
        Utils.storeComparatorInConf(conf, cmp);
        Utils.storeSchemaInConf(conf, baseSchema, BASE_SCHEMA_KEY);
    }

    public Comparator getCmp() {
        return cmp;
    }

    public Schema getBaseSchema() {
        return baseSchema;
    }
}
