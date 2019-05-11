package minimal_algorithms.config;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.Utils;

public class BaseConfig extends Config {
    private Configuration conf;
    private Schema baseSchema;

    public BaseConfig(Configuration conf, Schema baseSchema) {
        this.conf = conf;
        this.baseSchema = baseSchema;
        Utils.storeSchemaInConf(conf, baseSchema, BASE_SCHEMA);
    }

    public Configuration getConf() {
        return conf;
    }

    public Schema getBaseSchema() {
        return baseSchema;
    }
}
