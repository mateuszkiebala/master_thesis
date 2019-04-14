package sortavro.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class MultipleStatisticRecordsSchemaCreator {
    private static Schema schema;
    static public Schema getSchema() {
        return schema;
    }

    static public void setSchema(Schema schema) {
        MultipleStatisticRecordsSchemaCreator.schema = SchemaBuilder
                .record("MultipleStatisticRecord").namespace("sortavro.avro_types.statistics")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }
}
