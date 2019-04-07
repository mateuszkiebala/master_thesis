package sortavro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaForwarder {
    private static Schema schema;
    static Schema getSchema() {
        return schema;
    }

    static void setSchema(Schema schema) {
        SchemaForwarder.schema = SchemaBuilder
                .record("MultiGenericRecord").namespace("sortavro")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }
}
