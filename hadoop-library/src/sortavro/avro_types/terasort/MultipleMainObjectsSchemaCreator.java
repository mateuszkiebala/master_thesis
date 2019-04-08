package sortavro.avro_types.terasort;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class MultipleMainObjectsSchemaCreator {
    private static Schema schema;
    static public Schema getSchema() {
        return schema;
    }

    static public void setMainObjectSchema(Schema schema) {
        MultipleMainObjectsSchemaCreator.schema = SchemaBuilder
                .record("MultipleMainObjects").namespace("sortavro.avro_types.terasort")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }
}
