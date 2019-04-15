package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class MultipleGroupByRecordsSchemaCreator {
    private static Schema schema;
    static public Schema getSchema() {
        return schema;
    }

    static public void setSchema(Schema schema) {
        MultipleGroupByRecordsSchemaCreator.schema = SchemaBuilder
                .record("MultipleGroupByRecords").namespace("sortavro.avro_types.group_by")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }
}
