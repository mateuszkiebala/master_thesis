package sortavro.avro_types.ranking;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class MultipleRankWrappersSchemaCreator {
    private static Schema schema;
    static public Schema getSchema() {
        return schema;
    }

    static public void setMainObjectSchema(Schema schema) {
        MultipleRankWrappersSchemaCreator.schema = SchemaBuilder
                .record("MultipleRankWrappers").namespace("sortavro.avro_types.ranking")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }
}
