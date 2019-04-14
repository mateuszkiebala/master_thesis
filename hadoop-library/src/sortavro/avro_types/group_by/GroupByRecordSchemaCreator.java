package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class GroupByRecordSchemaCreator {
    private static Schema schema;

    static public Schema getSchema() {
        return schema;
    }

    static public void setSchema(Schema statisticerSchema, Schema keySchema) {
        GroupByRecordSchemaCreator.schema = SchemaBuilder.record("GroupByRecord")
                .namespace("sortavro.avro_types.group_by")
                .fields()
                .name("statisticer").type(statisticerSchema).noDefault()
                .name("key").type(keySchema).noDefault()
                .endRecord();
    }
}
