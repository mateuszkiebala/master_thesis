package sortavro.avro_types.ranking;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class RankWrapperSchemaCreator {
    private static Schema schema;
    static public Schema getSchema() {
        return schema;
    }

    static public void setMainObjectSchema(Schema schema) {
        RankWrapperSchemaCreator.schema = SchemaBuilder
                .record("RankWrapper").namespace("sortavro.avro_types.ranking")
                .fields()
                .name("rank").type().intType().noDefault()
                .name("mainObject").type(schema).noDefault()
                .endRecord();
    }
}
