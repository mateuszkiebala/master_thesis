package sortavro.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class StatisticsRecordSchemaCreator {
    private static Schema schema;

    static public Schema getSchema() {
        return schema;
    }

    static public void setSchema(Schema statisticerSchema, Schema mainObjectSchema) {
        SchemaBuilder.record("StatisticsRecord")
                .namespace("sortavro.avro_types.statistics")
                .fields()
                .name("statisticer").type(statisticerSchema).noDefault()
                .name("mainObject").type(mainObjectSchema).noDefault()
                .endRecord();
    }
}
