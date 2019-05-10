package minimal_algorithms.avro_types.statistics;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import minimal_algorithms.avro_types.statistics.StatisticsRecord;
import org.apache.avro.specific.SpecificData;

public class MultipleStatisticRecords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    static public void setSchema(Schema schema) {
        SCHEMA$ = SchemaBuilder
                .record("MultipleStatisticRecord").namespace("minimal_algorithms.avro_types.statistics")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static MultipleStatisticRecords deepCopy(MultipleStatisticRecords record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static MultipleStatisticRecords deepCopy(MultipleStatisticRecords record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    private List<StatisticsRecord> records;

    public MultipleStatisticRecords() {}

    public MultipleStatisticRecords(List<StatisticsRecord> records) {
        this.records = records;
    }

    public Schema getSchema() { return SCHEMA$; }

    public Object get(int field$) {
        switch (field$) {
            case 0: return records;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: records = (List<StatisticsRecord>)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public List<StatisticsRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StatisticsRecord> value) {
        this.records = value;
    }
}
