package minimal_algorithms.avro_types.group_by;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;

public class MultipleGroupByRecords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema schema) {
        SCHEMA$ = SchemaBuilder
                .record("MultipleGroupByRecords").namespace("minimal_algorithms.avro_types.group_by")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    private List<GroupByRecord> records;

    public MultipleGroupByRecords() {}

    public MultipleGroupByRecords(List<GroupByRecord> records) {
        this.records = records;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return records;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: records = (List<GroupByRecord>)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public List<GroupByRecord> getRecords() {
        return records;
    }

    public void setRecords(List<GroupByRecord> value) {
        this.records = value;
    }
}
