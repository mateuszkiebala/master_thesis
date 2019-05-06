package minimal_algorithms.avro_types.utils;

import java.util.List;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import minimal_algorithms.avro_types.statistics.StatisticsRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.generic.GenericRecord;

public class MultipleSendWrappers extends SpecificRecordBase implements SpecificRecord {
    public static Schema SCHEMA$;

    static public void setSchema(Schema schema) {
        SCHEMA$ = SchemaBuilder
                .record("MultipleSendWrappers").namespace("minimal_algorithms.avro_types.utils")
                .fields()
                .name("records").type().array().items(schema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    private List<SendWrapper> records;

    public MultipleSendWrappers() {}

    public MultipleSendWrappers(List<SendWrapper> records) {
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
            case 0: records = (List<SendWrapper>)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public List<SendWrapper> getRecords() {
        return records;
    }

    public void setRecords(List<SendWrapper> value) {
        this.records = value;
    }

    public List<GenericRecord> select1Records() {
        List<GenericRecord> result = new ArrayList<>();
        for (SendWrapper sw : records) {
            if (sw.isRecord1()) {
                result.add(sw.getRecord1());
            }
        }
        return result;
    }

    public List<GenericRecord> select2Records() {
        List<GenericRecord> result = new ArrayList<>();
        for (SendWrapper sw : records) {
            if (sw.isRecord2()) {
                result.add(sw.getRecord2());
            }
        }
        return result;
    }
}
