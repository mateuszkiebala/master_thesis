package minimal_algorithms.sending;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;

public class SendWrapper extends SpecificRecordBase implements SpecificRecord {

    public static Schema SCHEMA$;

    static public void setSchema(Schema record1Schema, Schema record2Schema) {
        System.out.println(record1Schema);
        System.out.println(record2Schema);
        SCHEMA$ = SchemaBuilder
                .record("SendWrapper").namespace("minimal_algorithms.sending")
                .fields()
                .name("record1").type().optional().type(record1Schema)
                .name("record2").type().optional().type(record2Schema)
                .endRecord();
        System.out.println(SCHEMA$);
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static SendWrapper deepCopy(SendWrapper record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static SendWrapper deepCopy(SendWrapper record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    private GenericRecord record1;
    private GenericRecord record2;

    public SendWrapper() {}

    public SendWrapper(GenericRecord record1, GenericRecord record2) {
        this.record1 = record1;
        this.record2 = record2;
    }

    public Object get(int field$) {
        switch (field$) {
            case 0: return record1;
            case 1: return record2;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: record1 = (GenericRecord)value$; break;
            case 1: record2 = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Schema getSchema() { return SCHEMA$; }

    public GenericRecord getRecord() {
        return isType1() ? record1 : record2;
    }

    public GenericRecord getRecord1() {
        return record1;
    }

    public GenericRecord getRecord2() {
        return record2;
    }

    public void setRecord1(GenericRecord record1) {
        this.record1 = record1;
    }

    public void setRecord2(GenericRecord record2) {
        this.record2 = record2;
    }

    public boolean isType1() {
        return record1 != null;
    }

    public boolean isType2() {
        return record2 != null;
    }

    public int getType() {
        return isType1() ? 1 : 2;
    }
}
