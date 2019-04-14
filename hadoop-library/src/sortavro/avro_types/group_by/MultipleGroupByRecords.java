package sortavro.avro_types.group_by;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

public class MultipleGroupByRecords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static final Schema SCHEMA$ = MultipleGroupByRecordsSchemaCreator.getSchema();
    public static Schema getClassSchema() { return SCHEMA$; }
    @Deprecated public List<GroupByRecord> records;

    public MultipleGroupByRecords() {}

    public MultipleGroupByRecords(List<GroupByRecord> records) {
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

    private static final org.apache.avro.io.DatumWriter
            WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

    @Override public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    private static final org.apache.avro.io.DatumReader
            READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

    @Override public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

}
