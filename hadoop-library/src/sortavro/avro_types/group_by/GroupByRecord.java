package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import sortavro.avro_types.statistics.Statisticer;
import sortavro.avro_types.utils.KeyRecord;

public class GroupByRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static final Schema SCHEMA$ = GroupByRecordSchemaCreator.getSchema();

    public Statisticer statisticer;
    public KeyRecord key;

    public GroupByRecord() {}

    public GroupByRecord(Statisticer statisticer, KeyRecord key) {
        this.statisticer = statisticer;
        this.key = key;
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticer;
            case 1: return key;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticer = (Statisticer)value$; break;
            case 1: key = (KeyRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public KeyRecord getKey() {
        return key;
    }

    public void setKey(KeyRecord key) {
        this.key = key;
    }

    public Statisticer getStatisticer() {
        return statisticer;
    }

    public void setStatisticer(Statisticer statisticer) {
        this.statisticer = statisticer;
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
