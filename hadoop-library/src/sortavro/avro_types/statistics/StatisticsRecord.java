package sortavro.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

public class StatisticsRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static final Schema SCHEMA$ = StatisticsRecordSchemaCreator.getSchema();

    public static Schema getClassSchema() { return SCHEMA$; }

    private Statisticer statisticer;
    private GenericRecord mainObject;

    public StatisticsRecord() {}

    public StatisticsRecord(Statisticer statisticer, GenericRecord mainObject) {
        this.statisticer = statisticer;
        this.mainObject = mainObject;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticer;
            case 1: return mainObject;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticer = (Statisticer)value$; break;
            case 1: mainObject = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Statisticer getStatisticer() {
        return statisticer;
    }

    public void setStatisticer(Statisticer statisticer) {
        this.statisticer = statisticer;
    }

    public GenericRecord getMainObject() {
        return mainObject;
    }

    public void setMainObject(GenericRecord mainObject) {
        this.mainObject = mainObject;
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
