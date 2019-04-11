package sortavro.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;

public class SumStatisticer extends StatisticsRecord {
    public static final Schema SCHEMA$ = SchemaBuilder
            .record("SumStatisticer").namespace("sortavro.avro_types.statistics")
            .fields().name("sum").type().intType().noDefault().endRecord();

    public static Schema getClassSchema() { return SCHEMA$; }

    public static Statisticer get(GenericRecord record) {
        new SumStatisticer(((Record4Float) record).getFirst());
    }

    private int sum;

    public SumStatisticer() {}

    public SumStatisticer(Integer sum) {
        this.sum = sum;
    }

    public Schema getSchema() { return SCHEMA$; }

    // Used by DatumWriter.  Applications should not call.
    public Object get(int field$) {
        switch (field$) {
            case 0: return sum;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: sum = (Integer)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer value) {
        this.sum = value;
    }

    public StatisticsRecord merge(StatisticsRecord that) {
        if (that instanceof SumStatisticer) {
            return new SumStatisticer(this.sum + ((SumStatisticer) that).getSum());
        }
        throw new org.apache.avro.AvroRuntimeException("Trying to merge " + that.getClass().getName() + " with SumStatisticer");
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
