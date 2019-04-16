package sortavro.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import sortavro.record.Record4Float;

public class SumStatisticer extends Statisticer {
    public static final Schema SCHEMA$ = SchemaBuilder
            .record("SumStatisticer").namespace("sortavro.avro_types.statistics")
            .fields().name("sum").type().intType().noDefault().endRecord();

    public static Schema getClassSchema() { return SCHEMA$; }

    private int sum;

    public SumStatisticer() {}

    public SumStatisticer(Integer sum) {
        this.sum = sum;
    }

    public String toString() {
        return "SUM: " + this.sum;
    }

    public Schema getSchema() { return SCHEMA$; }

    public void init(GenericRecord record) {
        this.sum = Math.round(((Record4Float) record).getSecond());
    }

    public Object get(int field$) {
        switch (field$) {
            case 0: return sum;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

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

    public Statisticer merge(Statisticer that) {
        if (that instanceof SumStatisticer) {
            return new SumStatisticer(this.sum + ((SumStatisticer) that).getSum());
        }
        throw new org.apache.avro.AvroRuntimeException("Trying to merge " + that.getClass().getName() + " with SumStatisticer");
    }
}
