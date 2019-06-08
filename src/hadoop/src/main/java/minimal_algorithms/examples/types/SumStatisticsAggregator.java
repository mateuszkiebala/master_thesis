package minimal_algorithms.examples.types;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.statistics.*;

public class SumStatisticsAggregator extends StatisticsAggregator {
    public static final Schema SCHEMA$ = SchemaBuilder
            .record("SumStatisticsAggregator").namespace("minimal_algorithms.examples.types")
            .fields().name("sum").type().intType().noDefault().endRecord();

    public static Schema getClassSchema() { return SCHEMA$; }

    private int sum;

    public SumStatisticsAggregator() {}

    public SumStatisticsAggregator(Integer sum) {
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

    public StatisticsAggregator merge(StatisticsAggregator that) {
        if (that instanceof SumStatisticsAggregator) {
            return new SumStatisticsAggregator(this.sum + ((SumStatisticsAggregator) that).getSum());
        }
        throw new org.apache.avro.AvroRuntimeException("Trying to merge " + that.getClass().getName() + " with SumStatisticsAggregator");
    }
}
