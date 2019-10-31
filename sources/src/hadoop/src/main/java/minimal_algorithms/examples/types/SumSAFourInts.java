package minimal_algorithms.hadoop.examples.types;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.hadoop.statistics.*;

public class SumSAFourInts extends StatisticsAggregator {
  public static final Schema SCHEMA$ = SchemaBuilder
      .record("SumSAFourInts").namespace("minimal_algorithms.hadoop.examples.types")
      .fields().name("sum").type().intType().noDefault().endRecord();

  public static Schema getClassSchema() { return SCHEMA$; }

  private int sum;

  public SumSAFourInts() {}

  public SumSAFourInts(Integer sum) {
    this.sum = sum;
  }

  public String toString() {
    return "SUM: " + this.sum;
  }

  public Schema getSchema() { return SCHEMA$; }

  public void init(GenericRecord record) {
    FourInts fourInts = (FourInts) record;
    this.sum = fourInts.getSecond() + fourInts.getThird() + fourInts.getFourth();
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
    if (that instanceof SumSAFourInts) {
      return new SumSAFourInts(this.sum + ((SumSAFourInts) that).getSum());
    }
    throw new org.apache.avro.AvroRuntimeException("Trying to merge " + that.getClass().getName() + " with SumSAFourInts");
  }
}
