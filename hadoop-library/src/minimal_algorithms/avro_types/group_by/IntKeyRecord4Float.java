package minimal_algorithms.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.record.Record4Float;
import minimal_algorithms.avro_types.utils.KeyRecord;
import java.util.Comparator;

public class IntKeyRecord4Float extends KeyRecord<IntKeyRecord4Float> {

  public static final Schema SCHEMA$ = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IntKeyRecord4Float\",\"namespace\":\"minimal_algorithms.avro_types.group_by\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}");
  public static Schema getClassSchema() { return SCHEMA$; }

  private GenericRecord objectRecord;
  private int value;

  public IntKeyRecord4Float() {}

  public IntKeyRecord4Float(int value) {
    this.value = value;
  }

  @Override
  public void init(GenericRecord record) {
    this.objectRecord = record;
    this.value = Math.round(((Record4Float) record).getFirst());
  }

  @Override
  public int compare(IntKeyRecord4Float that) {
    if (this.value > that.getValue())
      return 1;
    else if (this.value == that.getValue())
      return 0;
    else
      return -1;
  }

  public Object get(int field$) {
    switch (field$) {
      case 0: return value;
      default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public void put(int field$, Object value$) {
    switch (field$) {
      case 0: value = (Integer)value$; break;
      default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof IntKeyRecord4Float)) {
      return false;
    }

    return this.value == ((IntKeyRecord4Float) o).getValue();
  }

  @Override
  public int hashCode() {
    return this.value;
  }

  @Override
  public String toString() {
    return ((Integer)this.value).toString();
  }

  public Schema getSchema() { return SCHEMA$; }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
