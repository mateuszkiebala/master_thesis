package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import sortavro.record.Record4Float;
import sortavro.avro_types.utils.KeyRecord;

public class IntKeyRecord4Float extends KeyRecord<IntKeyRecord4Float> {

  public static final Schema SCHEMA$ = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IntKeyRecord4Float\",\"namespace\":\"sortavro.avro_types.group_by\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}");
  public static Schema getClassSchema() { return SCHEMA$; }
  public int value;
  public float all;

  public IntKeyRecord4Float() {}

  public IntKeyRecord4Float(int value) {
    this.value = value;
  }

  public void create(GenericRecord record) {
    this.value = Math.round(((Record4Float) record).getFirst()) % 10;
    this.all = ((Record4Float) record).getFirst();
  }

  @Override
  public int compareTo(IntKeyRecord4Float that) {
    return ((Integer) this.value).compareTo(that.getValue());
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
    return ((Integer)this.value).toString() + " | " + this.all;
  }

  public Schema getSchema() { return SCHEMA$; }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
