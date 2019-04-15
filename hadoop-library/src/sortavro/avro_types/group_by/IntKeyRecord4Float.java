package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;
import sortavro.record.Record4Float;
import sortavro.avro_types.utils.KeyRecord;
import java.util.Comparator;

public class IntKeyRecord4Float extends KeyRecord {

  public static final Schema SCHEMA$ = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IntKeyRecord4Float\",\"namespace\":\"sortavro.avro_types.group_by\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}");
  public static Schema getClassSchema() { return SCHEMA$; }

  private GenericRecord objectRecord;
  public int value;

  public IntKeyRecord4Float() {}

  public IntKeyRecord4Float(int value) {
    this.value = value;
  }

  public void create(GenericRecord record) {
    this.objectRecord = record;
    this.value = Math.round(((Record4Float) record).getFirst());
  }

  public GenericRecord getObjectRecord() {
    return this.objectRecord;
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
