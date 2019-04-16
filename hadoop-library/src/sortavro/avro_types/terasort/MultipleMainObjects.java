package sortavro.avro_types.terasort;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class MultipleMainObjects extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("MultipleMainObjects").namespace("sortavro.avro_types.terasort")
            .fields()
            .name("records").type().array().items(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  private List<GenericRecord> records;

  public MultipleMainObjects() {}

  public MultipleMainObjects(List<GenericRecord> records) {
    this.records = records;
  }

  public Schema getSchema() { return SCHEMA$; }

  @Override
  public Object get(int field$) {
    switch (field$) {
    case 0: return records;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  @Override
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: records = (List<GenericRecord>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public List<GenericRecord> getRecords() {
    return records;
  }

  public void setRecords(List<GenericRecord> value) {
    this.records = value;
  }
}
