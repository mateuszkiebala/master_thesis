package minimal_algorithms.hadoop.utils;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class MultipleRecords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("MultipleRecords").namespace("minimal_algorithms.hadoop.utils")
            .fields()
            .name("records").type().array().items(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  public static MultipleRecords deepCopy(MultipleRecords record) {
    return SpecificData.get().deepCopy(getClassSchema(), record);
  }

  public static MultipleRecords deepCopy(Schema recordSchema, MultipleRecords record) {
    return SpecificData.get().deepCopy(recordSchema, record);
  }

  private List<GenericRecord> records;

  public MultipleRecords() {}

  public MultipleRecords(List<GenericRecord> records) {
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
