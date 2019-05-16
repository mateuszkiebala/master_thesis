package minimal_algorithms.ranking;

import java.util.List;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class MultipleRankRecords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("MultipleRankRecords").namespace("minimal_algorithms.ranking")
            .fields()
            .name("records").type().array().items(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  public static MultipleRankRecords deepCopy(MultipleRankRecords record) {
    return SpecificData.get().deepCopy(getClassSchema(), record);
  }

  public static MultipleRankRecords deepCopy(Schema recordSchema, MultipleRankRecords record) {
    return SpecificData.get().deepCopy(recordSchema, record);
  }

  private List<RankRecord> records;

  public MultipleRankRecords() {}

  public MultipleRankRecords(List<RankRecord> records) {
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
    case 0: records = (List<RankRecord>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public List<GenericRecord> getBaseRecords() {
    List<GenericRecord> baseRecords = new ArrayList<>();
    for (RankRecord record : records) {
      baseRecords.add(record.getRecord());
    }
    return baseRecords;
  }

  public List<RankRecord> getRecords() {
    return records;
  }

  public void setRecords(List<RankRecord> value) {
    this.records = value;
  }
}
