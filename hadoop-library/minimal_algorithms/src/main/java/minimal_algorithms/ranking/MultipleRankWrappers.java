package minimal_algorithms.ranking;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;

public class MultipleRankWrappers extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("MultipleRankWrappers").namespace("minimal_algorithms.ranking")
            .fields()
            .name("records").type().array().items(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  public static MultipleRankWrappers deepCopy(MultipleRankWrappers record) {
    return SpecificData.get().deepCopy(getClassSchema(), record);
  }

  public static MultipleRankWrappers deepCopy(MultipleRankWrappers record, Schema recordSchema) {
    return SpecificData.get().deepCopy(recordSchema, record);
  }

  private List<RankWrapper> records;

  public MultipleRankWrappers() {}

  public MultipleRankWrappers(List<RankWrapper> records) {
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
    case 0: records = (List<RankWrapper>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public List<RankWrapper> getRecords() {
    return records;
  }

  public void setRecords(List<RankWrapper> value) {
    this.records = value;
  }
}
