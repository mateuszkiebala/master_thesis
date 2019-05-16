package minimal_algorithms.ranking;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class RankRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder.record("RankRecord").namespace("minimal_algorithms.ranking")
            .fields()
            .name("rank").type().longType().noDefault()
            .name("record").type(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  public static RankRecord deepCopy(RankRecord record) {
    return SpecificData.get().deepCopy(getClassSchema(), record);
  }

  public static RankRecord deepCopy(RankRecord record, Schema recordSchema) {
    return SpecificData.get().deepCopy(recordSchema, record);
  }

  public static Comparator<RankRecord> cmp = new RankRecordComparator();

  public static class RankRecordComparator implements Comparator<RankRecord> {
    @Override
    public int compare(RankRecord o1, RankRecord o2) {
      return o1.getRank() > o2.getRank() ? 1 : (o1.getRank() < o2.getRank() ? -1 : 0);
    }
  }

  public static Comparator<GenericRecord> genericCmp = new GenericRecordComparator();

  public static class GenericRecordComparator implements Comparator<GenericRecord> {
    @Override
    public int compare(GenericRecord o1, GenericRecord o2) {
      RankRecord rw1 = (RankRecord) o1;
      RankRecord rw2 = (RankRecord) o2;
      return new RankRecordComparator().compare(rw1, rw2);
    }
  }

  private long rank;
  private GenericRecord record;

  public RankRecord() {}

  public RankRecord(java.lang.Long rank, GenericRecord record) {
    this.rank = rank;
    this.record = record;
  }

  public Schema getSchema() { return SCHEMA$; }

  public Object get(int field$) {
    switch (field$) {
    case 0: return rank;
    case 1: return record;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  @Override
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: rank = (Long)value$; break;
    case 1: record = (GenericRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public Long getRank() {
    return rank;
  }

  public void setRank(Long value) {
    this.rank = value;
  }

  public GenericRecord getRecord() {
    return record;
  }

  public void setRecord(GenericRecord record) {
    this.record = record;
  }
}
