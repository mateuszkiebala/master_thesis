package minimal_algorithms.ranking;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class RankWrapper extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("RankWrapper").namespace("minimal_algorithms.ranking")
            .fields()
            .name("rank").type().longType().noDefault()
            .name("baseRecord").type(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  public static RankWrapper deepCopy(RankWrapper record) {
    return SpecificData.get().deepCopy(getClassSchema(), record);
  }

  public static RankWrapper deepCopy(RankWrapper record, Schema recordSchema) {
    return SpecificData.get().deepCopy(recordSchema, record);
  }

  public static Comparator<RankWrapper> cmp = new RankWrapperComparator();

  public static class RankWrapperComparator implements Comparator<RankWrapper> {
    @Override
    public int compare(RankWrapper o1, RankWrapper o2) {
      return o1.getRank() > o2.getRank() ? 1 : (o1.getRank() < o2.getRank() ? -1 : 0);
    }
  }

  private long rank;
  private GenericRecord value;

  public RankWrapper() {}

  public RankWrapper(java.lang.Long rank, GenericRecord value) {
    this.rank = rank;
    this.value = value;
  }

  public Schema getSchema() { return SCHEMA$; }

  public Object get(int field$) {
    switch (field$) {
    case 0: return rank;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  @Override
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: rank = (Long)value$; break;
    case 1: value = (GenericRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public Long getRank() {
    return rank;
  }

  public void setRank(Long value) {
    this.rank = value;
  }

  public GenericRecord getValue() {
    return value;
  }

  public void setValue(GenericRecord value) {
    this.value = value;
  }
}
