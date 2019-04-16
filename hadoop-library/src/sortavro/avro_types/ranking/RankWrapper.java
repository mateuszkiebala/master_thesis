package sortavro.avro_types.ranking;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class RankWrapper extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static Schema SCHEMA$;

  public static void setSchema(Schema schema) {
    SCHEMA$ = SchemaBuilder
            .record("RankWrapper").namespace("sortavro.avro_types.ranking")
            .fields()
            .name("rank").type().intType().noDefault()
            .name("mainObject").type(schema).noDefault()
            .endRecord();
  }

  public static Schema getClassSchema() { return SCHEMA$; }

  private int rank;
  private GenericRecord value;

  public RankWrapper() {}

  public RankWrapper(java.lang.Integer rank, GenericRecord value) {
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
    case 0: rank = (Integer)value$; break;
    case 1: value = (GenericRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  public Integer getRank() {
    return rank;
  }

  public void setRank(Integer value) {
    this.rank = value;
  }

  public GenericRecord getValue() {
    return value;
  }

  public void setValue(GenericRecord value) {
    this.value = value;
  }
}
