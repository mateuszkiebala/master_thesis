package sortavro.avro_types.ranking;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class RankWrapper extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -3644392059589873895L;
  public static final org.apache.avro.Schema SCHEMA$ = RankWrapperSchemaCreator.getSchema();
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int rank;
  @Deprecated public GenericRecord value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RankWrapper() {}

  /**
   * All-args constructor.
   * @param rank The new value for rank
   * @param value The new value for value
   */
  public RankWrapper(java.lang.Integer rank, GenericRecord value) {
    this.rank = rank;
    this.value = value;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return rank;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: rank = (java.lang.Integer)value$; break;
    case 1: value = (GenericRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'rank' field.
   * @return The value of the 'rank' field.
   */
  public java.lang.Integer getRank() {
    return rank;
  }

  /**
   * Sets the value of the 'rank' field.
   * @param value the value to set.
   */
  public void setRank(java.lang.Integer value) {
    this.rank = value;
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public GenericRecord getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(GenericRecord value) {
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
