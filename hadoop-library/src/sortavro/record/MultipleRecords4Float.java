/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package sortavro.record;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class MultipleRecords4Float extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2914383606488360834L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MultipleRecords4Float\",\"namespace\":\"sortavro.record\",\"fields\":[{\"name\":\"arrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Record4Float\",\"fields\":[{\"name\":\"first\",\"type\":\"float\"},{\"name\":\"second\",\"type\":\"float\"},{\"name\":\"third\",\"type\":\"float\"},{\"name\":\"fourth\",\"type\":\"float\"}]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.util.List<sortavro.record.Record4Float> arrayOfRecords;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public MultipleRecords4Float() {}

  /**
   * All-args constructor.
   * @param arrayOfRecords The new value for arrayOfRecords
   */
  public MultipleRecords4Float(java.util.List<sortavro.record.Record4Float> arrayOfRecords) {
    this.arrayOfRecords = arrayOfRecords;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return arrayOfRecords;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: arrayOfRecords = (java.util.List<sortavro.record.Record4Float>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'arrayOfRecords' field.
   * @return The value of the 'arrayOfRecords' field.
   */
  public java.util.List<sortavro.record.Record4Float> getArrayOfRecords() {
    return arrayOfRecords;
  }

  /**
   * Sets the value of the 'arrayOfRecords' field.
   * @param value the value to set.
   */
  public void setArrayOfRecords(java.util.List<sortavro.record.Record4Float> value) {
    this.arrayOfRecords = value;
  }

  /**
   * Creates a new MultipleRecords4Float RecordBuilder.
   * @return A new MultipleRecords4Float RecordBuilder
   */
  public static sortavro.record.MultipleRecords4Float.Builder newBuilder() {
    return new sortavro.record.MultipleRecords4Float.Builder();
  }

  /**
   * Creates a new MultipleRecords4Float RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new MultipleRecords4Float RecordBuilder
   */
  public static sortavro.record.MultipleRecords4Float.Builder newBuilder(sortavro.record.MultipleRecords4Float.Builder other) {
    return new sortavro.record.MultipleRecords4Float.Builder(other);
  }

  /**
   * Creates a new MultipleRecords4Float RecordBuilder by copying an existing MultipleRecords4Float instance.
   * @param other The existing instance to copy.
   * @return A new MultipleRecords4Float RecordBuilder
   */
  public static sortavro.record.MultipleRecords4Float.Builder newBuilder(sortavro.record.MultipleRecords4Float other) {
    return new sortavro.record.MultipleRecords4Float.Builder(other);
  }

  /**
   * RecordBuilder for MultipleRecords4Float instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MultipleRecords4Float>
    implements org.apache.avro.data.RecordBuilder<MultipleRecords4Float> {

    private java.util.List<sortavro.record.Record4Float> arrayOfRecords;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(sortavro.record.MultipleRecords4Float.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.arrayOfRecords)) {
        this.arrayOfRecords = data().deepCopy(fields()[0].schema(), other.arrayOfRecords);
        fieldSetFlags()[0] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing MultipleRecords4Float instance
     * @param other The existing instance to copy.
     */
    private Builder(sortavro.record.MultipleRecords4Float other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.arrayOfRecords)) {
        this.arrayOfRecords = data().deepCopy(fields()[0].schema(), other.arrayOfRecords);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'arrayOfRecords' field.
      * @return The value.
      */
    public java.util.List<sortavro.record.Record4Float> getArrayOfRecords() {
      return arrayOfRecords;
    }

    /**
      * Sets the value of the 'arrayOfRecords' field.
      * @param value The value of 'arrayOfRecords'.
      * @return This builder.
      */
    public sortavro.record.MultipleRecords4Float.Builder setArrayOfRecords(java.util.List<sortavro.record.Record4Float> value) {
      validate(fields()[0], value);
      this.arrayOfRecords = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'arrayOfRecords' field has been set.
      * @return True if the 'arrayOfRecords' field has been set, false otherwise.
      */
    public boolean hasArrayOfRecords() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'arrayOfRecords' field.
      * @return This builder.
      */
    public sortavro.record.MultipleRecords4Float.Builder clearArrayOfRecords() {
      arrayOfRecords = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public MultipleRecords4Float build() {
      try {
        MultipleRecords4Float record = new MultipleRecords4Float();
        record.arrayOfRecords = fieldSetFlags()[0] ? this.arrayOfRecords : (java.util.List<sortavro.record.Record4Float>) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
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
