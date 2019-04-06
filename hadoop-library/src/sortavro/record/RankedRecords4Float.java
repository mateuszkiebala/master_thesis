/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package sortavro.record;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class RankedRecords4Float extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 4785228628713973121L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RankedRecords4Float\",\"namespace\":\"sortavro.record\",\"fields\":[{\"name\":\"ranking\",\"type\":\"int\"},{\"name\":\"value\",\"type\":{\"type\":\"record\",\"name\":\"Record4Float\",\"fields\":[{\"name\":\"first\",\"type\":\"float\"},{\"name\":\"second\",\"type\":\"float\"},{\"name\":\"third\",\"type\":\"float\"},{\"name\":\"fourth\",\"type\":\"float\"}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int ranking;
  @Deprecated public sortavro.record.Record4Float value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RankedRecords4Float() {}

  /**
   * All-args constructor.
   * @param ranking The new value for ranking
   * @param value The new value for value
   */
  public RankedRecords4Float(java.lang.Integer ranking, sortavro.record.Record4Float value) {
    this.ranking = ranking;
    this.value = value;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return ranking;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: ranking = (java.lang.Integer)value$; break;
    case 1: value = (sortavro.record.Record4Float)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'ranking' field.
   * @return The value of the 'ranking' field.
   */
  public java.lang.Integer getRanking() {
    return ranking;
  }

  /**
   * Sets the value of the 'ranking' field.
   * @param value the value to set.
   */
  public void setRanking(java.lang.Integer value) {
    this.ranking = value;
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public sortavro.record.Record4Float getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(sortavro.record.Record4Float value) {
    this.value = value;
  }

  /**
   * Creates a new RankedRecords4Float RecordBuilder.
   * @return A new RankedRecords4Float RecordBuilder
   */
  public static sortavro.record.RankedRecords4Float.Builder newBuilder() {
    return new sortavro.record.RankedRecords4Float.Builder();
  }

  /**
   * Creates a new RankedRecords4Float RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RankedRecords4Float RecordBuilder
   */
  public static sortavro.record.RankedRecords4Float.Builder newBuilder(sortavro.record.RankedRecords4Float.Builder other) {
    return new sortavro.record.RankedRecords4Float.Builder(other);
  }

  /**
   * Creates a new RankedRecords4Float RecordBuilder by copying an existing RankedRecords4Float instance.
   * @param other The existing instance to copy.
   * @return A new RankedRecords4Float RecordBuilder
   */
  public static sortavro.record.RankedRecords4Float.Builder newBuilder(sortavro.record.RankedRecords4Float other) {
    return new sortavro.record.RankedRecords4Float.Builder(other);
  }

  /**
   * RecordBuilder for RankedRecords4Float instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RankedRecords4Float>
    implements org.apache.avro.data.RecordBuilder<RankedRecords4Float> {

    private int ranking;
    private sortavro.record.Record4Float value;
    private sortavro.record.Record4Float.Builder valueBuilder;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(sortavro.record.RankedRecords4Float.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.ranking)) {
        this.ranking = data().deepCopy(fields()[0].schema(), other.ranking);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
      if (other.hasValueBuilder()) {
        this.valueBuilder = sortavro.record.Record4Float.newBuilder(other.getValueBuilder());
      }
    }

    /**
     * Creates a Builder by copying an existing RankedRecords4Float instance
     * @param other The existing instance to copy.
     */
    private Builder(sortavro.record.RankedRecords4Float other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.ranking)) {
        this.ranking = data().deepCopy(fields()[0].schema(), other.ranking);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
      this.valueBuilder = null;
    }

    /**
      * Gets the value of the 'ranking' field.
      * @return The value.
      */
    public java.lang.Integer getRanking() {
      return ranking;
    }

    /**
      * Sets the value of the 'ranking' field.
      * @param value The value of 'ranking'.
      * @return This builder.
      */
    public sortavro.record.RankedRecords4Float.Builder setRanking(int value) {
      validate(fields()[0], value);
      this.ranking = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'ranking' field has been set.
      * @return True if the 'ranking' field has been set, false otherwise.
      */
    public boolean hasRanking() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'ranking' field.
      * @return This builder.
      */
    public sortavro.record.RankedRecords4Float.Builder clearRanking() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public sortavro.record.Record4Float getValue() {
      return value;
    }

    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public sortavro.record.RankedRecords4Float.Builder setValue(sortavro.record.Record4Float value) {
      validate(fields()[1], value);
      this.valueBuilder = null;
      this.value = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[1];
    }

    /**
     * Gets the Builder instance for the 'value' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public sortavro.record.Record4Float.Builder getValueBuilder() {
      if (valueBuilder == null) {
        if (hasValue()) {
          setValueBuilder(sortavro.record.Record4Float.newBuilder(value));
        } else {
          setValueBuilder(sortavro.record.Record4Float.newBuilder());
        }
      }
      return valueBuilder;
    }

    /**
     * Sets the Builder instance for the 'value' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public sortavro.record.RankedRecords4Float.Builder setValueBuilder(sortavro.record.Record4Float.Builder value) {
      clearValue();
      valueBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'value' field has an active Builder instance
     * @return True if the 'value' field has an active Builder instance
     */
    public boolean hasValueBuilder() {
      return valueBuilder != null;
    }

    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public sortavro.record.RankedRecords4Float.Builder clearValue() {
      value = null;
      valueBuilder = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public RankedRecords4Float build() {
      try {
        RankedRecords4Float record = new RankedRecords4Float();
        record.ranking = fieldSetFlags()[0] ? this.ranking : (java.lang.Integer) defaultValue(fields()[0]);
        if (valueBuilder != null) {
          record.value = this.valueBuilder.build();
        } else {
          record.value = fieldSetFlags()[1] ? this.value : (sortavro.record.Record4Float) defaultValue(fields()[1]);
        }
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
