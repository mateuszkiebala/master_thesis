/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package sequential_algorithms.schema_types;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class MiddleNested extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -7766724523516068314L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MiddleNested\",\"namespace\":\"sequential_algorithms.schema_types\",\"fields\":[{\"name\":\"middle_array\",\"type\":{\"type\":\"array\",\"items\":\"float\"}},{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"InnerNested\",\"fields\":[{\"name\":\"inner_int\",\"type\":\"int\"},{\"name\":\"inner_string\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.util.List<java.lang.Float> middle_array;
  @Deprecated public sequential_algorithms.schema_types.InnerNested inner;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public MiddleNested() {}

  /**
   * All-args constructor.
   * @param middle_array The new value for middle_array
   * @param inner The new value for inner
   */
  public MiddleNested(java.util.List<java.lang.Float> middle_array, sequential_algorithms.schema_types.InnerNested inner) {
    this.middle_array = middle_array;
    this.inner = inner;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return middle_array;
    case 1: return inner;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: middle_array = (java.util.List<java.lang.Float>)value$; break;
    case 1: inner = (sequential_algorithms.schema_types.InnerNested)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'middle_array' field.
   * @return The value of the 'middle_array' field.
   */
  public java.util.List<java.lang.Float> getMiddleArray() {
    return middle_array;
  }

  /**
   * Sets the value of the 'middle_array' field.
   * @param value the value to set.
   */
  public void setMiddleArray(java.util.List<java.lang.Float> value) {
    this.middle_array = value;
  }

  /**
   * Gets the value of the 'inner' field.
   * @return The value of the 'inner' field.
   */
  public sequential_algorithms.schema_types.InnerNested getInner() {
    return inner;
  }

  /**
   * Sets the value of the 'inner' field.
   * @param value the value to set.
   */
  public void setInner(sequential_algorithms.schema_types.InnerNested value) {
    this.inner = value;
  }

  /**
   * Creates a new MiddleNested RecordBuilder.
   * @return A new MiddleNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.MiddleNested.Builder newBuilder() {
    return new sequential_algorithms.schema_types.MiddleNested.Builder();
  }

  /**
   * Creates a new MiddleNested RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new MiddleNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.MiddleNested.Builder newBuilder(sequential_algorithms.schema_types.MiddleNested.Builder other) {
    return new sequential_algorithms.schema_types.MiddleNested.Builder(other);
  }

  /**
   * Creates a new MiddleNested RecordBuilder by copying an existing MiddleNested instance.
   * @param other The existing instance to copy.
   * @return A new MiddleNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.MiddleNested.Builder newBuilder(sequential_algorithms.schema_types.MiddleNested other) {
    return new sequential_algorithms.schema_types.MiddleNested.Builder(other);
  }

  /**
   * RecordBuilder for MiddleNested instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MiddleNested>
    implements org.apache.avro.data.RecordBuilder<MiddleNested> {

    private java.util.List<java.lang.Float> middle_array;
    private sequential_algorithms.schema_types.InnerNested inner;
    private sequential_algorithms.schema_types.InnerNested.Builder innerBuilder;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(sequential_algorithms.schema_types.MiddleNested.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.middle_array)) {
        this.middle_array = data().deepCopy(fields()[0].schema(), other.middle_array);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.inner)) {
        this.inner = data().deepCopy(fields()[1].schema(), other.inner);
        fieldSetFlags()[1] = true;
      }
      if (other.hasInnerBuilder()) {
        this.innerBuilder = sequential_algorithms.schema_types.InnerNested.newBuilder(other.getInnerBuilder());
      }
    }

    /**
     * Creates a Builder by copying an existing MiddleNested instance
     * @param other The existing instance to copy.
     */
    private Builder(sequential_algorithms.schema_types.MiddleNested other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.middle_array)) {
        this.middle_array = data().deepCopy(fields()[0].schema(), other.middle_array);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.inner)) {
        this.inner = data().deepCopy(fields()[1].schema(), other.inner);
        fieldSetFlags()[1] = true;
      }
      this.innerBuilder = null;
    }

    /**
      * Gets the value of the 'middle_array' field.
      * @return The value.
      */
    public java.util.List<java.lang.Float> getMiddleArray() {
      return middle_array;
    }

    /**
      * Sets the value of the 'middle_array' field.
      * @param value The value of 'middle_array'.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.MiddleNested.Builder setMiddleArray(java.util.List<java.lang.Float> value) {
      validate(fields()[0], value);
      this.middle_array = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'middle_array' field has been set.
      * @return True if the 'middle_array' field has been set, false otherwise.
      */
    public boolean hasMiddleArray() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'middle_array' field.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.MiddleNested.Builder clearMiddleArray() {
      middle_array = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'inner' field.
      * @return The value.
      */
    public sequential_algorithms.schema_types.InnerNested getInner() {
      return inner;
    }

    /**
      * Sets the value of the 'inner' field.
      * @param value The value of 'inner'.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.MiddleNested.Builder setInner(sequential_algorithms.schema_types.InnerNested value) {
      validate(fields()[1], value);
      this.innerBuilder = null;
      this.inner = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'inner' field has been set.
      * @return True if the 'inner' field has been set, false otherwise.
      */
    public boolean hasInner() {
      return fieldSetFlags()[1];
    }

    /**
     * Gets the Builder instance for the 'inner' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public sequential_algorithms.schema_types.InnerNested.Builder getInnerBuilder() {
      if (innerBuilder == null) {
        if (hasInner()) {
          setInnerBuilder(sequential_algorithms.schema_types.InnerNested.newBuilder(inner));
        } else {
          setInnerBuilder(sequential_algorithms.schema_types.InnerNested.newBuilder());
        }
      }
      return innerBuilder;
    }

    /**
     * Sets the Builder instance for the 'inner' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public sequential_algorithms.schema_types.MiddleNested.Builder setInnerBuilder(sequential_algorithms.schema_types.InnerNested.Builder value) {
      clearInner();
      innerBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'inner' field has an active Builder instance
     * @return True if the 'inner' field has an active Builder instance
     */
    public boolean hasInnerBuilder() {
      return innerBuilder != null;
    }

    /**
      * Clears the value of the 'inner' field.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.MiddleNested.Builder clearInner() {
      inner = null;
      innerBuilder = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public MiddleNested build() {
      try {
        MiddleNested record = new MiddleNested();
        record.middle_array = fieldSetFlags()[0] ? this.middle_array : (java.util.List<java.lang.Float>) defaultValue(fields()[0]);
        if (innerBuilder != null) {
          record.inner = this.innerBuilder.build();
        } else {
          record.inner = fieldSetFlags()[1] ? this.inner : (sequential_algorithms.schema_types.InnerNested) defaultValue(fields()[1]);
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
