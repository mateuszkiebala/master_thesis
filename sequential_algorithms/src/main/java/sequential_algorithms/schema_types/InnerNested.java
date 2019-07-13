/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package sequential_algorithms.schema_types;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class InnerNested extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6656537815130739218L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"InnerNested\",\"namespace\":\"sequential_algorithms.schema_types\",\"fields\":[{\"name\":\"inner_int\",\"type\":\"int\"},{\"name\":\"inner_string\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int inner_int;
  @Deprecated public java.lang.String inner_string;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public InnerNested() {}

  /**
   * All-args constructor.
   * @param inner_int The new value for inner_int
   * @param inner_string The new value for inner_string
   */
  public InnerNested(java.lang.Integer inner_int, java.lang.String inner_string) {
    this.inner_int = inner_int;
    this.inner_string = inner_string;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return inner_int;
    case 1: return inner_string;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: inner_int = (java.lang.Integer)value$; break;
    case 1: inner_string = (java.lang.String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'inner_int' field.
   * @return The value of the 'inner_int' field.
   */
  public java.lang.Integer getInnerInt() {
    return inner_int;
  }

  /**
   * Sets the value of the 'inner_int' field.
   * @param value the value to set.
   */
  public void setInnerInt(java.lang.Integer value) {
    this.inner_int = value;
  }

  /**
   * Gets the value of the 'inner_string' field.
   * @return The value of the 'inner_string' field.
   */
  public java.lang.String getInnerString() {
    return inner_string;
  }

  /**
   * Sets the value of the 'inner_string' field.
   * @param value the value to set.
   */
  public void setInnerString(java.lang.String value) {
    this.inner_string = value;
  }

  /**
   * Creates a new InnerNested RecordBuilder.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.InnerNested.Builder newBuilder() {
    return new sequential_algorithms.schema_types.InnerNested.Builder();
  }

  /**
   * Creates a new InnerNested RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.InnerNested.Builder newBuilder(sequential_algorithms.schema_types.InnerNested.Builder other) {
    return new sequential_algorithms.schema_types.InnerNested.Builder(other);
  }

  /**
   * Creates a new InnerNested RecordBuilder by copying an existing InnerNested instance.
   * @param other The existing instance to copy.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.schema_types.InnerNested.Builder newBuilder(sequential_algorithms.schema_types.InnerNested other) {
    return new sequential_algorithms.schema_types.InnerNested.Builder(other);
  }

  /**
   * RecordBuilder for InnerNested instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<InnerNested>
    implements org.apache.avro.data.RecordBuilder<InnerNested> {

    private int inner_int;
    private java.lang.String inner_string;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(sequential_algorithms.schema_types.InnerNested.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.inner_int)) {
        this.inner_int = data().deepCopy(fields()[0].schema(), other.inner_int);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.inner_string)) {
        this.inner_string = data().deepCopy(fields()[1].schema(), other.inner_string);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing InnerNested instance
     * @param other The existing instance to copy.
     */
    private Builder(sequential_algorithms.schema_types.InnerNested other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.inner_int)) {
        this.inner_int = data().deepCopy(fields()[0].schema(), other.inner_int);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.inner_string)) {
        this.inner_string = data().deepCopy(fields()[1].schema(), other.inner_string);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'inner_int' field.
      * @return The value.
      */
    public java.lang.Integer getInnerInt() {
      return inner_int;
    }

    /**
      * Sets the value of the 'inner_int' field.
      * @param value The value of 'inner_int'.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.InnerNested.Builder setInnerInt(int value) {
      validate(fields()[0], value);
      this.inner_int = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'inner_int' field has been set.
      * @return True if the 'inner_int' field has been set, false otherwise.
      */
    public boolean hasInnerInt() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'inner_int' field.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.InnerNested.Builder clearInnerInt() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'inner_string' field.
      * @return The value.
      */
    public java.lang.String getInnerString() {
      return inner_string;
    }

    /**
      * Sets the value of the 'inner_string' field.
      * @param value The value of 'inner_string'.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.InnerNested.Builder setInnerString(java.lang.String value) {
      validate(fields()[1], value);
      this.inner_string = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'inner_string' field has been set.
      * @return True if the 'inner_string' field has been set, false otherwise.
      */
    public boolean hasInnerString() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'inner_string' field.
      * @return This builder.
      */
    public sequential_algorithms.schema_types.InnerNested.Builder clearInnerString() {
      inner_string = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public InnerNested build() {
      try {
        InnerNested record = new InnerNested();
        record.inner_int = fieldSetFlags()[0] ? this.inner_int : (java.lang.Integer) defaultValue(fields()[0]);
        record.inner_string = fieldSetFlags()[1] ? this.inner_string : (java.lang.String) defaultValue(fields()[1]);
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
