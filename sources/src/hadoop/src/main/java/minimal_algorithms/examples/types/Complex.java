/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package minimal_algorithms.hadoop.examples.types;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Complex extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -6297084603788467154L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Complex\",\"namespace\":\"minimal_algorithms.hadoop.examples.types\",\"fields\":[{\"name\":\"null_prim\",\"type\":[\"null\",\"int\"]},{\"name\":\"boolean_prim\",\"type\":\"boolean\"},{\"name\":\"int_prim\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":-10,\"max\":10}}}},{\"name\":\"long_prim\",\"type\":\"long\"},{\"name\":\"float_prim\",\"type\":\"float\"},{\"name\":\"double_prim\",\"type\":\"double\"},{\"name\":\"string_prim\",\"type\":\"string\"},{\"name\":\"bytes_prim\",\"type\":\"bytes\"},{\"name\":\"middle\",\"type\":{\"type\":\"record\",\"name\":\"MiddleNested\",\"fields\":[{\"name\":\"middle_array\",\"type\":{\"type\":\"array\",\"items\":\"float\"}},{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"InnerNested\",\"fields\":[{\"name\":\"inner_int\",\"type\":\"int\"},{\"name\":\"inner_string\",\"type\":\"string\"}]}}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.Integer null_prim;
  @Deprecated public boolean boolean_prim;
  @Deprecated public int int_prim;
  @Deprecated public long long_prim;
  @Deprecated public float float_prim;
  @Deprecated public double double_prim;
  @Deprecated public java.lang.CharSequence string_prim;
  @Deprecated public java.nio.ByteBuffer bytes_prim;
  @Deprecated public minimal_algorithms.hadoop.examples.types.MiddleNested middle;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Complex() {}

  /**
   * All-args constructor.
   * @param null_prim The new value for null_prim
   * @param boolean_prim The new value for boolean_prim
   * @param int_prim The new value for int_prim
   * @param long_prim The new value for long_prim
   * @param float_prim The new value for float_prim
   * @param double_prim The new value for double_prim
   * @param string_prim The new value for string_prim
   * @param bytes_prim The new value for bytes_prim
   * @param middle The new value for middle
   */
  public Complex(java.lang.Integer null_prim, java.lang.Boolean boolean_prim, java.lang.Integer int_prim, java.lang.Long long_prim, java.lang.Float float_prim, java.lang.Double double_prim, java.lang.CharSequence string_prim, java.nio.ByteBuffer bytes_prim, minimal_algorithms.hadoop.examples.types.MiddleNested middle) {
    this.null_prim = null_prim;
    this.boolean_prim = boolean_prim;
    this.int_prim = int_prim;
    this.long_prim = long_prim;
    this.float_prim = float_prim;
    this.double_prim = double_prim;
    this.string_prim = string_prim;
    this.bytes_prim = bytes_prim;
    this.middle = middle;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return null_prim;
    case 1: return boolean_prim;
    case 2: return int_prim;
    case 3: return long_prim;
    case 4: return float_prim;
    case 5: return double_prim;
    case 6: return string_prim;
    case 7: return bytes_prim;
    case 8: return middle;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: null_prim = (java.lang.Integer)value$; break;
    case 1: boolean_prim = (java.lang.Boolean)value$; break;
    case 2: int_prim = (java.lang.Integer)value$; break;
    case 3: long_prim = (java.lang.Long)value$; break;
    case 4: float_prim = (java.lang.Float)value$; break;
    case 5: double_prim = (java.lang.Double)value$; break;
    case 6: string_prim = (java.lang.CharSequence)value$; break;
    case 7: bytes_prim = (java.nio.ByteBuffer)value$; break;
    case 8: middle = (minimal_algorithms.hadoop.examples.types.MiddleNested)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'null_prim' field.
   * @return The value of the 'null_prim' field.
   */
  public java.lang.Integer getNullPrim() {
    return null_prim;
  }

  /**
   * Sets the value of the 'null_prim' field.
   * @param value the value to set.
   */
  public void setNullPrim(java.lang.Integer value) {
    this.null_prim = value;
  }

  /**
   * Gets the value of the 'boolean_prim' field.
   * @return The value of the 'boolean_prim' field.
   */
  public java.lang.Boolean getBooleanPrim() {
    return boolean_prim;
  }

  /**
   * Sets the value of the 'boolean_prim' field.
   * @param value the value to set.
   */
  public void setBooleanPrim(java.lang.Boolean value) {
    this.boolean_prim = value;
  }

  /**
   * Gets the value of the 'int_prim' field.
   * @return The value of the 'int_prim' field.
   */
  public java.lang.Integer getIntPrim() {
    return int_prim;
  }

  /**
   * Sets the value of the 'int_prim' field.
   * @param value the value to set.
   */
  public void setIntPrim(java.lang.Integer value) {
    this.int_prim = value;
  }

  /**
   * Gets the value of the 'long_prim' field.
   * @return The value of the 'long_prim' field.
   */
  public java.lang.Long getLongPrim() {
    return long_prim;
  }

  /**
   * Sets the value of the 'long_prim' field.
   * @param value the value to set.
   */
  public void setLongPrim(java.lang.Long value) {
    this.long_prim = value;
  }

  /**
   * Gets the value of the 'float_prim' field.
   * @return The value of the 'float_prim' field.
   */
  public java.lang.Float getFloatPrim() {
    return float_prim;
  }

  /**
   * Sets the value of the 'float_prim' field.
   * @param value the value to set.
   */
  public void setFloatPrim(java.lang.Float value) {
    this.float_prim = value;
  }

  /**
   * Gets the value of the 'double_prim' field.
   * @return The value of the 'double_prim' field.
   */
  public java.lang.Double getDoublePrim() {
    return double_prim;
  }

  /**
   * Sets the value of the 'double_prim' field.
   * @param value the value to set.
   */
  public void setDoublePrim(java.lang.Double value) {
    this.double_prim = value;
  }

  /**
   * Gets the value of the 'string_prim' field.
   * @return The value of the 'string_prim' field.
   */
  public java.lang.CharSequence getStringPrim() {
    return string_prim;
  }

  /**
   * Sets the value of the 'string_prim' field.
   * @param value the value to set.
   */
  public void setStringPrim(java.lang.CharSequence value) {
    this.string_prim = value;
  }

  /**
   * Gets the value of the 'bytes_prim' field.
   * @return The value of the 'bytes_prim' field.
   */
  public java.nio.ByteBuffer getBytesPrim() {
    return bytes_prim;
  }

  /**
   * Sets the value of the 'bytes_prim' field.
   * @param value the value to set.
   */
  public void setBytesPrim(java.nio.ByteBuffer value) {
    this.bytes_prim = value;
  }

  /**
   * Gets the value of the 'middle' field.
   * @return The value of the 'middle' field.
   */
  public minimal_algorithms.hadoop.examples.types.MiddleNested getMiddle() {
    return middle;
  }

  /**
   * Sets the value of the 'middle' field.
   * @param value the value to set.
   */
  public void setMiddle(minimal_algorithms.hadoop.examples.types.MiddleNested value) {
    this.middle = value;
  }

  /**
   * Creates a new Complex RecordBuilder.
   * @return A new Complex RecordBuilder
   */
  public static minimal_algorithms.hadoop.examples.types.Complex.Builder newBuilder() {
    return new minimal_algorithms.hadoop.examples.types.Complex.Builder();
  }

  /**
   * Creates a new Complex RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Complex RecordBuilder
   */
  public static minimal_algorithms.hadoop.examples.types.Complex.Builder newBuilder(minimal_algorithms.hadoop.examples.types.Complex.Builder other) {
    return new minimal_algorithms.hadoop.examples.types.Complex.Builder(other);
  }

  /**
   * Creates a new Complex RecordBuilder by copying an existing Complex instance.
   * @param other The existing instance to copy.
   * @return A new Complex RecordBuilder
   */
  public static minimal_algorithms.hadoop.examples.types.Complex.Builder newBuilder(minimal_algorithms.hadoop.examples.types.Complex other) {
    return new minimal_algorithms.hadoop.examples.types.Complex.Builder(other);
  }

  /**
   * RecordBuilder for Complex instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Complex>
    implements org.apache.avro.data.RecordBuilder<Complex> {

    private java.lang.Integer null_prim;
    private boolean boolean_prim;
    private int int_prim;
    private long long_prim;
    private float float_prim;
    private double double_prim;
    private java.lang.CharSequence string_prim;
    private java.nio.ByteBuffer bytes_prim;
    private minimal_algorithms.hadoop.examples.types.MiddleNested middle;
    private minimal_algorithms.hadoop.examples.types.MiddleNested.Builder middleBuilder;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(minimal_algorithms.hadoop.examples.types.Complex.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.null_prim)) {
        this.null_prim = data().deepCopy(fields()[0].schema(), other.null_prim);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.boolean_prim)) {
        this.boolean_prim = data().deepCopy(fields()[1].schema(), other.boolean_prim);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.int_prim)) {
        this.int_prim = data().deepCopy(fields()[2].schema(), other.int_prim);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.long_prim)) {
        this.long_prim = data().deepCopy(fields()[3].schema(), other.long_prim);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.float_prim)) {
        this.float_prim = data().deepCopy(fields()[4].schema(), other.float_prim);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.double_prim)) {
        this.double_prim = data().deepCopy(fields()[5].schema(), other.double_prim);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.string_prim)) {
        this.string_prim = data().deepCopy(fields()[6].schema(), other.string_prim);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.bytes_prim)) {
        this.bytes_prim = data().deepCopy(fields()[7].schema(), other.bytes_prim);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.middle)) {
        this.middle = data().deepCopy(fields()[8].schema(), other.middle);
        fieldSetFlags()[8] = true;
      }
      if (other.hasMiddleBuilder()) {
        this.middleBuilder = minimal_algorithms.hadoop.examples.types.MiddleNested.newBuilder(other.getMiddleBuilder());
      }
    }

    /**
     * Creates a Builder by copying an existing Complex instance
     * @param other The existing instance to copy.
     */
    private Builder(minimal_algorithms.hadoop.examples.types.Complex other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.null_prim)) {
        this.null_prim = data().deepCopy(fields()[0].schema(), other.null_prim);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.boolean_prim)) {
        this.boolean_prim = data().deepCopy(fields()[1].schema(), other.boolean_prim);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.int_prim)) {
        this.int_prim = data().deepCopy(fields()[2].schema(), other.int_prim);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.long_prim)) {
        this.long_prim = data().deepCopy(fields()[3].schema(), other.long_prim);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.float_prim)) {
        this.float_prim = data().deepCopy(fields()[4].schema(), other.float_prim);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.double_prim)) {
        this.double_prim = data().deepCopy(fields()[5].schema(), other.double_prim);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.string_prim)) {
        this.string_prim = data().deepCopy(fields()[6].schema(), other.string_prim);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.bytes_prim)) {
        this.bytes_prim = data().deepCopy(fields()[7].schema(), other.bytes_prim);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.middle)) {
        this.middle = data().deepCopy(fields()[8].schema(), other.middle);
        fieldSetFlags()[8] = true;
      }
      this.middleBuilder = null;
    }

    /**
      * Gets the value of the 'null_prim' field.
      * @return The value.
      */
    public java.lang.Integer getNullPrim() {
      return null_prim;
    }

    /**
      * Sets the value of the 'null_prim' field.
      * @param value The value of 'null_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setNullPrim(java.lang.Integer value) {
      validate(fields()[0], value);
      this.null_prim = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'null_prim' field has been set.
      * @return True if the 'null_prim' field has been set, false otherwise.
      */
    public boolean hasNullPrim() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'null_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearNullPrim() {
      null_prim = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'boolean_prim' field.
      * @return The value.
      */
    public java.lang.Boolean getBooleanPrim() {
      return boolean_prim;
    }

    /**
      * Sets the value of the 'boolean_prim' field.
      * @param value The value of 'boolean_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setBooleanPrim(boolean value) {
      validate(fields()[1], value);
      this.boolean_prim = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'boolean_prim' field has been set.
      * @return True if the 'boolean_prim' field has been set, false otherwise.
      */
    public boolean hasBooleanPrim() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'boolean_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearBooleanPrim() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'int_prim' field.
      * @return The value.
      */
    public java.lang.Integer getIntPrim() {
      return int_prim;
    }

    /**
      * Sets the value of the 'int_prim' field.
      * @param value The value of 'int_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setIntPrim(int value) {
      validate(fields()[2], value);
      this.int_prim = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'int_prim' field has been set.
      * @return True if the 'int_prim' field has been set, false otherwise.
      */
    public boolean hasIntPrim() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'int_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearIntPrim() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'long_prim' field.
      * @return The value.
      */
    public java.lang.Long getLongPrim() {
      return long_prim;
    }

    /**
      * Sets the value of the 'long_prim' field.
      * @param value The value of 'long_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setLongPrim(long value) {
      validate(fields()[3], value);
      this.long_prim = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'long_prim' field has been set.
      * @return True if the 'long_prim' field has been set, false otherwise.
      */
    public boolean hasLongPrim() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'long_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearLongPrim() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'float_prim' field.
      * @return The value.
      */
    public java.lang.Float getFloatPrim() {
      return float_prim;
    }

    /**
      * Sets the value of the 'float_prim' field.
      * @param value The value of 'float_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setFloatPrim(float value) {
      validate(fields()[4], value);
      this.float_prim = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'float_prim' field has been set.
      * @return True if the 'float_prim' field has been set, false otherwise.
      */
    public boolean hasFloatPrim() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'float_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearFloatPrim() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'double_prim' field.
      * @return The value.
      */
    public java.lang.Double getDoublePrim() {
      return double_prim;
    }

    /**
      * Sets the value of the 'double_prim' field.
      * @param value The value of 'double_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setDoublePrim(double value) {
      validate(fields()[5], value);
      this.double_prim = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'double_prim' field has been set.
      * @return True if the 'double_prim' field has been set, false otherwise.
      */
    public boolean hasDoublePrim() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'double_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearDoublePrim() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'string_prim' field.
      * @return The value.
      */
    public java.lang.CharSequence getStringPrim() {
      return string_prim;
    }

    /**
      * Sets the value of the 'string_prim' field.
      * @param value The value of 'string_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setStringPrim(java.lang.CharSequence value) {
      validate(fields()[6], value);
      this.string_prim = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'string_prim' field has been set.
      * @return True if the 'string_prim' field has been set, false otherwise.
      */
    public boolean hasStringPrim() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'string_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearStringPrim() {
      string_prim = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'bytes_prim' field.
      * @return The value.
      */
    public java.nio.ByteBuffer getBytesPrim() {
      return bytes_prim;
    }

    /**
      * Sets the value of the 'bytes_prim' field.
      * @param value The value of 'bytes_prim'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setBytesPrim(java.nio.ByteBuffer value) {
      validate(fields()[7], value);
      this.bytes_prim = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'bytes_prim' field has been set.
      * @return True if the 'bytes_prim' field has been set, false otherwise.
      */
    public boolean hasBytesPrim() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'bytes_prim' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearBytesPrim() {
      bytes_prim = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /**
      * Gets the value of the 'middle' field.
      * @return The value.
      */
    public minimal_algorithms.hadoop.examples.types.MiddleNested getMiddle() {
      return middle;
    }

    /**
      * Sets the value of the 'middle' field.
      * @param value The value of 'middle'.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setMiddle(minimal_algorithms.hadoop.examples.types.MiddleNested value) {
      validate(fields()[8], value);
      this.middleBuilder = null;
      this.middle = value;
      fieldSetFlags()[8] = true;
      return this;
    }

    /**
      * Checks whether the 'middle' field has been set.
      * @return True if the 'middle' field has been set, false otherwise.
      */
    public boolean hasMiddle() {
      return fieldSetFlags()[8];
    }

    /**
     * Gets the Builder instance for the 'middle' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public minimal_algorithms.hadoop.examples.types.MiddleNested.Builder getMiddleBuilder() {
      if (middleBuilder == null) {
        if (hasMiddle()) {
          setMiddleBuilder(minimal_algorithms.hadoop.examples.types.MiddleNested.newBuilder(middle));
        } else {
          setMiddleBuilder(minimal_algorithms.hadoop.examples.types.MiddleNested.newBuilder());
        }
      }
      return middleBuilder;
    }

    /**
     * Sets the Builder instance for the 'middle' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder setMiddleBuilder(minimal_algorithms.hadoop.examples.types.MiddleNested.Builder value) {
      clearMiddle();
      middleBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'middle' field has an active Builder instance
     * @return True if the 'middle' field has an active Builder instance
     */
    public boolean hasMiddleBuilder() {
      return middleBuilder != null;
    }

    /**
      * Clears the value of the 'middle' field.
      * @return This builder.
      */
    public minimal_algorithms.hadoop.examples.types.Complex.Builder clearMiddle() {
      middle = null;
      middleBuilder = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    @Override
    public Complex build() {
      try {
        Complex record = new Complex();
        record.null_prim = fieldSetFlags()[0] ? this.null_prim : (java.lang.Integer) defaultValue(fields()[0]);
        record.boolean_prim = fieldSetFlags()[1] ? this.boolean_prim : (java.lang.Boolean) defaultValue(fields()[1]);
        record.int_prim = fieldSetFlags()[2] ? this.int_prim : (java.lang.Integer) defaultValue(fields()[2]);
        record.long_prim = fieldSetFlags()[3] ? this.long_prim : (java.lang.Long) defaultValue(fields()[3]);
        record.float_prim = fieldSetFlags()[4] ? this.float_prim : (java.lang.Float) defaultValue(fields()[4]);
        record.double_prim = fieldSetFlags()[5] ? this.double_prim : (java.lang.Double) defaultValue(fields()[5]);
        record.string_prim = fieldSetFlags()[6] ? this.string_prim : (java.lang.CharSequence) defaultValue(fields()[6]);
        record.bytes_prim = fieldSetFlags()[7] ? this.bytes_prim : (java.nio.ByteBuffer) defaultValue(fields()[7]);
        if (middleBuilder != null) {
          record.middle = this.middleBuilder.build();
        } else {
          record.middle = fieldSetFlags()[8] ? this.middle : (minimal_algorithms.hadoop.examples.types.MiddleNested) defaultValue(fields()[8]);
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
