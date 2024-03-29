/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package sequential_algorithms.types;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class InnerNested extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -6563696797645470390L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"InnerNested\",\"namespace\":\"sequential_algorithms.types\",\"fields\":[{\"name\":\"inner_int\",\"type\":\"int\"},{\"name\":\"inner_string\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<InnerNested> ENCODER =
      new BinaryMessageEncoder<InnerNested>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<InnerNested> DECODER =
      new BinaryMessageDecoder<InnerNested>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<InnerNested> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<InnerNested> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<InnerNested> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<InnerNested>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this InnerNested to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a InnerNested from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a InnerNested instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static InnerNested fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public int inner_int;
  @Deprecated public java.lang.CharSequence inner_string;

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
  public InnerNested(java.lang.Integer inner_int, java.lang.CharSequence inner_string) {
    this.inner_int = inner_int;
    this.inner_string = inner_string;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
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
    case 1: inner_string = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'inner_int' field.
   * @return The value of the 'inner_int' field.
   */
  public int getInnerInt() {
    return inner_int;
  }


  /**
   * Sets the value of the 'inner_int' field.
   * @param value the value to set.
   */
  public void setInnerInt(int value) {
    this.inner_int = value;
  }

  /**
   * Gets the value of the 'inner_string' field.
   * @return The value of the 'inner_string' field.
   */
  public java.lang.CharSequence getInnerString() {
    return inner_string;
  }


  /**
   * Sets the value of the 'inner_string' field.
   * @param value the value to set.
   */
  public void setInnerString(java.lang.CharSequence value) {
    this.inner_string = value;
  }

  /**
   * Creates a new InnerNested RecordBuilder.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.types.InnerNested.Builder newBuilder() {
    return new sequential_algorithms.types.InnerNested.Builder();
  }

  /**
   * Creates a new InnerNested RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.types.InnerNested.Builder newBuilder(sequential_algorithms.types.InnerNested.Builder other) {
    if (other == null) {
      return new sequential_algorithms.types.InnerNested.Builder();
    } else {
      return new sequential_algorithms.types.InnerNested.Builder(other);
    }
  }

  /**
   * Creates a new InnerNested RecordBuilder by copying an existing InnerNested instance.
   * @param other The existing instance to copy.
   * @return A new InnerNested RecordBuilder
   */
  public static sequential_algorithms.types.InnerNested.Builder newBuilder(sequential_algorithms.types.InnerNested other) {
    if (other == null) {
      return new sequential_algorithms.types.InnerNested.Builder();
    } else {
      return new sequential_algorithms.types.InnerNested.Builder(other);
    }
  }

  /**
   * RecordBuilder for InnerNested instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<InnerNested>
    implements org.apache.avro.data.RecordBuilder<InnerNested> {

    private int inner_int;
    private java.lang.CharSequence inner_string;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(sequential_algorithms.types.InnerNested.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.inner_int)) {
        this.inner_int = data().deepCopy(fields()[0].schema(), other.inner_int);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.inner_string)) {
        this.inner_string = data().deepCopy(fields()[1].schema(), other.inner_string);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing InnerNested instance
     * @param other The existing instance to copy.
     */
    private Builder(sequential_algorithms.types.InnerNested other) {
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
    public int getInnerInt() {
      return inner_int;
    }


    /**
      * Sets the value of the 'inner_int' field.
      * @param value The value of 'inner_int'.
      * @return This builder.
      */
    public sequential_algorithms.types.InnerNested.Builder setInnerInt(int value) {
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
    public sequential_algorithms.types.InnerNested.Builder clearInnerInt() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'inner_string' field.
      * @return The value.
      */
    public java.lang.CharSequence getInnerString() {
      return inner_string;
    }


    /**
      * Sets the value of the 'inner_string' field.
      * @param value The value of 'inner_string'.
      * @return This builder.
      */
    public sequential_algorithms.types.InnerNested.Builder setInnerString(java.lang.CharSequence value) {
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
    public sequential_algorithms.types.InnerNested.Builder clearInnerString() {
      inner_string = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public InnerNested build() {
      try {
        InnerNested record = new InnerNested();
        record.inner_int = fieldSetFlags()[0] ? this.inner_int : (java.lang.Integer) defaultValue(fields()[0]);
        record.inner_string = fieldSetFlags()[1] ? this.inner_string : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<InnerNested>
    WRITER$ = (org.apache.avro.io.DatumWriter<InnerNested>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<InnerNested>
    READER$ = (org.apache.avro.io.DatumReader<InnerNested>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeInt(this.inner_int);

    out.writeString(this.inner_string);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.inner_int = in.readInt();

      this.inner_string = in.readString(this.inner_string instanceof Utf8 ? (Utf8)this.inner_string : null);

    } else {
      for (int i = 0; i < 2; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.inner_int = in.readInt();
          break;

        case 1:
          this.inner_string = in.readString(this.inner_string instanceof Utf8 ? (Utf8)this.inner_string : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










