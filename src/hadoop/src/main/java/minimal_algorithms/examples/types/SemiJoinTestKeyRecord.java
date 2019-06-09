package minimal_algorithms.hadoop.examples.types;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.hadoop.utils.KeyRecord;
import java.util.Comparator;

public class SemiJoinTestKeyRecord extends KeyRecord<SemiJoinTestKeyRecord> {

    public static final Schema SCHEMA$ = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SemiJoinTestKeyRecord\",\"namespace\":\"minimal_algorithms.hadoop.examples.types\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}");
    public static Schema getClassSchema() { return SCHEMA$; }

    private GenericRecord objectRecord;
    private int value;

    public SemiJoinTestKeyRecord() {}

    public SemiJoinTestKeyRecord(int value) {
        this.value = value;
    }

    @Override
    public void init(GenericRecord record) {
        this.objectRecord = record;
        this.value = Math.round(((SemiJoinTestRecord) record).getRecord().getFirst());
    }

    @Override
    public int compare(SemiJoinTestKeyRecord that) {
        if (this.value > that.getValue())
            return 1;
        else if (this.value == that.getValue())
            return 0;
        else
            return -1;
    }

    public Object get(int field$) {
        switch (field$) {
            case 0: return value;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: value = (Integer)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof SemiJoinTestKeyRecord)) {
            return false;
        }

        return this.value == ((SemiJoinTestKeyRecord) o).getValue();
    }

    @Override
    public int hashCode() {
        return this.value;
    }

    @Override
    public String toString() {
        return ((Integer)this.value).toString();
    }

    public Schema getSchema() { return SCHEMA$; }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
