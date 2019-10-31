package minimal_algorithms.hadoop.examples.types;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import minimal_algorithms.hadoop.semi_join.SemiJoinRecord;

public class SemiJoinTestRecord extends SemiJoinRecord {
    public static Schema SCHEMA$ = SchemaBuilder.record("SemiJoinTestRecord")
                .namespace("minimal_algorithms.hadoop.examples.types")
                .fields()
                .name("type").type().enumeration("type").symbols("R", "T").noDefault()
                .name("record").type(Record4Float.getClassSchema()).noDefault()
                .endRecord();

    public static Schema getClassSchema() { return SCHEMA$; }

    public static Comparator<SemiJoinTestRecord> cmp = new SemiJoinTestComparator();

    public static class SemiJoinTestComparator implements Comparator<SemiJoinTestRecord> {
        @Override
        public int compare(SemiJoinTestRecord o1, SemiJoinTestRecord o2) {
            return o1.getRecord().first > o2.getRecord().first ? 1 : (o1.getRecord().first < o2.getRecord().first ? -1 :
                    o1.getRecord().second > o2.getRecord().second ? 1 : (o1.getRecord().second < o2.getRecord().second ? -1 :
                            o1.getRecord().third > o2.getRecord().third ? 1 : (o1.getRecord().third < o2.getRecord().third ? -1 :
                                    o1.getRecord().fourth > o2.getRecord().fourth ? 1 : (o1.getRecord().fourth < o2.getRecord().fourth ? -1 : 0))));
        }
    }

    private Type type;
    private Record4Float record;

    public SemiJoinTestRecord() {}

    public SemiJoinTestRecord(Type type, Record4Float record) {
        this.type = type;
        this.record = record;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return type;
            case 1: return record;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: type = Type.valueOf((value$).toString()); break;
            case 1: record = (Record4Float)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Record4Float getRecord() {
        return record;
    }

    public void setRecord(Record4Float record) {
        this.record = record;
    }

    public void setType(Type type) {
        this.type = type;
    }
}
