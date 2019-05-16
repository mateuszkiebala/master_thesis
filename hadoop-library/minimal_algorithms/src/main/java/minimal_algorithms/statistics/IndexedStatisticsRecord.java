package minimal_algorithms.statistics;

import java.util.Comparator;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericRecord;

public class IndexedStatisticsRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema schema) {
        SCHEMA$ = SchemaBuilder.record("IndexedStatisticsRecord").namespace("minimal_algorithms.statistics")
                .fields()
                .name("index").type().longType().noDefault()
                .name("record").type(schema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static IndexedStatisticsRecord deepCopy(IndexedStatisticsRecord record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static IndexedStatisticsRecord deepCopy(IndexedStatisticsRecord record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    public static Comparator<IndexedStatisticsRecord> cmp = new IndexedStatisticsRecordComparator();

    public static class IndexedStatisticsRecordComparator implements Comparator<IndexedStatisticsRecord> {
        @Override
        public int compare(IndexedStatisticsRecord o1, IndexedStatisticsRecord o2) {
            return o1.getIndex() > o2.getIndex() ? 1 : (o1.getIndex() < o2.getIndex() ? -1 : 0);
        }
    }

    public static Comparator<GenericRecord> genericRecordCmp = new GenericRecordComparator();

    public static class GenericRecordComparator implements Comparator<GenericRecord> {
        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
            IndexedStatisticsRecord rec1 = (IndexedStatisticsRecord) o1;
            IndexedStatisticsRecord rec2 = (IndexedStatisticsRecord) o2;
            return rec1.getIndex() > rec2.getIndex() ? 1 : (rec1.getIndex() < rec2.getIndex() ? -1 : 0);
        }
    }

    private long index;
    private GenericRecord record;

    public IndexedStatisticsRecord() {}

    public IndexedStatisticsRecord(java.lang.Long index, GenericRecord record) {
        this.index = index;
        this.record = record;
    }

    public Schema getSchema() { return SCHEMA$; }

    public Object get(int field$) {
        switch (field$) {
            case 0: return index;
            case 1: return record;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
            case 0: index = (Long)value$; break;
            case 1: record = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long record) {
        this.index = record;
    }

    public GenericRecord getRecord() {
        return record;
    }

    public void setRecord(GenericRecord record) {
        this.record = record;
    }
}
