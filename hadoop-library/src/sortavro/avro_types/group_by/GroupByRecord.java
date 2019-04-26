package sortavro.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import sortavro.avro_types.statistics.Statisticer;
import sortavro.avro_types.utils.KeyRecord;

public class GroupByRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema statisticerSchema, Schema keySchema) {
        SCHEMA$ = SchemaBuilder.record("GroupByRecord")
                .namespace("sortavro.avro_types.group_by")
                .fields()
                .name("statisticer").type(statisticerSchema).noDefault()
                .name("key").type(keySchema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    private Statisticer statisticer;
    private KeyRecord key;

    public GroupByRecord() {}

    public GroupByRecord(Statisticer statisticer, KeyRecord key) {
        this.statisticer = statisticer;
        this.key = key;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticer;
            case 1: return key;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticer = (Statisticer)value$; break;
            case 1: key = (KeyRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public KeyRecord getKey() {
        return key;
    }

    public void setKey(KeyRecord key) {
        this.key = key;
    }

    public Statisticer getStatisticer() {
        return statisticer;
    }

    public void setStatisticer(Statisticer statisticer) {
        this.statisticer = statisticer;
    }
}