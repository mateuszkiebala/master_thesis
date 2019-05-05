package minimal_algorithms.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

public class StatisticsRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema statisticerSchema, Schema mainObjectSchema) {
        SCHEMA$ = SchemaBuilder.record("StatisticsRecord")
                .namespace("sortavro.avro_types.statistics")
                .fields()
                .name("statisticer").type(statisticerSchema).noDefault()
                .name("mainObject").type(mainObjectSchema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    private Statisticer statisticer;
    private GenericRecord mainObject;

    public StatisticsRecord() {}

    public StatisticsRecord(Statisticer statisticer, GenericRecord mainObject) {
        this.statisticer = statisticer;
        this.mainObject = mainObject;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticer;
            case 1: return mainObject;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticer = (Statisticer)value$; break;
            case 1: mainObject = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public Statisticer getStatisticer() {
        return statisticer;
    }

    public void setStatisticer(Statisticer statisticer) {
        this.statisticer = statisticer;
    }

    public GenericRecord getMainObject() {
        return mainObject;
    }

    public void setMainObject(GenericRecord mainObject) {
        this.mainObject = mainObject;
    }
}
