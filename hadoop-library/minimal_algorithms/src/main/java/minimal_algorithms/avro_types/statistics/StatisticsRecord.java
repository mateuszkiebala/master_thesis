package minimal_algorithms.avro_types.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

public class StatisticsRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema statisticsAggregatorSchema, Schema mainObjectSchema) {
        SCHEMA$ = SchemaBuilder.record("StatisticsRecord")
                .namespace("sortavro.avro_types.statistics")
                .fields()
                .name("statisticsAggregator").type(statisticsAggregatorSchema).noDefault()
                .name("mainObject").type(mainObjectSchema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static StatisticsRecord deepCopy(StatisticsRecord record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static StatisticsRecord deepCopy(StatisticsRecord record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    private StatisticsAggregator statisticsAggregator;
    private GenericRecord mainObject;

    public StatisticsRecord() {}

    public StatisticsRecord(StatisticsAggregator statisticsAggregator, GenericRecord mainObject) {
        this.statisticsAggregator = statisticsAggregator;
        this.mainObject = mainObject;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticsAggregator;
            case 1: return mainObject;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticsAggregator = (StatisticsAggregator)value$; break;
            case 1: mainObject = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public StatisticsAggregator getStatisticsAggregator() {
        return statisticsAggregator;
    }

    public void setStatisticsAggregator(StatisticsAggregator statisticsAggregator) {
        this.statisticsAggregator = statisticsAggregator;
    }

    public GenericRecord getMainObject() {
        return mainObject;
    }

    public void setMainObject(GenericRecord mainObject) {
        this.mainObject = mainObject;
    }
}
