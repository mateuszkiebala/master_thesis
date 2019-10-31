package minimal_algorithms.hadoop.statistics;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

public class StatisticsRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema statisticsAggregatorSchema, Schema baseSchema) {
        SCHEMA$ = SchemaBuilder.record("StatisticsRecord")
                .namespace("minimal_algorithms.hadoop.statistics")
                .fields()
                .name("statisticsAggregator").type(statisticsAggregatorSchema).noDefault()
                .name("record").type(baseSchema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static StatisticsRecord deepCopy(StatisticsRecord record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static StatisticsRecord deepCopy(Schema recordSchema, StatisticsRecord record) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    private StatisticsAggregator statisticsAggregator;
    private GenericRecord record;

    public StatisticsRecord() {}

    public StatisticsRecord(StatisticsAggregator statisticsAggregator, GenericRecord record) {
        this.statisticsAggregator = statisticsAggregator;
        this.record = record;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticsAggregator;
            case 1: return record;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticsAggregator = (StatisticsAggregator)value$; break;
            case 1: record = (GenericRecord)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public StatisticsAggregator getStatisticsAggregator() {
        return statisticsAggregator;
    }

    public void setStatisticsAggregator(StatisticsAggregator statisticsAggregator) {
        this.statisticsAggregator = statisticsAggregator;
    }

    public GenericRecord getRecord() {
        return record;
    }

    public void setRecord(GenericRecord record) {
        this.record = record;
    }
}
