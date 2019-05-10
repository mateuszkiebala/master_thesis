package minimal_algorithms.avro_types.group_by;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import minimal_algorithms.avro_types.statistics.StatisticsAggregator;
import minimal_algorithms.avro_types.utils.KeyRecord;

public class GroupByRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public static Schema SCHEMA$;

    public static void setSchema(Schema statisticsAggregatorSchema, Schema keySchema) {
        SCHEMA$ = SchemaBuilder.record("GroupByRecord")
                .namespace("minimal_algorithms.avro_types.group_by")
                .fields()
                .name("statisticsAggregator").type(statisticsAggregatorSchema).noDefault()
                .name("key").type(keySchema).noDefault()
                .endRecord();
    }

    public static Schema getClassSchema() { return SCHEMA$; }

    public static GroupByRecord deepCopy(GroupByRecord record) {
        return SpecificData.get().deepCopy(getClassSchema(), record);
    }

    public static GroupByRecord deepCopy(GroupByRecord record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }

    private StatisticsAggregator statisticsAggregator;
    private KeyRecord key;

    public GroupByRecord() {}

    public GroupByRecord(StatisticsAggregator statisticsAggregator, KeyRecord key) {
        this.statisticsAggregator = statisticsAggregator;
        this.key = key;
    }

    public Schema getSchema() { return SCHEMA$; }

    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return statisticsAggregator;
            case 1: return key;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: statisticsAggregator = (StatisticsAggregator)value$; break;
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

    public StatisticsAggregator getStatisticsAggregator() {
        return statisticsAggregator;
    }

    public void setStatisticsAggregator(StatisticsAggregator statisticsAggregator) {
        this.statisticsAggregator = statisticsAggregator;
    }
}
