package minimal_algorithms.avro_types.statistics;

import org.apache.avro.generic.GenericRecord;

public abstract class StatisticsAggregator extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public abstract void create(GenericRecord record);
    public abstract StatisticsAggregator merge(StatisticsAggregator that);
}
