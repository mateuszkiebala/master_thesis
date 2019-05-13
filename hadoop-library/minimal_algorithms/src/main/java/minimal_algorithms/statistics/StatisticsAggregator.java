package minimal_algorithms.statistics;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.generic.GenericRecord;

public abstract class StatisticsAggregator extends SpecificRecordBase implements SpecificRecord {
    public abstract void init(GenericRecord record);
    public abstract StatisticsAggregator merge(StatisticsAggregator that);

    public static StatisticsAggregator safeMerge(StatisticsAggregator a, StatisticsAggregator b) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else {
            return a.merge(b);
        }
    }

    public static StatisticsAggregator create(Schema statisticsAggregatorSchema, GenericRecord record) {
        StatisticsAggregator statisticsAggregator = null;
        try {
            Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
            statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
            statisticsAggregator.init(record);
        } catch (Exception e) {
            System.err.println("Cannot create StatisticsAggreagtor from schema and record: " + e.toString());
        }
        return statisticsAggregator;
    }

    public static StatisticsAggregator deepCopy(GenericRecord record, Schema recordSchema) {
        return (StatisticsAggregator) SpecificData.get().deepCopy(recordSchema, record);
    }

    public static StatisticsAggregator deepCopy(StatisticsAggregator record, Schema recordSchema) {
        return SpecificData.get().deepCopy(recordSchema, record);
    }
}
