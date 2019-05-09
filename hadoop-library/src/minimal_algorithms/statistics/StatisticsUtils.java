package minimal_algorithms.statistics;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.AvroRuntimeException;
import minimal_algorithms.avro_types.statistics.StatisticsAggregator;
import minimal_algorithms.avro_types.statistics.StatisticsRecord;

class StatisticsException extends Exception {
    public StatisticsException(String message) {
        super(message);
    }
};

public class StatisticsUtils {
    private Schema statisticsAggregatorSchema;

    public StatisticsUtils(Schema statisticsAggregatorSchema) {
        this.statisticsAggregatorSchema = statisticsAggregatorSchema;
    }

    public List<StatisticsAggregator> scanLeftAggregators(List<GenericRecord> aggregators) {
        return scanLeftAggregators(aggregators, null);
    }

    public List<StatisticsAggregator> scanLeftAggregators(List<GenericRecord> aggregators, StatisticsAggregator start) {
        List<StatisticsAggregator> result = new ArrayList<>();
        StatisticsAggregator statsMerger = start;
        System.out.println(aggregators);
        try {
            for (GenericRecord record : aggregators) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) SpecificData.get().deepCopy(statisticsAggregatorSchema, record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                result.add(SpecificData.get().deepCopy(statisticsAggregatorSchema, statsMerger));
            }
        } catch (Exception e) {
            System.err.println("Cannot perform foldLeftAggregators: " + e.toString());
        }
        return result;
    }

    public List<StatisticsAggregator> scanLeftRecords(List<GenericRecord> records) {
        return scanLeftRecords(records, null);
    }

    public List<StatisticsAggregator> scanLeftRecords(List<GenericRecord> records, StatisticsAggregator start) {
        List<StatisticsAggregator> result = new ArrayList<>();
        StatisticsAggregator statsMerger = start;
        try {
            Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
            for (GenericRecord record : records) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                statisticsAggregator.init(record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                result.add(SpecificData.get().deepCopy(statisticsAggregatorSchema, statsMerger));
            }
        } catch (Exception e) {
            System.err.println("Cannot perform foldLeftRecords: " + e.toString());
        }
        return result;
    }

    public StatisticsAggregator foldLeftAggregators(List<GenericRecord> aggregators) {
        return foldLeftAggregators(aggregators, null);
    }

    public StatisticsAggregator foldLeftAggregators(List<GenericRecord> aggregators, StatisticsAggregator start) {
        StatisticsAggregator statsMerger = start;
        try {
            for (GenericRecord record : aggregators) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) SpecificData.get().deepCopy(statisticsAggregatorSchema, record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
            }
        } catch (Exception e) {
            System.err.println("Cannot perform foldLeftAggregators: " + e.toString());
        }
        return statsMerger;
    }

    public StatisticsAggregator foldLeftRecords(List<GenericRecord> records) {
        return foldLeftRecords(records, null);
    }

    public StatisticsAggregator foldLeftRecords(List<GenericRecord> records, StatisticsAggregator start) {
        StatisticsAggregator statsMerger = start;
        try {
            Class statisticsAggregatorClass = SpecificData.get().getClass(statisticsAggregatorSchema);
            for (GenericRecord record : records) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) statisticsAggregatorClass.newInstance();
                statisticsAggregator.init(record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
            }
        } catch (Exception e) {
            System.err.println("Cannot perform foldLeftRecords: " + e.toString());
        }
        return statsMerger;
    }

    public List<StatisticsRecord> zip(List<StatisticsAggregator> aggregators, List<GenericRecord> records) {
        if (aggregators.size() != records.size()) {
            throw new AvroRuntimeException("Aggregators length (" + aggregators.size() + ") doesn't equal records length (" + records.size() + ")");
        }

        List<StatisticsRecord> result = new ArrayList<>();
        Iterator<StatisticsAggregator> itA = aggregators.iterator();
        Iterator<GenericRecord> itR = records.iterator();
        while (itA.hasNext() && itR.hasNext()) {
            result.add(new StatisticsRecord(SpecificData.get().deepCopy(statisticsAggregatorSchema, itA.next()), itR.next()));
        }
        return result;
    }
}
