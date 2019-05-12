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
        if (aggregators != null) {
            StatisticsAggregator statsMerger = start;
            for (GenericRecord record : aggregators) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) record;
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                result.add(StatisticsAggregator.deepCopy(statsMerger, statisticsAggregatorSchema));
            }
        }
        return result;
    }

    public List<StatisticsAggregator> scanLeftRecords(List<GenericRecord> records) {
        return scanLeftRecords(records, null);
    }

    public List<StatisticsAggregator> scanLeftRecords(List<GenericRecord> records, StatisticsAggregator start) {
        List<StatisticsAggregator> result = new ArrayList<>();
        if (records != null) {
            StatisticsAggregator statsMerger = start;
            for (GenericRecord record : records) {
                StatisticsAggregator statisticsAggregator = StatisticsAggregator.create(statisticsAggregatorSchema, record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
                result.add(StatisticsAggregator.deepCopy(statsMerger, statisticsAggregatorSchema));
            }
        }
        return result;
    }

    public StatisticsAggregator foldLeftAggregators(List<GenericRecord> aggregators) {
        return foldLeftAggregators(aggregators, null);
    }

    public StatisticsAggregator foldLeftAggregators(List<GenericRecord> aggregators, StatisticsAggregator start) {
        StatisticsAggregator statsMerger = null;
        if (aggregators != null) {
            statsMerger = start;
            for (GenericRecord record : aggregators) {
                StatisticsAggregator statisticsAggregator = (StatisticsAggregator) record;
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
            }
        }
        return statsMerger;
    }

    public StatisticsAggregator foldLeftRecords(List<GenericRecord> records) {
        return foldLeftRecords(records, null);
    }

    public StatisticsAggregator foldLeftRecords(List<GenericRecord> records, StatisticsAggregator start) {
        StatisticsAggregator statsMerger = null;
        if (records != null) {
            statsMerger = start;
            for (GenericRecord record : records) {
                StatisticsAggregator statisticsAggregator = StatisticsAggregator.create(statisticsAggregatorSchema, record);
                statsMerger = statsMerger == null ? statisticsAggregator : statsMerger.merge(statisticsAggregator);
            }
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
            result.add(new StatisticsRecord(itA.next(), itR.next()));
        }
        return result;
    }
}
