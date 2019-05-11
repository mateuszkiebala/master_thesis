package minimal_algorithms;

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;
import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import minimal_algorithms.record.Record4Float;
import minimal_algorithms.avro_types.statistics.*;
import minimal_algorithms.avro_types.terasort.*;
import minimal_algorithms.avro_types.utils.*;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.PhasePrefix.PrefixMapper;
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.config.StatisticsConfig;
import minimal_algorithms.config.GroupByConfig;
import minimal_algorithms.Utils;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.avro.specific.SpecificData;

public class MyOutputCollector<K, V> implements OutputCollector<K, V> {

    private final ArrayList<Pair<K, V>> collectedOutputs;
    private final Schema keySchema;
    private final Schema valueSchema;

    public MyOutputCollector(final Schema keySchema, final Schema valueSchema) {
        collectedOutputs = new ArrayList<Pair<K, V>>();
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    private Object deepKeyCopy(final Object obj) {
        return SpecificData.get().deepCopy(keySchema, obj);
    }

    private Object deepValueCopy(final Object obj) {
        return SpecificData.get().deepCopy(valueSchema, obj);
    }

    /**
     * Accepts another (key, value) pair as an output of this mapper/reducer.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void collect(final K key, final V value) throws IOException {
        collectedOutputs
                .add(new Pair<K, V>((K) deepKeyCopy(key), (V) deepValueCopy(value)));
    }

    /**
     * @return The outputs generated by the mapper/reducer being tested
     */
    public List<Pair<K, V>> getOutputs() {
        return collectedOutputs;
    }
}