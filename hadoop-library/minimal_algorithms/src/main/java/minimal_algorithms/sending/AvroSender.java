package minimal_algorithms.sending;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

public class AvroSender extends Sender {
    public AvroSender(Mapper.Context context) {
        super(context);
    }

    public AvroSender(Reducer.Context context) {
        super(context);
    }

    public <V> void sendToRangeMachines(V value, int lowerBound, int upperBound) throws IOException, InterruptedException {
        sendToRangeMachines(new AvroValue<V>(value), lowerBound, upperBound);
    }

    public <V> void sendToRangeMachines(AvroValue<V> avVal, int lowerBound, int upperBound) throws IOException, InterruptedException {
        final AvroKey<Integer> avKey = new AvroKey<>();
        for (int i = lowerBound; i < upperBound; i++) {
            avKey.datum(i);
            super.send(avKey, avVal);
        }
    }

    public <K, V> void send(K key, AvroValue<V> avVal) throws IOException, InterruptedException {
        super.send(new AvroKey<K>(key), avVal);
    }

    public <K, V> void send(AvroKey<K> key, V value) throws IOException, InterruptedException {
        super.send(key, new AvroValue<V>(value));
    }

    public <K, V> void send(K key, V value) throws IOException, InterruptedException {
        super.send(new AvroKey<K>(key), new AvroValue<V>(value));
    }

    public <K, V> void send(AvroKey<K> avKey, AvroValue<V> avVal) throws IOException, InterruptedException {
        super.send(avKey, avVal);
    }
}
