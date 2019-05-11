package minimal_algorithms.sending;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

public class Sender<K, V> {
    private final AvroValue<V> avVal = new AvroValue<>();
    private final AvroKey<K> avKey = new AvroKey<>();
    private Mapper.Context mContext;
    private Reducer.Context rContext;

    public Sender(Mapper.Context context) {
        this.mContext = context;
    }

    public Sender(Reducer.Context context) {
        this.rContext = context;
    }

    public void sendToMachine(V o, AvroKey<K> dstMachineIndex) throws IOException, InterruptedException {
        avKey.datum(dstMachineIndex.datum());
        avVal.datum(o);
        send();
    }

    public void sendToMachine(V o, int dstMachineIndex) throws IOException, InterruptedException {
        final AvroKey<Integer> key = new AvroKey<>();
        key.datum(dstMachineIndex);
        avVal.datum(o);
        send(key);
    }

    public void sendToRangeMachines(V o, int lowerBound, int upperBound) throws IOException, InterruptedException {
        final AvroKey<Integer> key = new AvroKey<>();
        for (int i = lowerBound; i < upperBound; i++) {
            key.datum(i);
            avVal.datum(o);
            send(key);
        }
    }

    private <T> void send(AvroKey<T> key) throws IOException, InterruptedException {
        if (mContext == null) {
            rContext.write(key, avVal);
        } else {
            mContext.write(key, avVal);
        }
    }

    private void send() throws IOException, InterruptedException {
        if (mContext == null) {
            rContext.write(avKey, avVal);
        } else {
            mContext.write(avKey, avVal);
        }
    }
}
