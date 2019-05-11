package minimal_algorithms.sending;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

public class Sender {
    protected Mapper.Context mContext;
    protected Reducer.Context rContext;

    public Sender(Mapper.Context context) {
        this.mContext = context;
    }

    public Sender(Reducer.Context context) {
        this.rContext = context;
    }

    public <V> void sendToRangeMachines(V value, int lowerBound, int upperBound) throws IOException, InterruptedException {
        for (int i = lowerBound; i < upperBound; i++) {
            send(i, value);
        }
    }

    public <K, V> void send(K key, V value) throws IOException, InterruptedException {
        if (mContext == null) {
            rContext.write(key, value);
        } else {
            mContext.write(key, value);
        }
    }
}
