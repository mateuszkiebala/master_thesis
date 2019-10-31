package minimal_algorithms.hadoop.sending;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import minimal_algorithms.hadoop.utils.Utils;

public class Sender {
    protected Mapper.Context mContext;
    protected Reducer.Context rContext;

    public Sender(Mapper.Context context) {
        this.mContext = context;
    }

    public Sender(Reducer.Context context) {
        this.rContext = context;
    }

    public <V> void sendToAllLowerMachines(V value, int upperBound) throws IOException, InterruptedException {
        sendToRangeMachines(value, 0, upperBound);
    }

    public <V> void sendToAllHigherMachines(V value, int lowerBound) throws IOException, InterruptedException {
        sendToRangeMachines(value, lowerBound + 1, Utils.getMachinesNo(getConf()));
    }

    public <V> void sendToAllMachines(V value) throws IOException, InterruptedException {
        sendToRangeMachines(value, 0, Utils.getMachinesNo(getConf()));
    }

    public <V> void sendToRangeMachines(V value, int lowerBound, int upperBound) throws IOException, InterruptedException {
        for (int i = lowerBound; i < upperBound; i++) {
            sendBounded(i, value);
        }
    }

    public <V> void sendBounded(int key, V value) throws IOException, InterruptedException {
        if (key >= 0 && key < Utils.getMachinesNo(getConf())) {
            send(key, value);
        }
    }

    public <K, V> void send(K key, V value) throws IOException, InterruptedException {
        if (mContext == null) {
            rContext.write(key, value);
        } else {
            mContext.write(key, value);
        }
    }

    public Configuration getConf() {
        if (mContext == null) {
            return rContext.getConfiguration();
        } else {
            return mContext.getConfiguration();
        }
    }
}
