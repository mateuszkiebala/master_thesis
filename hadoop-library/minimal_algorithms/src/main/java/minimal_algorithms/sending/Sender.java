package minimal_algorithms.sending;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

public class Sender<T> {
    private final AvroValue<T> avVal = new AvroValue<>();
    private final AvroKey<Integer> avKey = new AvroKey<>();
    private Mapper.Context mContext;
    private Reducer.Context rContext;

    public Sender(Mapper.Context context) {
        this.mContext = context;
    }

    public Sender(Reducer.Context context) {
        this.rContext = context;
    }

    public void sendToMachine(T o, AvroKey<Integer> dstMachineIndex) throws IOException, InterruptedException {
        avKey.datum(dstMachineIndex.datum());
        avVal.datum(o);
        send();
    }

    public void sendToMachine(T o, int dstMachineIndex) throws IOException, InterruptedException {
        avKey.datum(dstMachineIndex);
        avVal.datum(o);
        send();
    }

    public void sendToRangeMachines(T o, int lowerBound, int upperBound) throws IOException, InterruptedException {
        for (int i = lowerBound; i < upperBound; i++) {
            avKey.datum(i);
            avVal.datum(o);
            send();
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
