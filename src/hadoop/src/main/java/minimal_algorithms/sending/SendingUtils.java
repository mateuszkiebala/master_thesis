package minimal_algorithms.sending;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;


public class SendingUtils {

    public static List<GenericRecord> select1Records(List<SendWrapper> records) {
        List<GenericRecord> result = new ArrayList<>();
        for (SendWrapper sw : records) {
            if (sw.isType1()) {
                result.add(sw.getRecord1());
            }
        }
        return result;
    }

    public static List<GenericRecord> select2Records(List<SendWrapper> records) {
        List<GenericRecord> result = new ArrayList<>();
        for (SendWrapper sw : records) {
            if (sw.isType2()) {
                result.add(sw.getRecord2());
            }
        }
        return result;
    }

    public static Map<Integer, List<GenericRecord>> partitionRecords(Iterable<AvroValue<SendWrapper>> records) {
        Map<Integer, List<GenericRecord>> result = new HashMap<>();
        for (AvroValue<SendWrapper> record : records) {
            SendWrapper sw = SendWrapper.deepCopy(record.datum());
            int swType = sw.getType();
            List<GenericRecord> value = new ArrayList<>();

            if (result.containsKey(swType)) {
                value = result.get(swType);
                value.add(sw.getRecord());
                result.put(swType, value);
            } else {
                value.add(sw.getRecord());
                result.put(swType, value);
            }
        }
        return result;
    }
}