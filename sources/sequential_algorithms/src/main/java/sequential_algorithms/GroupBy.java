package sequential_algorithms;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.GroupByComplex;
import org.apache.avro.specific.SpecificRecordBase;

public class GroupBy {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("data.avro");
        Map<Integer, Long> grouped = new HashMap<>();
        for (Complex c : elements) {
            Integer key = c.getIntPrim();
            long value = (long)c.getMiddle().getInner().getInnerInt() % 10000;
            if (grouped.containsKey(key)) {
                grouped.put(key, grouped.get(key) + value);
            } else {
                grouped.put(key, value);
            }
        }

        List<SpecificRecordBase> result = new ArrayList<>();
        for (Entry<Integer, Long> entry : grouped.entrySet()) {
            result.add(new GroupByComplex(entry.getKey(), entry.getValue()));
        }
        AvroUtils.write(result, "groupby_stats.avro", GroupByComplex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
