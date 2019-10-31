package sequential_algorithms;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.GroupByComplex;
import org.apache.avro.specific.SpecificRecordBase;

public class SemiJoin {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> rElements = AvroUtils.read("dataR.avro");
        List<Complex> tElements = AvroUtils.read("dataT.avro");

        Map<Long, Complex> tMap = new HashMap<>();
        for (Complex t : tElements) {
            long key = (long)t.getMiddle().getInner().getInnerInt() % 10;
            tMap.put(key, t);
        }

        List<SpecificRecordBase> result = new ArrayList<>();
        for (Complex r : rElements) {
            long key = (long)r.getMiddle().getInner().getInnerInt() % 10;
            if (tMap.containsKey(key)) {
                result.add(tMap.get(key));
            }
        }
        AvroUtils.write(result, "semijoin.avro", Complex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
