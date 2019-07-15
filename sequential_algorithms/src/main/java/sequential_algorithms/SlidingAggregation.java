package sequential_algorithms;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.StatsComplex;
import sequential_algorithms.types.ComplexCmp;
import org.apache.avro.specific.SpecificRecordBase;

public class SlidingAggregation {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("data.avro");
        java.util.Collections.sort(elements, new ComplexCmp());

        List<Long> prefixSum = new ArrayList<>();
        long sum = 0;
        for (Complex c : elements) {
            sum += c.getIntPrim();
            prefixSum.add(sum);
        }

        List<SpecificRecordBase> result = new ArrayList<>();
        int window = 100;
        for (int i = 0; i < prefixSum.size(); i++) {
            long subValue = i - window <= 0 ? 0 : prefixSum.get(i - window);
            long value = prefixSum.get(i) - subValue;
            result.add(new StatsComplex(value, elements.get(i)));
        }
        AvroUtils.write(result, "sliding_agg.avro", StatsComplex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
