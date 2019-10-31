package sequential_algorithms;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.StatsComplex;
import sequential_algorithms.types.ComplexCmp;
import org.apache.avro.specific.SpecificRecordBase;

public class PrefixStats {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("data.avro");
        java.util.Collections.sort(elements, new ComplexCmp());

        List<SpecificRecordBase> result = new ArrayList<>();
        long sum = 0;
        for (Complex c : elements) {
            sum += c.getIntPrim();
            result.add(new StatsComplex(sum, c));
        }
        AvroUtils.write(result, "prefix_stats.avro", StatsComplex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
