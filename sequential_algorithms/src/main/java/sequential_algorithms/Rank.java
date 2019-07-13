package sequential_algorithms;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.RankComplex;
import sequential_algorithms.types.ComplexCmp;
import org.apache.avro.specific.SpecificRecordBase;

public class Rank {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("data.avro");
        java.util.Collections.sort(elements, new ComplexCmp());

        List<SpecificRecordBase> result = new ArrayList<>();
        int i = 0;
        for (Complex c : elements) {
            result.add(new RankComplex(i, c));
            i++;
        }
        AvroUtils.write(result, "rank.avro", RankComplex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
