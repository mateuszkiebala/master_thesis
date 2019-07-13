package sequential_algorithms;

import java.util.List;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.schema_types.Complex;
import sequential_algorithms.schema_types.ComplexCmp;

public class TeraSort {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("src/main/java/sequential_algorithms/data.avro");
        java.util.Collections.sort(elements, new ComplexCmp());
        AvroUtils.write(elements, "src/main/java/sequential_algorithms/outputs/terasort.avro");

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
