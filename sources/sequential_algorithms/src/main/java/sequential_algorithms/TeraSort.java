package sequential_algorithms;

import java.util.List;
import java.util.concurrent.TimeUnit;
import sequential_algorithms.types.Complex;
import sequential_algorithms.types.ComplexCmp;
import org.apache.avro.specific.SpecificRecordBase;

public class TeraSort {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        List<Complex> elements = AvroUtils.read("data.avro");
        java.util.Collections.sort(elements, new ComplexCmp());
        AvroUtils.write((List<SpecificRecordBase>)(List<?>)elements, "terasort.avro", Complex.getClassSchema());

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        System.out.println("Execution time in ms: " + timeElapsed);
    }
}
