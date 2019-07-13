package sequential_algorithms;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import sequential_algorithms.types.Complex;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

public class AvroUtils {
    public static String INPUT_DIR = "src/main/java/sequential_algorithms/";
    public static String OUTPUT_DIR = "src/main/java/sequential_algorithms/outputs/";

    public static List<Complex> read(String filename) {
        List result = new ArrayList<>();
        File input = new File(INPUT_DIR + filename);

        try {
            DatumReader<Complex> complexDatumReader = new SpecificDatumReader<Complex>(Complex.class);
            DataFileReader<Complex> dataFileReader = new DataFileReader<Complex>(input, complexDatumReader);
            Complex c = null;
            while (dataFileReader.hasNext()) {
                result.add(dataFileReader.next(c));
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("Can't read avro file");
        }
        return result;
    }

    public static void write(List<SpecificRecordBase> data, String filename, Schema schema) {
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) {
            if (!dir.mkdir())
                throw new IllegalArgumentException("Can't create output directory");
        }

        File output = new File(OUTPUT_DIR + filename);

        try (DataFileWriter<SpecificRecordBase> dataFileWriter = new DataFileWriter<SpecificRecordBase>(new SpecificDatumWriter<SpecificRecordBase>(schema));) {
            dataFileWriter.create(schema, output);
            for (SpecificRecordBase c : data) {
                dataFileWriter.append(c);
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("Can't write to avro file");
        }
    }
}
