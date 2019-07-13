package sequential_algorithms;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import sequential_algorithms.schema_types.Complex;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroUtils {
    public static List<Complex> read(String filename) {
        List result = new ArrayList<>();
        File input = new File(filename);

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

    public static void write(List<Complex> data, String filename) {
        File output = new File(filename);

        try {
            DatumWriter<Complex> complexDatumWriter = new SpecificDatumWriter<Complex>(Complex.class);
            DataFileWriter<Complex> dataFileWriter = new DataFileWriter<Complex>(complexDatumWriter);
            dataFileWriter.create(Complex.getClassSchema(), output);
            for (Complex c : data) {
                dataFileWriter.append(c);
            }
            dataFileWriter.close();
        } catch (IOException ie) {
            throw new IllegalArgumentException("Can't write to avro file");
        }
    }
}
