package minimal_algorithms;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.tool.ConcatTool;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import minimal_algorithms.config.Config;

/**
 *
 * @authors jsroka, mateuszkiebala
 */
public class Utils {

    public static GenericRecord deepCopy(Schema schema, GenericRecord record) {
        return SpecificData.get().deepCopy(schema, record);
    }

    public static void storeComparatorInConf(Configuration conf, Comparator sortingCmp) {
        conf.set(Config.MAIN_COMPARATOR_KEY, sortingCmp.getClass().getName());
    }

    public static final Comparator retrieveComparatorFromConf(Configuration conf) {
        String className = conf.get(Config.MAIN_COMPARATOR_KEY);
        try {
            return (Comparator) Class.forName(className).newInstance();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't find class while retriving comparator: " + className, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't instantiate class while retriving comparator: " + className, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't access class while retriving comparator", ex);
        }
    }

    public static void storeSchemaInConf(Configuration conf, Schema schema, String key) {
        conf.set(key, schema.toString());
    }

    public static Schema retrieveSchemaFromConf(Configuration conf, String key) {
        String schema = conf.get(key);
        return new Schema.Parser().parse(schema);
    }

    public static void storePathInConf(Configuration conf, Path path, String key) {
        conf.set(key, path.toString());
    }

    public static Path retrievePathFromConf(Configuration conf, String key) {
        String path = conf.get(key);
        return new Path(path);
    }

    public static int getStripsCount(Configuration conf) {
        return conf.getInt(Config.NO_OF_STRIPS_KEY, Config.NO_OF_KEYS_DEFAULT);
    }

    public static int getTotalValuesCount(Configuration conf) {
        return conf.getInt(Config.NO_OF_VALUES_KEY, Config.NO_OF_KEYS_DEFAULT);
    }

    public static int getReduceTasksCount(Configuration conf) {
        return conf.getInt(Config.NO_OF_REDUCE_TASKS_KEY, Config.NO_OF_REDUCE_TASKS_DEFAULT);
    }

    public static Integer[] readAvroSortingCounts(Configuration conf, String fileName) {
        int stripsCount = Utils.getStripsCount(conf);
        Integer[] records = new Integer[stripsCount];
        File f = new File(fileName);

        GenericRecord datumKeyValuePair = null;
        Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));
        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(f, new GenericDatumReader<GenericRecord>(keyValueSchema))) {
            while (fileReader.hasNext()) {
                datumKeyValuePair = (GenericRecord) fileReader.next(datumKeyValuePair);
                records[(int) datumKeyValuePair.get(0)] = (int) datumKeyValuePair.get(1);
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileName, ie);
        }
        return records;
    }

    public static void writeRecordsToHDFSAvro(Configuration conf, String fileName, List<GenericRecord> records, Schema schema) {
        Path path = new Path(fileName);

        try (FileSystem hdfs = FileSystem.get(conf);
             OutputStream os = hdfs.create(path);
             DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new SpecificDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.create(schema, os);

            for (GenericRecord rec : records) {
                if (rec != null) {
                    dataFileWriter.append(rec);
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't write merged record file " + fileName, ie);
        }
    }

    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, String fileName, Schema schema) {
        int noOfStrips = Utils.getStripsCount(conf);
        return readRecordsFromHDFSAvro(conf, fileName, schema, noOfStrips);
    }

    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, String fileName, Schema schema, int count) {
        GenericRecord[] records = new GenericRecord[count];
        Path path = new Path(fileName);

        try (SeekableInput sInput = new FsInput(path, conf);
             FileReader<GenericRecord> fileReader = DataFileReader.openReader(sInput, new GenericDatumReader<GenericRecord>(schema))) {
            for (int i = 0; i < count; i++) {
                records[i] = fileReader.next();
            }
        } catch (IOException ex) {
            throw new IllegalArgumentException("can't read hdfs file " + fileName, ex);
        }
        return records;
    }

    public static void mergeHDFSAvro(Configuration conf, Path inputDir, String filePattern, String outFileName) {
        try {
            List<String> merged = new ArrayList<>();
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] statusList = hdfs.listStatus(inputDir);
            if (statusList != null) {
                for (FileStatus fileStatus : statusList) {
                    String filename = fileStatus.getPath().getName();
                    Pattern regex = Pattern.compile(filePattern);
                    Matcher matcher = regex.matcher(filename);

                    if (matcher.find()) {
                        merged.add(inputDir.toString() + "/" + filename);
                    }
                }
                merged.add(inputDir.toString() + "/" + outFileName);
                new ConcatTool().run(System.in, System.out, System.err, merged);
            }
        } catch (Exception e) {
            System.err.println("Cannot merge AVRO files: " + e.toString());
        }
    }

    public static GenericRecord[] readMainObjectRecordsFromLocalFileAvro(Configuration conf, String fileName) {
        return readRecordsFromLocalFileAvro(conf, fileName, Config.BASE_SCHEMA);
    }

    public static GenericRecord[] readRecordsFromLocalFileAvro(Configuration conf, String fileName, String schemaKey) {
        Schema schema = retrieveSchemaFromConf(conf, schemaKey);
        return readRecordsFromLocalFileAvro(conf, fileName, schema);
    }

    public static GenericRecord[] readRecordsFromLocalFileAvro(Configuration conf, String fileName, Schema schema) {
        int noOfStrips = getStripsCount(conf);
        GenericRecord[] records = new GenericRecord[noOfStrips - 1];

        File f = new File(fileName);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(f, new SpecificData().createDatumReader(schema))){
            for (int i = 0; i < noOfStrips - 1; i++) {
                records[i] = dataFileReader.next();
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileName, ie);
        }

        return records;
    }

    public static void writeRecordsToLocalFileAvro(Configuration conf, String fileName, GenericRecord[] records, Schema schema) {
        File f = new File(fileName);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new SpecificDatumWriter<GenericRecord>(schema))) {
            dataFileWriter.create(schema, f);

            for (GenericRecord rec : records) {
                if (rec != null) {
                    dataFileWriter.append(rec);
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileName, ie);
        }
    }
}
