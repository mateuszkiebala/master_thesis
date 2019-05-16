package minimal_algorithms.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
        return new Schema.Parser().parse(getSafeString(conf, key));
    }

    public static void storePathInConf(Configuration conf, Path path, String key) {
        conf.set(key, path.toString());
    }

    public static Path retrievePathFromConf(Configuration conf, String key) {
        return new Path(getSafeString(conf, key));
    }

    public static String getSafeString(Configuration conf, String key) {
        String result = conf.get(key);
        if (result == null) {
            throw new IllegalArgumentException("Can't find key (" + key + ") in configuration.");
        }
        return result;
    }

    public static int getMachinesNo(Configuration conf) {
        return getStripsCount(conf);
    }

    public static int getStripsCount(Configuration conf) {
        return getSafeInt(conf, Config.NO_OF_STRIPS_KEY);
    }

    public static long getTotalValuesCount(Configuration conf) {
        return getSafeLong(conf, Config.NO_OF_VALUES_KEY);
    }

    public static int getReduceTasksCount(Configuration conf) {
        return getSafeInt(conf, Config.NO_OF_REDUCE_TASKS_KEY);
    }

    public static long getItemsNoByMachines(Configuration conf) {
        return getSafeLong(conf, Config.NO_OF_ITEMS_BY_MACHINE);
    }

    public static int getRatioForRandomKey(Configuration conf) {
        return getSafeInt(conf, Config.RATIO_FOR_RANDOM_KEY);
    }

    public static int getSafeInt(Configuration conf, String key) {
        int result = conf.getInt(key, -1);
        if (result == -1) {
            result = conf.getInt(key, -2);
            if (result == -2) {
                throw new IllegalArgumentException("Can't find key (" + key + ") in configuration.");
            }
        }
        return result;
    }

    public static long getSafeLong(Configuration conf, String key) {
        long result = conf.getLong(key, -1);
        if (result == -1) {
            result = conf.getLong(key, -2);
            if (result == -2) {
                throw new IllegalArgumentException("Can't find key (" + key + ") in configuration.");
            }
        }
        return result;
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

    public static Long[] readAvroSortingCountsFromCache(Configuration conf, String fileNameCache) {
        int stripsCount = Utils.getStripsCount(conf);
        Long[] records = new Long[stripsCount];
        File f = new File(fileNameCache);

        GenericRecord datumKeyValuePair = null;
        Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG));
        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(f, new GenericDatumReader<GenericRecord>(keyValueSchema))) {
            while (fileReader.hasNext()) {
                datumKeyValuePair = (GenericRecord) fileReader.next(datumKeyValuePair);
                records[(int) datumKeyValuePair.get(0)] = (long) datumKeyValuePair.get(1);
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileNameCache, ie);
        }
        return records;
    }

    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, URI fileURI, String schemaKey) {
        int noOfStrips = Utils.getStripsCount(conf);
        return readRecordsFromHDFSAvro(conf, fileURI, schemaKey, noOfStrips);
    }

    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, URI fileURI, Schema schema) {
        int noOfStrips = Utils.getStripsCount(conf);
        return readRecordsFromHDFSAvro(conf, fileURI, schema, noOfStrips);
    }
    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, URI fileURI, String schemaKey, int limit) {
        Schema schema = retrieveSchemaFromConf(conf, schemaKey);
        return readRecordsFromHDFSAvro(conf, fileURI, schema, limit);
    }

    public static GenericRecord[] readRecordsFromHDFSAvro(Configuration conf, URI fileURI, Schema schema, int limit) {
        GenericRecord[] records = new GenericRecord[limit];

        try (FsInput in = new FsInput(new Path(fileURI), conf);
             DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(in, new GenericDatumReader<GenericRecord>(schema))) {

            int i = 0;
            while (dataFileReader.hasNext()) {
                records[i] = dataFileReader.next();
                if (i++ >= limit) {
                    break;
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read avro file " + fileURI, ie);
        }

        return records;
    }

    public static GenericRecord[] readRecordsFromCacheAvro(Configuration conf, String fileName, String schemaKey) {
        int noOfStrips = getStripsCount(conf);
        return readRecordsFromCacheAvro(conf, fileName, schemaKey, noOfStrips);
    }

    public static GenericRecord[] readRecordsFromCacheAvro(Configuration conf, String fileName, Schema schema) {
        int noOfStrips = getStripsCount(conf);
        return readRecordsFromCacheAvro(conf, fileName, schema, noOfStrips);
    }

    public static GenericRecord[] readRecordsFromCacheAvro(Configuration conf, String fileName, String schemaKey, int limit) {
        Schema schema = retrieveSchemaFromConf(conf, schemaKey);
        return readRecordsFromCacheAvro(conf, fileName, schema, limit);
    }

    public static GenericRecord[] readRecordsFromCacheAvro(Configuration conf, String fileName, Schema schema, int limit) {
        GenericRecord[] records = new GenericRecord[limit - 1];
        File f = new File(fileName);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(f, new SpecificData().createDatumReader(schema))){
            for (int i = 0; i < limit - 1; i++) {
                records[i] = dataFileReader.next();
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read cache file " + fileName, ie);
        }
        return records;
    }

    public static void writeRecordsToHDFSAvro(Configuration conf, URI fileURI, GenericRecord[] records, Schema schema) {
        writeRecordsToHDFSAvro(conf, fileURI, Arrays.asList(records), schema);
    }

    public static void writeRecordsToHDFSAvro(Configuration conf, URI fileURI, Iterable<GenericRecord> records, Schema schema) {
        Path path = new Path(fileURI);

        try (FileSystem hdfs = FileSystem.get(conf);
             OutputStream os = hdfs.create(path);
             DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new SpecificDatumWriter<GenericRecord>(schema))) {
            //dataFileWriter.setCodec(CodecFactory.snappyCodec());
            dataFileWriter.create(schema, os);
            for (GenericRecord rec : records) {
                if (rec != null) {
                    dataFileWriter.append(rec);
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't write avro file " + fileURI, ie);
        }
    }

    public static void deleteDirFromHDFS(Configuration conf, URI dirURI, boolean isRecursive) {
        deleteDirFromHDFS(conf, new Path(dirURI), isRecursive);
    }

    public static void deleteDirFromHDFS(Configuration conf, Path dirURIPath, boolean isRecursive) {
        try (FileSystem hdfs = FileSystem.get(conf)) {
            hdfs.delete(dirURIPath, isRecursive);
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't delete " + dirURIPath, ie);
        }
    }
}
