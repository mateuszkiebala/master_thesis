package sortavro;

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
import org.apache.hadoop.fs.Path;
import sortavro.record.Record4Float;

/**
 *
 * @author jsroka
 */
public class Utils {

    public static final String LO_BORDERS_FILENAMES_KEY = "lo.borders.filemanes.key";
    public static final String HI_BORDERS_FILENAMES_KEY = "hi.borders.filemanes.key";
    public static final String TREE_COMPARATORS_KEY = "tree.comparators.key";
    public static final String MAIN_COMPARATOR_KEY = "main.comparator.key";
    public static final String NO_OF_REDUCE_TASKS_KEY = "no.of.reduce.tasks.key";
    public static final String MAIN_OBJECT_SCHEMA = "main.object.schema";
    public static final int NO_OF_REDUCE_TASKS_DEFAULT = 2;

    public static final Comparator retrieveComparatorFromConf(Configuration conf) {
        String className = conf.get(MAIN_COMPARATOR_KEY);
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

    private static void createStringAndStoreComparators(Comparator[] fieldCmps, Configuration conf) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < fieldCmps.length; i++) {
            if (i < fieldCmps.length - 1) {
                names.add(fieldCmps[i].getClass().getName() + ";");
            } else {
                names.add(fieldCmps[i].getClass().getName());
            }
        }
        StringBuilder b = new StringBuilder();
        names.forEach(b::append);
        conf.set(TREE_COMPARATORS_KEY, b.toString());
    }

    public static Comparator[] retrieveComparatorsFromConf(Configuration conf) {
        try {
            String[] classNames = conf.get(TREE_COMPARATORS_KEY).split(";");
            Comparator[] comparators = new Comparator[classNames.length];
            for (int i = 0; i < classNames.length; i++) {
                comparators[i] = (Comparator) Class.forName(classNames[i]).newInstance();
            }
            return comparators;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't find class while retriving comparator ", ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't instantiate class while retriving comparator", ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException("can't access class while retriving comparator", ex);
        }
    }

    public static void storeComparatorsInConf(Configuration conf, Comparator sortingCmp, Comparator... otherCmps) {
        conf.set(MAIN_COMPARATOR_KEY, sortingCmp.getClass().getName());
        Utils.createStringAndStoreComparators(otherCmps, conf);
    }

    public static void storeMainObjectSchemaInConf(Configuration conf, Schema schema) {
        conf.set(MAIN_OBJECT_SCHEMA, schema.toString());
    }

    public static Schema retrieveMainObjectSchemaFromConf(Configuration conf) {
        String schema = conf.get(MAIN_OBJECT_SCHEMA);
        return new Schema.Parser().parse(schema);
    }

    //assumes that filenames have no semicolons ;
    //and that filenames are not empty strings
    private static String filenamesToString(String... filenames) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < filenames.length; i++) {
            names.add(filenames[i] + ";");
        }
        names.add(";<dummy>");
        StringBuilder b = new StringBuilder();
        names.forEach(b::append);
        return b.toString();
    }

    //assumes that filenames have no semicolons ;
    //and that filenames are not empty strings
    public static void storeInConfLoBoundsFilenamesComputedSoFar(Configuration conf, String... filenames) {
        conf.set(LO_BORDERS_FILENAMES_KEY, filenamesToString(filenames));
    }

    //assumes that filenames have no semicolons ;
    //and that filenames are not empty strings
    public static void storeInConfHiBoundsFilenamesComputedSoFar(Configuration conf, String... filenames) {
        conf.set(HI_BORDERS_FILENAMES_KEY, filenamesToString(filenames));
    }

    //this method is here so it is easier to do unit testing
    static String[] retrieveBoundsFilenamesFromConf(Configuration conf, String key) {
        String val = conf.get(key);
        String[] fileNames = val.split(";");
        if (fileNames[fileNames.length - 2].equals("")) {
            return java.util.Arrays.copyOfRange(fileNames, 0, fileNames.length - 2);
        } else {
            return java.util.Arrays.copyOfRange(fileNames, 0, fileNames.length - 1);
        }
    }

    static Record4Float[][] retrieveLoBoundsFromHDFSFile(Configuration conf) {
        return retrieveComputedBoundsHDFSFile(conf, retrieveBoundsFilenamesFromConf(conf, LO_BORDERS_FILENAMES_KEY));
    }

    static Record4Float[][] retrieveHiBoundsFromHDFSFile(Configuration conf) {
        return retrieveComputedBoundsHDFSFile(conf, retrieveBoundsFilenamesFromConf(conf, HI_BORDERS_FILENAMES_KEY));
    }

    //arrays that stores subsequent arrays with bounds for subsequent dimensions
    //for missing dimensions contains nulls
    static private Record4Float[][] retrieveComputedBoundsHDFSFile(Configuration conf, String[] fileNames) {
        Record4Float[][] bounds = new Record4Float[SortAvroRecord.NO_OF_DIMENSIONS][];
        int noOfStrips = Utils.getStripsCount(conf);
        for (int i = 0; i < fileNames.length; i++) {
            if (!fileNames[i].equals("")) {
                    bounds[i] = readRecordsFromHDFSAvro(conf, fileNames[i], noOfStrips);
            } else {
                bounds[i] = null;
            }
        }
        return bounds;
    }

    public static final void copyRecord(Record4Float from, int count, Record4Float to) {
        to.first = from.first;
        to.second = from.second;
        to.third = from.third;
        to.fourth = from.fourth;
    }

    public static int getStripsCount(Configuration conf) {
        return conf.getInt(PhaseSampling.NO_OF_STRIPS_KEY, PhaseSampling.NO_OF_KEYS_DEFAULT);
    }

    public static int getTotalValuesCount(Configuration conf) {
        return conf.getInt(PhaseSampling.NO_OF_VALUES_KEY, PhaseSampling.NO_OF_KEYS_DEFAULT);
    }

    public static int getReduceTasksCount(Configuration conf) {
        return conf.getInt(NO_OF_REDUCE_TASKS_KEY, NO_OF_REDUCE_TASKS_DEFAULT);
    }

    public static Record4Float[] readRecordsFromHDFSAvro(Configuration conf, String fileName, int count) {
        Record4Float[] records = new Record4Float[count];

        Path path = new Path(fileName);
        try (SeekableInput sInput = new FsInput(path, conf);
                FileReader<Record4Float> fileReader = DataFileReader.openReader(sInput, new SpecificDatumReader<>(Record4Float.class))) {

            for (int i = 0; i < count; i++) {
                records[i] = fileReader.next();
            }
        } catch (IOException ex) {
            throw new IllegalArgumentException("can't read hdfs file " + fileName, ex);
        }

        return records;
    }

    public static void writeRecordsToHDFSAvro(Configuration conf, String fileName, Record4Float[] records) {
        //write result
        Path path = new Path(fileName);

        try (FileSystem hdfs = FileSystem.get(conf);
                OutputStream os = hdfs.create(path);
                DataFileWriter<Record4Float> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(Record4Float.class))) {
            dataFileWriter.create(Record4Float.getClassSchema(), os);

            for (Record4Float rec : records) {
                if (rec != null) {
                    dataFileWriter.append(rec);
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't write merged record file " + fileName, ie);
        }
    }

    public static GenericRecord[] readRecordsFromLocalFileAvro(Configuration conf, String fileName) {
        int noOfStrips = getStripsCount(conf);
        Schema mainObjectSchema = retrieveMainObjectSchemaFromConf(conf);
        GenericRecord[] records = new GenericRecord[noOfStrips - 1];

        File f = new File(fileName);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(f, new SpecificData().createDatumReader(mainObjectSchema))){
            for (int i = 0; i < noOfStrips - 1; i++) {
                records[i] = dataFileReader.next();
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read local file " + fileName, ie);
        }

        return records;
    }

    public static int[] readIntsFromLocalFileNormal(Configuration conf, String mergedFilename, int count) {
        int[] values = new int[count];

        File f = new File(mergedFilename);
        try (Scanner sc = new Scanner(f);) {
            for (int i = 0; i < count; i++) {
                values[i] = sc.nextInt();
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't read file with merged values " + mergedFilename, ie);
        }

        return values;
    }

    public static void mergeIntFilesIntoNormal(String filesFormatter, String mergedFilename, Configuration conf, int stripscount) {
        int reducerTasksCount = getReduceTasksCount(conf);
        int[] values = new int[stripscount];

        //read small files
        Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.INT));
        GenericRecord datum = null;
        Path path;
        for (int i = 0; i < reducerTasksCount; i++) {
            path = new Path(String.format(filesFormatter, i));
            try (SeekableInput sInput = new FsInput(path, conf);
                    FileReader<GenericRecord> fileReader = DataFileReader.openReader(sInput, new GenericDatumReader<>(keyValueSchema))) {
                while (fileReader.hasNext()) {
                    datum = fileReader.next(datum);
                    values[(int) datum.get(0)] = (int) datum.get(1);
                }
            } catch (IOException ex) {
                throw new IllegalArgumentException("can't read while merging small int files " + path.getName(), ex);
            }
        }

        //write result
        try {
            Path mergedPath = new Path(new URI(mergedFilename));
            try (FileSystem fs = mergedPath.getFileSystem(conf);
                    OutputStream os = fs.create(mergedPath);
                    PrintWriter pw = new PrintWriter(os);) {
                for (int i : values) {
                    pw.println(i);
                }
            }
        } catch (IOException ie) {
            throw new IllegalArgumentException("can't write merged file " + mergedFilename, ie);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("wrong uri while writing merged file " + mergedFilename, ex);
        }

//        try {
//            String mergedFileName = conf.get(PhaseBalancing.MERGED_COUNTS_FILENAME_KEY, PhaseBalancing.DEFAULT_COUNTS_KEY);
//            FileUtil.copyMerge(input.getFileSystem(conf), input, input.getFileSystem(conf), new Path(mergedFileName), false, conf, "");
//        } catch (IOException ie) {
//            throw new IllegalArgumentException("can't write file with all counts", ie);
//        }
    }

    public static void mergeRecordFilesIntoAvro(String filesFormatter, String mergedFilename, Configuration conf, int stripsCount) {
        int reducerTasksCount = getReduceTasksCount(conf);
        Record4Float[] values = new Record4Float[stripsCount];

        //read small files
        Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Record4Float.getClassSchema());
        GenericRecord datumKeyValuePair = null;
        GenericRecord datumRecord4Float = null;
        Path path;
        for (int i = 0; i < reducerTasksCount; i++) {
            path = new Path(String.format(filesFormatter, i));
            try (SeekableInput sInput = new FsInput(path, conf);
                    FileReader<GenericRecord> fileReader = DataFileReader.openReader(sInput, new GenericDatumReader<>(keyValueSchema))) {
                while (fileReader.hasNext()) {
                    //niestety z avrokeyvalue nie dziala specificdatumreader, wiec trzeba taki hack
                    datumKeyValuePair = (GenericRecord) fileReader.next(datumKeyValuePair);
                    datumRecord4Float = (GenericRecord) datumKeyValuePair.get(1);
                    values[(int) datumKeyValuePair.get(0)] = new Record4Float((float) datumRecord4Float.get(0), (float) datumRecord4Float.get(1), (float) datumRecord4Float.get(2), (float) datumRecord4Float.get(3));
                }
            } catch (IOException ex) {
                throw new IllegalArgumentException("can't read while merging small files " + path.getName(), ex);
            }
        }

        //write result
        writeRecordsToHDFSAvro(conf, mergedFilename, values);
    }
}
