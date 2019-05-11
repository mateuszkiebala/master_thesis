package minimal_algorithms.record;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author jsroka
 */
public class InputPreprocessing4TextToAvro extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(InputPreprocessing4TextToAvro.class);
    static final String DELIMETER = " ";

    public static class ParsingMapper extends Mapper<LongWritable, Text, AvroKey<Record4Float>, NullWritable> {

        private final AvroKey<Record4Float> avKey = new AvroKey();
        private final int howManyFields = 4;
        private final Record4Float record = new Record4Float();
        private final NullWritable nullWr = NullWritable.get();

        @Override
        protected void map(LongWritable offset, Text line, Mapper.Context context) throws IOException, InterruptedException {
            String[] numbers = line.toString().split(InputPreprocessing4TextToAvro.DELIMETER);
            if (numbers.length != howManyFields) {
                LOG.error("found record with " + numbers.length + " fields at offset " + offset);
            }
            record.first = Float.parseFloat(numbers[0]);
            record.second = Float.parseFloat(numbers[1]);
            record.third = Float.parseFloat(numbers[2]);
            record.fourth = Float.parseFloat(numbers[3]);
            avKey.datum(record);

            context.write(avKey, nullWr);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InputPreprocessing4TextToAvro <input> <output>");
            return -1;
        }
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = getConf();
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);//potrzebne zeby hadoop bral odpowiednie jary avro        

        LOG.info("starting input preprocessing");
        Job job = Job.getInstance(conf, "JOB: input preprocessing");
        job.setJarByClass(InputPreprocessing4TextToAvro.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);   

        job.setMapperClass(InputPreprocessing4TextToAvro.ParsingMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        //job.setOutputValueClass(NullWritable.class);

        //AvroJob.setInputKeySchema(job, Record4Float.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.NULL));
        AvroJob.setMapOutputKeySchema(job, Record4Float.getClassSchema());//Schema.create(Schema.Type.STRING));
        
        LOG.info("waiting for phase 1 sampling");
        int ret = (job.waitForCompletion(true) ? 0 : 1);

        Counters counters = job.getCounters();
        long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        LOG.info("finished input preprocessing, processed " + total + " key/value pairs");

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InputPreprocessing4TextToAvro(), args);//to configuration jest tu potrzebne zeby odczytac -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        System.exit(res);
    }
}
