package minimal_algorithms.hadoop.examples.input;

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
import minimal_algorithms.hadoop.semi_join.SemiJoinRecord;
import minimal_algorithms.hadoop.examples.types.*;

public class SemiJoinInputToAvro extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(SemiJoinInputToAvro.class);
    static final String DELIMETER = " ";

    public static class ParsingMapper extends Mapper<LongWritable, Text, AvroKey<SemiJoinTestRecord>, NullWritable> {

        private final AvroKey<SemiJoinTestRecord> avKey = new AvroKey();
        private final int howManyFields = 5;
        private final NullWritable nullWr = NullWritable.get();

        @Override
        protected void map(LongWritable offset, Text line, Mapper.Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split(SemiJoinInputToAvro.DELIMETER);
            if (args.length != howManyFields) {
                LOG.error("found record with " + args.length + " fields at offset " + offset);
            }

            Record4Float record4Float = new Record4Float();
            record4Float.first = Float.parseFloat(args[0]);
            record4Float.second = Float.parseFloat(args[1]);
            record4Float.third = Float.parseFloat(args[2]);
            record4Float.fourth = Float.parseFloat(args[3]);

            SemiJoinTestRecord semiJoinRecord = new SemiJoinTestRecord(SemiJoinRecord.Type.valueOf(args[4]), record4Float);
            avKey.datum(semiJoinRecord);

            context.write(avKey, nullWr);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SemiJoinInputToAvro <input> <output>");
            return -1;
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = getConf();
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        LOG.info("Starting input SemiJoinInputToAvro preprocessing");
        Job job = Job.getInstance(conf, "JOB: input SemiJoinInputToAvro preprocessing");
        job.setJarByClass(SemiJoinInputToAvro.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setMapperClass(SemiJoinInputToAvro.ParsingMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.NULL));
        AvroJob.setMapOutputKeySchema(job, SemiJoinTestRecord.getClassSchema());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SemiJoinInputToAvro(), args);
        System.exit(res);
    }
}
