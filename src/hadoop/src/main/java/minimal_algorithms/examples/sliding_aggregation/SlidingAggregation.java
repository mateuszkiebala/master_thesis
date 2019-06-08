package minimal_algorithms.examples;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.avro.generic.GenericRecord;
import minimal_algorithms.examples.types.*;
import minimal_algorithms.config.Config;
import minimal_algorithms.MinimalAlgorithm;

public class SlidingAggregation extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(SlidingAggregation.class);

    public static final String SLIDING_AGGREGATION_SUPERDIR = "/sliding_aggregation_output";

    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: SlidingAggregation <input> <intermediate_prefix> <elements> <splits> <reduce_tasks> <window_length>");
            return -1;
        }

        long windowLength = Long.parseLong(args[5]);
        MinimalAlgorithm ma = new MinimalAlgorithm(getConf(), Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Config config = ma.getConfig();
        int ret = ma.slidingAggregation(new Path(args[1]), new Path(args[0]), new Path(args[1] + SLIDING_AGGREGATION_SUPERDIR),
                                        RWC4Cmps.firstCmp, Record4Float.getClassSchema(), SumStatisticsAggregator.getClassSchema(),
                                        windowLength);
        return ret;
    }

    public static void main(String[] args) throws Exception {
        // configuration is necessary to add -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        int res = ToolRunner.run(new Configuration(), new SlidingAggregation(), args);
        System.exit(res);
    }
}
