package minimal_algorithms.factory.examples.hadoop;

import java.io.IOException;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.avro.Schema;
import minimal_algorithms.factory.MinimalAlgFactory;
import minimal_algorithms.hadoop.examples.types.*;
import minimal_algorithms.hadoop.config.IOConfig;
import minimal_algorithms.hadoop.config.Config;

public class ExamplePrefix extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(ExamplePrefix.class);

    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: ExamplePrefix <home_dir> <input_path> <output_path> <items_no> <partitions_no> <reduce_tasks_no>");
            return -1;
        }

        IOConfig ioConfig = new IOConfig(new Path(args[0]), new Path(args[1]), new Path(args[2]), Record4Float.getClassSchema());
        Config config = new Config(getConf(), Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        Comparator cmp = RWC4Cmps.firstCmp;
        Schema statsSchema = SumStatisticsAggregator.getClassSchema();
        return MinimalAlgFactory.hadoop(config).prefix(ioConfig, cmp, statsSchema);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ExamplePrefix(), args);
        System.exit(res);
    }
}
