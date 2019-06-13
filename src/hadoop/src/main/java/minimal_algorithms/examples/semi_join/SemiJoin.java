package minimal_algorithms.hadoop.examples;

import java.io.IOException;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import minimal_algorithms.hadoop.examples.types.*;
import minimal_algorithms.hadoop.HadoopMinAlgFactory;
import minimal_algorithms.hadoop.config.IOConfig;
import minimal_algorithms.hadoop.config.Config;

public class SemiJoin extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(SemiJoin.class);

    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: SemiJoin <home_dir> <input_path> <output_path> <elements_no> <partitions_no> <reduce_tasks_no>");
            return -1;
        }

        IOConfig ioConfig = new IOConfig(new Path(args[0]), new Path(args[1]), new Path(args[2]), SemiJoinTestRecord.getClassSchema());
        Config config = new Config(getConf(), Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        return new HadoopMinAlgFactory(config).semiJoin(ioConfig, SemiJoinTestRecord.cmp, SemiJoinTestKeyRecord.getClassSchema());
    }

    public static void main(String[] args) throws Exception {
        // configuration is necessary to add -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        int res = ToolRunner.run(new Configuration(), new SemiJoin(), args);
        System.exit(res);
    }
}
