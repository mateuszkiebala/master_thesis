package minimal_algorithms.examples;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import minimal_algorithms.examples.types.*;
import minimal_algorithms.MinimalAlgorithm;
import minimal_algorithms.config.GroupByConfig;
import minimal_algorithms.config.Config;

public class GroupBy extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(GroupBy.class);

    public static final String GROUP_BY_SUPERDIR = "/group_by_output";

    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: Prefix <input> <intermediate_prefix> <elements> <splits> <reduce_tasks>");
            return -1;
        }

        MinimalAlgorithm ma = new MinimalAlgorithm(getConf(), Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Config config = ma.getConfig();
        GroupByConfig groupByConfig = new GroupByConfig(config, RWC4Cmps.firstCmp, Record4Float.getClassSchema(),
                                                        SumStatisticsAggregator.getClassSchema(), IntKeyRecord4Float.getClassSchema());
        ma.group(new Path(args[1]), new Path(args[0]), new Path(args[1] + GROUP_BY_SUPERDIR), groupByConfig);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // configuration is necessary to add -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        int res = ToolRunner.run(new Configuration(), new GroupBy(), args);
        System.exit(res);
    }
}
