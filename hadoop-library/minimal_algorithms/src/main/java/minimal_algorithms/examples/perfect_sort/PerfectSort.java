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
import minimal_algorithms.config.BaseConfig;
import minimal_algorithms.config.Config;
import minimal_algorithms.MinimalAlgorithm;

public class PerfectSort extends Configured implements Tool {

    static final Log LOG = LogFactory.getLog(PerfectSort.class);

    public static final String PERFECT_SORT_SUPERDIR = "/perfect_sort_output";

    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: PerfectSort <input> <intermediate_prefix> <elements> <splits> <reduce_tasks>");
            return -1;
        }

        MinimalAlgorithm ma = new MinimalAlgorithm(getConf(), Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Config config = ma.getConfig();
        BaseConfig baseConfig = new BaseConfig(config, RWC4Cmps.firstCmp, Record4Float.getClassSchema());
        ma.perfectSort(new Path(args[1]), new Path(args[0]), new Path(args[1] + PERFECT_SORT_SUPERDIR), baseConfig);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // configuration is necessary to add -libjars (http://stackoverflow.com/questions/28520821/how-to-add-external-jar-to-hadoop-job)
        int res = ToolRunner.run(new Configuration(), new PerfectSort(), args);
        System.exit(res);
    }
}
