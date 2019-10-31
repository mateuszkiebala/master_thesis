package minimal_algorithms.hadoop.config;

import org.apache.hadoop.fs.Path;
import org.apache.avro.Schema;
import minimal_algorithms.hadoop.utils.Utils;

public class IOConfig {

    private Path homeDir;
    private Path input;
    private Path output;
    private Schema baseSchema;

    public IOConfig(Path homeDir, Path input, Path output, Schema baseSchema) {
        this.homeDir = homeDir;
        this.input = input;
        this.output = output;
        this.baseSchema = baseSchema;
    }

    public Path getHomeDir() {
        return homeDir;
    }

    public Path getInput() {
        return input;
    }

    public Path getOutput() {
        return output;
    }

    public Schema getBaseSchema() {
        return baseSchema;
    }
}
