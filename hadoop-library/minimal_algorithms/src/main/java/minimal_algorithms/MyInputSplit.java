package minimal_algorithms;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mock implementation of InputSplit that does nothing.
 */
public class MyInputSplit extends FileSplit {

    private static final Path MOCK_PATH = new Path("somefile");

    public MyInputSplit() {
        super(MOCK_PATH, 0, 0, (String[]) null);
    }

    @Override
    public String toString() {
        return "MockInputSplit";
    }

    /**
     * Return the path object represented by this as a FileSplit.
     */
    public static Path getMockPath() {
        return MOCK_PATH;
    }
}
