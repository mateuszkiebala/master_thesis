package prefix_app;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;

public class Utils {
  public static final String RATIO_KEY = "ratio";
  public static final String SPLIT_POINTS_KEY = "splitPoints";
  public static final String REDUCERS_NO_KEY = "reducersNo";

  public static ArrayList<String> readFromCache(Path filePath) {
    ArrayList<String> words = new ArrayList<>();
    try {
      BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
      String word = null;
      while((word = bufferedReader.readLine()) != null) {
        words.add(word);
      }
    } catch(IOException ex) {
        System.err.println("Exception while reading words from file: " + ex.getMessage());
    }
    return words;
  }
}
