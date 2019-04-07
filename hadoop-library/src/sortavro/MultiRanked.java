package sortavro;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.Schema;

public class MultiRanked {
    List<Ranked> multiRanked;

    public MultiRanked(List<Ranked> multiRanked){
        this.multiRanked = multiRanked;
    }

    public static Schema getClassSchema() {
        return ReflectData.get().getSchema(MultiRanked.class);
    }
}
