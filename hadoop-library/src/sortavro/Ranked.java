package sortavro;
import org.apache.avro.generic.GenericRecord;
import sortavro.record.Record4Float;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.Schema;

public class Ranked {
    int rank = 0;
    Record4Float record = null;

    public Ranked(int rank, Record4Float record){
        this.rank = rank;
        this.record = record;
    }

    public static Schema getClassSchema() {
        return ReflectData.get().getSchema(Ranked.class);
    }
}
