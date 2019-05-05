package minimal_algorithms.avro_types.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import java.util.Comparator;

public abstract class KeyRecord<T> extends SpecificRecordBase implements SpecificRecord {
    public abstract void create(GenericRecord objectRecord);
    public abstract int compare(T that);

    public static KeyRecord max(KeyRecord a, KeyRecord b) {
        return a.compare(b) >= 1 ? a : b;
    }

    public static KeyRecord min(KeyRecord a, KeyRecord b) {
        return a.compare(b) <= 0 ? a : b;
    }
}
