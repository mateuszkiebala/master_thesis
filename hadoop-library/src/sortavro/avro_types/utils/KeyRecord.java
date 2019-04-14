package sortavro.avro_types.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

public abstract class KeyRecord<T extends KeyRecord> extends GenericData implements Comparable<T> {
    public abstract void create(GenericRecord record);

    public static KeyRecord min(KeyRecord a, KeyRecord b) {
        return (a.compareTo(b) <= 0) ? a : b;
    }

    public static KeyRecord max(KeyRecord a, KeyRecord b) {
        return (a.compareTo(b) >= 1) ? a : b;
    }
    
    // Powinnismy przekazywac comparator - musi byc ten sam co przy tera sorcie.
}
