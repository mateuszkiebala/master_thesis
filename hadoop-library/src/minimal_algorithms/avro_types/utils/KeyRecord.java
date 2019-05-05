package minimal_algorithms.avro_types.utils;

import org.apache.avro.generic.GenericRecord;
import java.util.Comparator;

public abstract class KeyRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public abstract void create(GenericRecord objectRecord);

    public abstract GenericRecord getObjectRecord();

    public static KeyRecord min(KeyRecord a, KeyRecord b, Comparator<GenericRecord> comparator) {
        return comparator.compare(a.getObjectRecord(), b.getObjectRecord()) <= 0 ? a : b;
    }

    public static KeyRecord max(KeyRecord a, KeyRecord b, Comparator<GenericRecord> comparator) {
        return comparator.compare(a.getObjectRecord(), b.getObjectRecord()) >= 1 ? a : b;
    }
}
