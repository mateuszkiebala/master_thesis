package minimal_algorithms.avro_types.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import java.util.Comparator;

public abstract class KeyRecord<T> extends SpecificRecordBase implements SpecificRecord {
    public abstract void init(GenericRecord objectRecord);
    public abstract int compare(T that);

    public static KeyRecord max(KeyRecord a, KeyRecord b) {
        return a.compare(b) >= 1 ? a : b;
    }

    public static KeyRecord min(KeyRecord a, KeyRecord b) {
        return a.compare(b) <= 0 ? a : b;
    }

    public static KeyRecord create(Schema keyRecordSchema, GenericRecord record) {
        KeyRecord keyRecord = null;
        try {
            Class keyRecordClass = SpecificData.get().getClass(keyRecordSchema);
            keyRecord = (KeyRecord) keyRecordClass.newInstance();
            keyRecord.init(record);
        } catch (Exception e) {
            System.err.println("Cannot create KeyRecord: " + e.toString());
        }
        return keyRecord;
    }
}
