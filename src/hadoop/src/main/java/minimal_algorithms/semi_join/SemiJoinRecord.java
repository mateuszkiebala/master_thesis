package minimal_algorithms.semi_join;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.generic.GenericRecord;

public abstract class SemiJoinRecord<T> extends SpecificRecordBase implements SpecificRecord {
    public static enum Type {
        R,
        T
    }

    public abstract Type getType();

    public boolean isR() {
        return getType() == Type.R;
    }

    public boolean isT() {
        return getType() == Type.T;
    }
}
