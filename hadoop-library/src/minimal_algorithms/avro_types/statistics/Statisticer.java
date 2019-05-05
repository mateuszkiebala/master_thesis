package minimal_algorithms.avro_types.statistics;

import org.apache.avro.generic.GenericRecord;

public abstract class Statisticer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    public abstract void init(GenericRecord record);
    public abstract Statisticer merge(Statisticer that);
}
