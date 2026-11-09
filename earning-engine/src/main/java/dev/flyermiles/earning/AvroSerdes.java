package dev.flyermiles.earning;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/** Builds value serdes for the Avro types; swapped for a registry-free version in tests. */
@FunctionalInterface
public interface AvroSerdes {
  <T extends SpecificRecord> Serde<T> forValue(Class<T> type);
}
