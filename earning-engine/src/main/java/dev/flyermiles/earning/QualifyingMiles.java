package dev.flyermiles.earning;

import dev.flyermiles.avro.Tier;
import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Per-member, per-year aggregate. Keeping the tier before the latest flight lets the topology emit
 * {@code tier.changed} exactly on the record that crosses a threshold.
 */
public record QualifyingMiles(long miles, Tier tier, Tier previousTier) {

  public static QualifyingMiles initial() {
    return new QualifyingMiles(0, Tier.BASE, Tier.BASE);
  }

  public QualifyingMiles add(int distanceMiles) {
    long total = miles + distanceMiles;
    return new QualifyingMiles(total, EarningRules.tierFor(total), tier);
  }

  public boolean upgraded() {
    return tier.ordinal() > previousTier.ordinal();
  }

  /** 10 bytes: miles, tier ordinal, previous tier ordinal. Internal state only, so no registry. */
  public static Serde<QualifyingMiles> serde() {
    Serializer<QualifyingMiles> ser = (topic, v) -> v == null ? null
        : ByteBuffer.allocate(10).putLong(v.miles).put((byte) v.tier.ordinal())
            .put((byte) v.previousTier.ordinal()).array();
    Deserializer<QualifyingMiles> de = (topic, bytes) -> {
      if (bytes == null) return null;
      var buf = ByteBuffer.wrap(bytes);
      Tier[] tiers = Tier.values();
      return new QualifyingMiles(buf.getLong(), tiers[buf.get()], tiers[buf.get()]);
    };
    return Serdes.serdeFrom(ser, de);
  }
}
