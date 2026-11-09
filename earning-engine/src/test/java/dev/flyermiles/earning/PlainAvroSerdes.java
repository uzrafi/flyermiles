package dev.flyermiles.earning;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** Raw Avro binary, no schema registry: lets TopologyTestDriver run without any broker or registry. */
final class PlainAvroSerdes implements AvroSerdes {

  @Override
  public <T extends SpecificRecord> Serde<T> forValue(Class<T> type) {
    var writer = new SpecificDatumWriter<T>(type);
    var reader = new SpecificDatumReader<T>(type);
    return Serdes.serdeFrom(
        (topic, value) -> {
          if (value == null) return null;
          try {
            var out = new ByteArrayOutputStream();
            var enc = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(value, enc);
            enc.flush();
            return out.toByteArray();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        },
        (topic, bytes) -> {
          if (bytes == null) return null;
          try {
            return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }
}
