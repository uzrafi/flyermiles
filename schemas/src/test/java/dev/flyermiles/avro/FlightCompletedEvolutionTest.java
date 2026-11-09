package dev.flyermiles.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

/**
 * FlightCompleted v2 adds an optional {@code cabin}. The registry runs in BACKWARD mode (new readers
 * read old data); the rollout plan deploys the producer first, which also needs FORWARD (old readers
 * read new data). An optional field with a default gives both.
 */
class FlightCompletedEvolutionTest {

  private final Schema v1 = load("/FlightCompleted-v1.avsc");
  private final Schema v2 = FlightCompleted.getClassSchema();

  @Test
  void v2IsBackwardCompatibleWithV1() {
    var result = SchemaCompatibility.checkReaderWriterCompatibility(v2, v1);
    assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
  }

  @Test
  void v1ConsumersCanReadV2Events() throws Exception {
    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v1, v2).getType())
        .isEqualTo(SchemaCompatibilityType.COMPATIBLE);

    var event = FlightCompleted.newBuilder()
        .setEventId("e-1").setMemberId("m-1").setFlightNumber("FM101")
        .setOrigin("YYZ").setDestination("YVR").setDistanceMiles(2080)
        .setFareClass("J").setCompletedAtMs(1_700_000_000_000L).setCabin("BUSINESS")
        .build();

    var out = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
    new SpecificDatumWriter<>(FlightCompleted.class).write(event, enc);
    enc.flush();

    GenericRecord oldView = new GenericDatumReader<GenericRecord>(v2, v1, GenericData.get())
        .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

    assertThat(oldView.get("distanceMiles")).isEqualTo(2080);
    assertThat(oldView.getSchema().getField("cabin")).isNull();
  }

  @Test
  void addingARequiredFieldWouldBeRejected() {
    Schema breaking = new Schema.Parser().parse(v1.toString().replace(
        "{\"name\":\"completedAtMs\",\"type\":\"long\"}",
        "{\"name\":\"completedAtMs\",\"type\":\"long\"},{\"name\":\"gate\",\"type\":\"string\"}"));
    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(breaking, v1).getType())
        .isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
  }

  private static Schema load(String path) {
    try (InputStream in = FlightCompletedEvolutionTest.class.getResourceAsStream(path)) {
      return new Schema.Parser().parse(in);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
