package dev.flyermiles.member;

import dev.flyermiles.avro.MemberProfile;
import dev.flyermiles.avro.TierChanged;
import dev.flyermiles.avro.Topics;
import dev.flyermiles.member.MemberRepository.Member;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Postgres is the source of truth for profiles; {@code member.profiles} is a compacted copy keyed by
 * memberId that the earning engine reads as a KTable. Publishing happens after the DB write and is
 * awaited, and {@link #republishAll()} rebuilds the topic, so a failed publish is repairable.
 */
@Service
public class MemberService {

  private static final Logger log = LoggerFactory.getLogger(MemberService.class);
  private static final String[] FIRST = {"Ava", "Liam", "Noah", "Maya", "Zoe", "Omar", "Aisha", "Ethan", "Lea", "Raj"};
  private static final String[] LAST = {"Tremblay", "Singh", "Nguyen", "Martin", "Roy", "Khan", "Chen", "Wilson", "Gagnon", "Ali"};

  private final MemberRepository repo;
  private final KafkaTemplate<String, MemberProfile> kafka;

  public MemberService(MemberRepository repo, KafkaTemplate<String, MemberProfile> kafka) {
    this.repo = repo;
    this.kafka = kafka;
  }

  public Member enrol(String name, String email) {
    Member m = repo.insert(UUID.randomUUID(), name, email);
    publish(m).join();
    return m;
  }

  /** Seeds fake members for load tests. */
  public int enrolMany(int count) {
    var rnd = ThreadLocalRandom.current();
    var sends = new ArrayList<CompletableFuture<?>>(count);
    for (int i = 0; i < count; i++) {
      String name = FIRST[rnd.nextInt(FIRST.length)] + " " + LAST[rnd.nextInt(LAST.length)];
      UUID id = UUID.randomUUID();
      Member m = repo.insert(id, name, "member-" + id.toString().substring(0, 8) + "@example.com");
      sends.add(publish(m));
    }
    CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).join();
    return count;
  }

  public int republishAll() {
    int total = 0;
    UUID after = null;
    while (true) {
      List<Member> page = repo.page(after, 1000);
      if (page.isEmpty()) return total;
      CompletableFuture.allOf(page.stream().map(this::publish).toArray(CompletableFuture[]::new)).join();
      total += page.size();
      after = page.get(page.size() - 1).memberId();
    }
  }

  /** The earning engine detected a threshold crossing; apply it and tell the engine's KTable. */
  @KafkaListener(topics = Topics.TIER_CHANGED, groupId = "member-service")
  public void onTierChanged(TierChanged change) {
    repo.upgradeTier(UUID.fromString(change.getMemberId()), change.getNewTier())
        .ifPresentOrElse(
            m -> {
              log.info("member {} upgraded {} -> {} at {} qualifying miles ({})",
                  m.memberId(), change.getPreviousTier(), m.tier(), change.getQualifyingMiles(), change.getYear());
              publish(m).join();
            },
            () -> log.debug("tier.changed for {} to {} is not an upgrade, ignored", change.getMemberId(), change.getNewTier()));
  }

  private CompletableFuture<?> publish(Member m) {
    var profile = MemberProfile.newBuilder()
        .setMemberId(m.memberId().toString())
        .setName(m.name())
        .setTier(m.tier())
        .setUpdatedAtMs(m.updatedAt().toEpochMilli())
        .build();
    return kafka.send(Topics.MEMBER_PROFILES, profile.getMemberId(), profile);
  }
}
