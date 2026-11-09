package dev.flyermiles.query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Cache-aside for balances. Entries are deleted as soon as a ledger event for the member is applied,
 * with a short TTL as the safety net for the one race cache-aside can't close on its own (a reader
 * fills the cache with a value it read just before the projection committed). Redis being down
 * degrades to direct Postgres reads, never to errors.
 */
@Component
public class BalanceCache {

  public record Cached(long balance, long seq) {}

  private static final Logger log = LoggerFactory.getLogger(BalanceCache.class);

  private final StringRedisTemplate redis;
  private final Duration ttl;
  private final AtomicBoolean enabled;
  private final Counter hits;
  private final Counter misses;

  public BalanceCache(
      StringRedisTemplate redis,
      @Value("${query.cache.ttl:PT30S}") Duration ttl,
      @Value("${query.cache.enabled:true}") boolean enabled,
      MeterRegistry meters) {
    this.redis = redis;
    this.ttl = ttl;
    this.enabled = new AtomicBoolean(enabled);
    this.hits = Counter.builder("flyermiles_balance_cache_requests_total").tag("result", "hit").register(meters);
    this.misses = Counter.builder("flyermiles_balance_cache_requests_total").tag("result", "miss").register(meters);
  }

  public boolean enabled() {
    return enabled.get();
  }

  public void setEnabled(boolean on) {
    enabled.set(on);
  }

  public Optional<Cached> get(UUID memberId) {
    if (!enabled.get()) return Optional.empty();
    try {
      String v = redis.opsForValue().get(key(memberId));
      if (v == null) {
        misses.increment();
        return Optional.empty();
      }
      hits.increment();
      int sep = v.indexOf(':');
      return Optional.of(new Cached(Long.parseLong(v, 0, sep, 10), Long.parseLong(v, sep + 1, v.length(), 10)));
    } catch (Exception e) {
      log.debug("redis get failed: {}", e.toString());
      return Optional.empty();
    }
  }

  public void put(UUID memberId, Cached value) {
    if (!enabled.get()) return;
    try {
      redis.opsForValue().set(key(memberId), value.balance() + ":" + value.seq(), ttl);
    } catch (Exception e) {
      log.debug("redis set failed: {}", e.toString());
    }
  }

  public void evict(Collection<UUID> memberIds) {
    if (memberIds.isEmpty()) return;
    try {
      redis.delete(memberIds.stream().map(BalanceCache::key).toList());
    } catch (Exception e) {
      log.warn("redis evict failed, relying on TTL: {}", e.toString());
    }
  }

  public void clear() {
    try {
      var keys = redis.keys("balance:*");
      if (keys != null && !keys.isEmpty()) redis.delete(keys);
    } catch (Exception e) {
      log.warn("redis clear failed: {}", e.toString());
    }
  }

  private static String key(UUID memberId) {
    return "balance:" + memberId;
  }
}
