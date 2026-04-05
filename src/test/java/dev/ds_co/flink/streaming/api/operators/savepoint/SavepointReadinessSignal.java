package dev.ds_co.flink.streaming.api.operators.savepoint;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cross-thread coordination between operators running inside MiniCluster and the test harness.
 * Operators call {@link #markKeyReady} once keyed state (X + Y) has been populated; the test
 * harness polls {@link #allKeysReady} to know when it is safe to trigger a savepoint.
 */
public final class SavepointReadinessSignal {

  private static final Set<String> KEYS_WITH_XY_STATE = ConcurrentHashMap.newKeySet();

  private SavepointReadinessSignal() {}

  public static void markKeyReady(String key) {
    KEYS_WITH_XY_STATE.add(key);
  }

  public static boolean allKeysReady(String... keys) {
    return Arrays.stream(keys).allMatch(KEYS_WITH_XY_STATE::contains);
  }

  public static void reset() {
    KEYS_WITH_XY_STATE.clear();
  }
}
