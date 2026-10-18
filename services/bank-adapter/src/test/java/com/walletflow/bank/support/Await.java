package com.walletflow.bank.support;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** Chờ một điều kiện bằng cách poll tới hạn chót — không bao giờ ngủ cố định để "đợi cho chắc". */
public final class Await {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private Await() {
    }

    public static void until(BooleanSupplier condition, String description) {
        until(condition, description, DEFAULT_TIMEOUT);
    }

    public static void until(BooleanSupplier condition, String description, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
        throw new AssertionError("Điều kiện không đạt trong " + timeout + ": " + description);
    }
}
