package com.walletflow.common.outbox.interfaces;

import com.walletflow.common.outbox.OutboxEvent;

import java.util.List;

public interface OutboxReader {
  List<OutboxEvent> getBatch();
}
