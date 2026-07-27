package io.semanticdf.platform.streaming;

import dev.restate.sdk.Restate;
import io.semanticdf.platform.audit.AuditService;

/**
 * Production {@link AuditSink}: journals each emit via a Restate
 * cross-service call to {@code AuditService.append}. On retry, the
 * journaled response is replayed so the audit log does not double-count.
 *
 * <p>Failures (network blip, transient Restate issue, AuditService
 * back-pressure, NPE in payload) propagate to the caller's try/catch.
 * The caller marks {@code STATUS=failed-restart} so operators see the
 * failure in {@code getStatus()} — this is correct behavior: a missed
 * audit is an operator-visible event, not a silent failure.
 */
public final class RestateAuditSink implements AuditSink {

  @Override
  public void emit(
      String tenant,
      String eventType,
      long ts,
      String dedupHash,
      String payload) {
    Restate.virtualObject(AuditService.class, tenant)
        .append(
            new AuditService.AuditEventRequest(tenant, eventType, ts, dedupHash, payload));
  }
}
