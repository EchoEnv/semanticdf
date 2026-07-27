package io.semanticdf.platform.streaming;

/**
 * Encapsulates the audit-event emission path so the production-side
 * cross-service call to {@code AuditService} is testable without a
 * Restate handler context.
 *
 * <p>The default production implementation ({@link RestateAuditSink})
 * calls {@code Restate.virtualObject(AuditService.class, tenant).append(...)}
 * which journals the call and replays the cached response on retry.
 *
 * <p>Tests substitute a recording or no-op sink to exercise handlers
 * that emit audit events without booting a Restate runtime. This
 * eliminates the previous band-aid pattern of catching the SDK's
 * {@code "Restate methods must be invoked from within a Restate handler"}
 * exception to suppress failures in test contexts — that pattern
 * masked REAL production failures (network blip, transient Restate
 * issue, AuditService back-pressure, NPE in payload) and caused
 * silent audit-log gaps with a healthy-looking {@code STATUS}.
 *
 * <p>With {@code AuditSink}, a failure to emit in production propagates
 * naturally to the caller's try/catch (which already handles
 * {@code STATUS=failed-restart} updates) — operators see both the
 * missed audit and the failure marker.
 */
public interface AuditSink {

  /**
   * Append an audit event to the tenant's audit log.
   *
   * @param tenant the audit tenant (P1: always "default")
   * @param eventType namespace + action (e.g. {@code "streaming.started"})
   * @param ts epoch-millis at which the event occurred
   * @param dedupHash a SHA-256 hex digest used by the audit service
   *                  for idempotency across retries
   * @param payload the event body as a JSON string
   */
  void emit(
      String tenant,
      String eventType,
      long ts,
      String dedupHash,
      String payload);
}
