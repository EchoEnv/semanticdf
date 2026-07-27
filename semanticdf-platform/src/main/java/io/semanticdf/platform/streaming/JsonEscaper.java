package io.semanticdf.platform.streaming;

/**
 * Minimal JSON string escaper. Used by code paths that build JSON
 * bodies by hand (rather than via Jackson) — currently
 * {@link StreamingService#auditPayload} and the
 * {@code streaming.restarted} payload built in
 * {@link StreamingService#reconcileAfterJvmCrash} (indirectly via
 * the catalog's run-body construction in {@code StartupReconciler}).
 *
 * <p>Covers the five characters required by RFC 8259 ({"},\\,\b,\f,\n,\r,\t})'s
 * mandatory set: quote, backslash, newline, carriage return, tab.
 * Anything else passes through unchanged.
 *
 * <p>Visible-for-testing — package-private so {@code StreamingServiceTest}
 * and friends can verify the same escaping the production code uses.
 */
final class JsonEscaper {

  private JsonEscaper() {}

  /**
   * Escape a string for inclusion in a JSON value position. Returns
   * the empty string for {@code null}.
   */
  static String escape(String s) {
    if (s == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.toString();
  }
}
