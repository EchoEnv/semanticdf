package io.semanticdf.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Verifies the platform's `--add-opens` JVM flag set is identical
 * in two locations: the {@code <add.opens.flags>} property in
 * {@code semanticdf-platform/pom.xml} (consumed by surefire's
 * {@code <argLine>}) and {@code semanticdf-platform/.mvn/jvm.config}
 * (read by Maven at JVM startup).
 *
 * <p>The pom comment promises: <i>"a CI test should assert the two
 * stay in sync."</i> This test is that promise.
 *
 * <p>Both sources are extracted as sets of {@code --add-opens=...}
 * tokens. If a maintainer adds a flag to one location but forgets
 * the other, this test fails with a clear diff.
 */
class AddOpensFlagsParityTest {

  private static final Path PLATFORM_ROOT =
      Path.of("").toAbsolutePath();

  private static final Path POM_FILE = PLATFORM_ROOT.resolve("pom.xml");

  private static final Path JVM_CONFIG = PLATFORM_ROOT.resolve(".mvn/jvm.config");

  /** Extracts the {@code --add-opens=...} tokens from a string. */
  private static Set<String> extractFlags(String content) {
    Pattern pattern = Pattern.compile("--add-opens=[^\\s<>\"]+");
    Matcher matcher = pattern.matcher(content);
    Set<String> flags = new TreeSet<>();
    while (matcher.find()) {
      flags.add(matcher.group());
    }
    return flags;
  }

  @Test
  void pomProperty_matchesJvmConfig() throws IOException {
    String pomContent = Files.readString(POM_FILE, StandardCharsets.UTF_8);
    String jvmConfigContent = Files.readString(JVM_CONFIG, StandardCharsets.UTF_8);

    Set<String> pomFlags = extractFlags(pomContent);
    Set<String> jvmConfigFlags = extractFlags(jvmConfigContent);

    assertTrue(
        pomFlags.size() > 0,
        "expected <add.opens.flags> in " + POM_FILE + " to contain --add-opens tokens");
    assertTrue(
        jvmConfigFlags.size() > 0,
        "expected " + JVM_CONFIG + " to contain --add-opens tokens");

    assertEquals(
        pomFlags,
        jvmConfigFlags,
        "Flag set in pom.xml <add.opens.flags> diverges from .mvn/jvm.config. "
            + "Both must stay in sync (Maven reads .mvn/jvm.config before properties are resolved, "
            + "so they cannot be templated from one source).");
  }
}
