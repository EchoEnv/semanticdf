# semanticdf — developer targets
#
# Both targets assume Maven is on PATH (it is, since semanticdf builds with mvn).
# Run from the project root.

# Iterate over each `examples/<name>/models` directory. The leading `@` suppresses
# recipe echo so the output is just the work each command does.
EXAMPLES := $(wildcard examples/*/models)

# ---------------------------------------------------------------------------
# okfgen-build — compile main sources so `mvn exec:java` can find classes.
#
# `mvn exec:java` does NOT auto-compile; if target/classes/ is empty (clean
# checkout, fresh CI runner), the goal fails with an opaque "An exception
# occurred while executing the Java class" message. Both okfgen and
# okfgen-check depend on this so they work from a clean checkout.
# ---------------------------------------------------------------------------
.PHONY: okfgen-build
okfgen-build:
	@mvn -q compile

# ---------------------------------------------------------------------------
# okfgen — regenerate the OKF reference bundle into docs/agents/reference/.
#
# This rewrites the checked-in bundle to match the current state of the YAMLs.
# Pair it with `git add docs/agents/reference && git commit` to fix a drift PR.
# ---------------------------------------------------------------------------
.PHONY: okfgen
okfgen: okfgen-build
	@set -e; \
	for d in $(EXAMPLES); do \
	  name=$$(basename $$(dirname "$$d")); \
	  echo "  okfgen: $$name"; \
	  mvn -q exec:java -Dexec.mainClass=io.semanticdf.tools.Main \
	    -Dexec.args="okfgen --path $$d --out docs/agents/reference/$$name"; \
	done
	@echo "Bundle regenerated. Verify with: make okfgen-check"

# ---------------------------------------------------------------------------
# okfgen-check — assert docs/agents/reference/ is in sync with the YAMLs.
#
# Generates into a temp dir, then `diff -ru` against the checked-in bundle.
# Exits 0 if in sync, 1 if drift. Used by CI; safe to run locally.
#
# Timestamp lines are EXCLUDED from the comparison (-I flags). OkfGen embeds
# the YAML file's last-git-commit timestamp in two places — the frontmatter
# `timestamp:` field and the log.md `## <ISO-date>` header. These are derived
# from git history, which is inherently non-reproducible across:
#   - pre-commit generation (the YAML is dirty; git returns the PRIOR commit)
#   - squash merges (GitHub creates a new commit with a new timestamp)
# So the check verifies CONTENT only, not volatile metadata timestamps.
# ---------------------------------------------------------------------------
.PHONY: okfgen-check
okfgen-check: okfgen-build
	@TMP=$$(mktemp -d) && DIFF=$$(mktemp) && trap "rm -rf $$TMP $$DIFF" EXIT && set -e; \
	for d in $(EXAMPLES); do \
	  name=$$(basename $$(dirname "$$d")); \
	  mvn -q exec:java -Dexec.mainClass=io.semanticdf.tools.Main \
	    -Dexec.args="okfgen --path $$d --out $$TMP/$$name"; \
	done; \
	if diff -ru -I '^timestamp: ' -I '^## [0-9]' "$$TMP" docs/agents/reference/ > "$$DIFF" 2>&1; then \
	  echo "okfgen-check: bundle is in sync (content; timestamps excluded)."; \
	  exit 0; \
	else \
	  echo "okfgen-check: bundle is OUT OF DATE."; \
	  echo "----- diff (truncated to 80 lines) -----"; \
	  head -80 "$$DIFF"; \
	  echo "----------------------------------------"; \
	  echo "Run 'make okfgen' locally, then 'git add docs/agents/reference && git commit' to fix."; \
	  exit 1; \
	fi
