# Runtime Quickstart

How to build, test, run, and avoid the traps in semanticdf's toolchain.

This page is the single source of truth for **what runs on what**. It's intentionally
short and opinionated. If something here doesn't match what you see, file an issue.

---

## Supported runtimes

| Component     | Version     | Notes |
|---------------|-------------|-------|
| **JDK**       | 17          | Required. JDK 11 misses some Spark internals; JDK 21 also works |
| **Scala**     | 2.13.18     | Pinned. No 3.x support; no 2.12 support |
| **Spark**     | 3.5.8 (default) · 4.1.1 (`-Pspark4`) | Tests run green on both; no code shims needed |
| **ScalaTest** | 3.2.19      | Pinned for test alignment |
| **Maven**     | 3.9+        | Older versions may choke on `.mvn/jvm.config` parsing |

If you're on a different combination, **stop and check the [known-limitations](known-limitations.md)
page first.** The most common failure modes are listed there.

---

## Build & test

From the repo root:

```bash
mvn test                    # runs 335 library tests on Spark 3.5.8
mvn -Pspark4 test           # runs 335 library tests on Spark 4.1.1
cd semanticdf-mcp && mvn test  # adds 72 MCP tests on top
mvn install -DskipTests     # builds the jar so examples/pipeline/ can use it
```

Both Spark profiles run the **same test suite** — the only difference is which
Spark release is on the classpath. You should see `Tests: succeeded 335` in the
library and `Tests: succeeded 72` in the MCP server (407 in total) either way.

### Cross-version sanity check

Run both profiles before merging anything to `master`:

```bash
mvn test && mvn -Pspark4 test
```

If one fails and the other doesn't, you've hit an API that drifted between
Spark 3.x and 4.x. `DESIGN.md §7` lists which Spark APIs semanticdf touches;
all are stable across the matrix.

---

## Run the CLI tools

The CLI lives in `io.semanticdf.tools.Main` (classpath: target/classes).
It uses `exec:java` — **do not use `scala:run`** (see [Traps](#traps) below).

From the repo root:

```bash
# Generate HTML docs from a YAML model (or a directory of models)
mvn exec:java \
  -Dexec.args="docsgen --path examples/starter/models/ --out docs/index.html"

# Infer a starter YAML model from a data file (needs Spark)
mvn exec:java \
  -Dexec.args="introspect --path data/orders.csv --format csv --model orders"
```

Run with no args to see usage:

```bash
mvn exec:java -Dexec.mainClass=io.semanticdf.tools.Main
```

---

## Run the example projects

semanticdf ships seven reference projects. Each is self-contained — `mvn install`
the parent, `cd` into the directory, and run. Each demonstrates a different pattern.

### The two foundational examples

#### `examples/starter/` — gold-layer-only demo

The canonical "hello world". Loads YAML models, runs **seven queries** including
basic group-by, percent-of-total, joins, time-grain aggregation, filter + aggregate,
top-N via window functions, and month-over-month change via `lag`. PR #10 added a
typed-queries showcase (the `groupByDimensions` / `aggregateMeasures` / sealed
`Compare.Gt` patterns).

```bash
cd examples/starter
mvn scala:run -DmainClass=com.example.starter.Main
```

Note: starter uses `scala:run` deliberately — it doesn't start Spark and doesn't
care about the arg-leak. **Do not copy this command if you're starting Spark.**

#### `examples/pipeline/` — full bronze→silver→gold pipeline

The full BI lifecycle in one repo: messy CSV → ETL cleanup → silver parquet →
declarative YAML queries → gold-layer output.

```bash
cd examples/pipeline
mvn exec:java -Dexec.mainClass=com.example.pipeline.Main
```

The pipeline reads messy CSVs, cleans them, writes silver parquet, then runs
gold-layer queries through YAML models. Output goes to `output/`.

This project ships `.mvn/jvm.config` to handle Java 17 module-system flags
(see [Traps](#traps) §1). You don't need to set any env vars.

### Five domain-specific examples

Each is `mvn scala:run -DmainClass=com.example.<package>.Main` (same invocation
pattern as starter — none of these start Spark in a way that would conflict with
the arg-leak; they all use Scala-managed `local[*]`).

| Template | What it teaches |
|---|---|
| [`examples/hospital/`](../examples/hospital/README.md) | Data **cleansing** (dedup, normalize, fill) before loading into semanticdf; ALOS, 30-day readmission rate |
| [`examples/customer-analytics/`](../examples/customer-analytics/README.md) | RFM segmentation + cohort activity — multi-step calc-of-calc composition |
| [`examples/operations-analytics/`](../examples/operations-analytics/README.md) | Order fulfillment time, on-time rate, anomaly detection (2σ z-score) |
| [`examples/telco-analytics/`](../examples/telco-analytics/README.md) | Telco: monthly ARPU per plan, promotion effectiveness, roaming revenue split |
| [`examples/window-analytics/`](../examples/window-analytics/README.md) | Window functions: top-N per group, period-over-period (MoM), running totals |

Each example has its own `README.md` with the exact run command for its package
name.

---

## Generate browsable docs for any model

```bash
mvn exec:java -Dexec.args="docsgen --path <file-or-dir> --out <output.html>"
```

Opens in a browser as a self-contained page (no CDNs, no JS, embedded CSS).
Works for a single YAML file or a directory of `*.yml`/`*.yaml` files.

---

## Traps

The four gotchas that will cost you a debug session if you don't know them
upfront. All stem from how Scala/Spark/Maven interact.

### 1. Java 17 + Spark needs `--add-opens` flags

Spark 3.5.x touches JDK internals (`sun.nio.ch.DirectBuffer`,
`sun.nio.ch.FileChannelImpl`, etc.) that are sealed in JDK 17. Without
`--add-opens` flags, the driver crashes at `SparkSession.getOrCreate()` with:

```
java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
  cannot access class sun.nio.ch.DirectBuffer (in module java.base)
```

The flags (`--add-opens=java.base/java.lang=ALL-UNNAMED`, etc.) must be set
**at JVM startup**. They are not settable at runtime via `System.setProperty`.

There are three valid ways to set them:

| Where to set | How | When to use |
|---|---|---|
| `.mvn/jvm.config` (one flag per line) | create the file | **preferred** — automatic, version-controlled |
| `MAVEN_OPTS` env var | shell export | local debugging |
| Surefire/scalatest `argLine` | test runner config | when running the test suite |

The `examples/pipeline/` template uses `.mvn/jvm.config`. The semanticdf main
project uses `argLine` for tests (already wired). For ad-hoc tools, use
`MAVEN_OPTS=...--add-opens=...`.

**Why `JAVA_TOOL_OPTIONS` in `exec-maven-plugin`'s `<environmentVariables>` doesn't
work**: that field is accepted but does not propagate to the in-process JVM
that `exec:java` uses (verified v3.5.0, minimised reproducer with
`FOO=[null]` in the forked process). Use `.mvn/jvm.config` instead.

### 2. `mvn scala:run` leaks compiler args

The `scala-maven-plugin`'s `run` goal prepends the plugin-level `<args>`
(compiler flags like `-deprecation`, `-feature`, `-unchecked`) as **program
args** to the JVM. This breaks any tool whose `main` parses args expecting
clean input (e.g. `main.headOption` will see `-deprecation` as the first arg
and fail with "Unknown subcommand: -deprecation").

**Workaround**: don't use `scala:run`. Use `exec:java` for any non-trivial
program. This is why both the CLI tools and `examples/pipeline/` use `exec:java`.

If you must use `scala:run` for a hello-world, prepend your args after `--`:

```bash
mvn scala:run -Dexec.args="docsgen --path models -- --ignored-flag"
```

The `--` *does not* actually stop the leak (also verified). The only
true fix is to switch to `exec:java`.

### 3. Scala 2.13 uses `scala.jdk.CollectionConverters._`, not `scala.collection.JavaConverters._`

The old `scala.collection.JavaConverters` is deprecated in 2.13 (gives
compiler warnings, removed in 3.x). When converting Java collections:

```scala
import scala.jdk.CollectionConverters._    // ← right

// WRONG:
import scala.collection.JavaConverters._    // deprecated
```

### 4. Don't edit `pyproject.toml`, `.git/hooks/*`, or `.github/workflows/ci.yml` without explicit permission

Version bumping, the post-commit hook, and CI are wired together. Touching
them out of order breaks the auto-bump machinery.

---

## "What would you do?" by goal

| You want to… | Run this |
|---|---|
| Confirm the build still works | `mvn test` |
| Verify Spark 4 compat | `mvn -Pspark4 test` |
| Run a single test file | `mvn test -Dtest=DocsGenSpec` |
| Produce the jar | `mvn install -DskipTests` |
| Generate docs for a model | `mvn exec:java -Dexec.args="docsgen --path <dir> --out out.html"` |
| Try the gold-layer example | `cd examples/starter && mvn scala:run -DmainClass=com.example.starter.Main` |
| Try the silver-layer pipeline | `cd examples/pipeline && mvn exec:java -Dexec.mainClass=com.example.pipeline.Main` |
| Browse the 5 domain examples | see [Run the example projects](#run-the-example-projects) above |
| Debug a classpath issue | `mvn dependency:build-classpath` |
| See a stack trace with deps | `mvn -X test -Dtest=DocsGenSpec 2>&1 \| grep -A50 "Build CL"` |

---

## When the runtimes move

If you find yourself needing to bump Scala, Spark, or ScalaTest:

1. Update `pom.xml` properties section
2. Run `mvn test && mvn -Pspark4 test`
3. If anything fails, check `DESIGN.md §7` for the API compatibility list
4. Update `README.md` "Cross-version compatibility" table
5. Update this page's matrix

Do not change more than one major component per PR — collisions in error
messages are hard to attribute otherwise.
