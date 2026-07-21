# sdf — a CLI consumer for the semanticdf REST API

A command-line client for talking to a running [semanticdf](https://github.com/EchoEnv/semanticdf)
REST server. Run as `sdf` (or `semanticdf`) from your terminal.

**sdf is a thin client.** It does no Spark work, no model loading — it just makes
HTTP requests to a semanticdf server and pretty-prints the responses. So it's
fast (~0.5s startup), dependency-light (jackson-databind + scala-library only),
and a faithful probe for the REST contract.

## Install

Requirements: JDK 17+, Maven 3.6+.

```bash
cd examples/cli-consumer
mvn -q compile                            # one-time build

# Add both names to your PATH (optional but recommended)
ln -s "$(pwd)/bin/sdf" /usr/local/bin/sdf
ln -s "$(pwd)/bin/semanticdf" /usr/local/bin/semanticdf
```

The `bin/sdf` wrapper builds the classpath once and caches it; subsequent
invocations start in ~0.5s.

## Start a server to talk to

`sdf` is a **client** — it talks to a REST server that you run separately. To
start the server (from the `semanticdf-mcp` module):

```bash
cd semanticdf-mcp
mvn -q exec:java \
  -Dexec.mainClass=io.semanticdf.mcp.Main \
  -Dexec.args="--models ../examples/starter/models/ \
               --data ../examples/starter/data-config.yaml \
               --okf-bundle /tmp/okf/ \
               --transport rest --rest-port 8999"
```

Then point `sdf` at it:

```bash
export SDF_URL=http://localhost:8999     # default if unset
sdf list
```

## Usage

```
sdf list                            list available models
sdf describe <model>                show dimensions/measures/filters/joins
sdf query <model> -d <dim> -m <m>   run a semantic query, print a table
sdf explain <model> -d <dim> -m <m> show the semantic plan (no execution)

# query/explain options:
  -d, --dim <name>                dimension (repeatable)
  -m, --measure <name>            measure (repeatable)
  -o, --order <field[:asc|desc]>  order by field (repeatable; asc default)
  --limit <n>                     row limit

# global options:
  --url <base>                    server URL (default $SDF_URL or http://localhost:8080)
  --json                          print raw JSON response
  -h, --help                      show this help
  -v, --version                   print version
```

## Examples

```bash
$ sdf list
MODEL     DESCRIPTION
--------  ---------------------------------------
carriers  Airline carrier reference data (lookup)
flights

$ sdf describe flights
Model:        flights
Version:      0

Dimensions:
NAME           EXPR
-------------  -------------
carrier        carrier
carriers.hub   carriers.hub
carriers.name  carriers.name
flight_date    flight_date
hub            hub
name           name
origin         origin

Measures:
NAME              KIND  EXPR
----------------  ----  ----------------------------------------
avg_distance      calc  total_distance / flight_count
avg_passengers    calc  total_passengers / flight_count
flight_count      base  count(1)
pct_of_total      calc  total_passengers / all(total_passengers)
total_distance    base  sum(distance)
total_passengers  base  sum(passengers)

Joins:
NAME                  LEFT     RIGHT     KEYS
--------------------  -------  --------  -------
One_flights_carriers  flights  carriers  carrier

$ sdf query flights -d origin -m total_distance -o total_distance:desc --limit 5
origin  total_distance
------  --------------
LAX     17896
JFK     14516
SFO     6261
BOS     6034
SEA     5241

5 rows

$ sdf explain flights -d carrier -m flight_count
PLAN SUMMARY
────────────
  table:   flights + carriers
  group by: carrier
  compute:  flight_count
...
```

## Streaming models over `sdf`

The `sdf` binary is **model-only** with respect to streaming. Lifecycle (start / stop / hold a stream for an unbounded time) is the operator's program — there is no `sdf start`, `sdf stop`, no implicit streaming query. What the five verbs (`list` / `describe` / `query` / `explain` / `introspect`) DO is interact with streaming-rooted models through their static schema, identically to batch:

```bash
# Streaming model wired into the example data-config
$ sdf list
MODEL     DESCRIPTION
--------  --------------------------------------
flights   Flight facts ...
events    Real-time events arriving on the events topic.

$ sdf describe events
Model:        events
Version:      0
Source table: events_stream          ← streaming read name

Dimensions:
NAME             EXPR
---------------  ----------------
event_type       type
timestamp_bucket timestamp

Measures:
NAME         KIND  EXPR
-----------  ----  --------
event_count  base  count(1)
total_value  base  sum(value)

$ sdf query events -d event_type -m event_count
ERROR streaming-terminal: groupBy(...).aggregate(...) requires a window spec
in StreamingQueryOptions (set StreamingQueryOptions.window)
```

The error message is the *correct* answer, not a silent failure. `sdf query` ran the streaming terminal's validator against the op tree — the same validator the library runs — and it correctly rejected an aggregation against a streaming model that has no window spec (`sdf` doesn't carry operator-side `StreamingQueryOptions`, so aggregation is operator-only).

For streaming models, prefer filter-only queries:

```bash
$ sdf query events --where "event_type = 'deploy'"
event_type
---------
deploy
```

(filter-only returns rows matching the filter at the moment of the call; useful for spot-checks, *not* a continuous tail.)

The streaming terminal (`model.toStreamingQuery(spark, cfg)`) lives in the operator's program. The `sdf` CLI has no opinion on lifecycle. For the canonical operator workflow — opening the source, constructing `StreamingConfig`, calling `toStreamingQuery`, running for N seconds, calling `.stop()` — see [`examples/streaming-events`](../streaming-events/).

## Exit codes

| Code | Meaning |
|-----:|---------|
| 0    | success |
| 1    | server returned a domain error (e.g. MODEL_NOT_FOUND) |
| 2    | usage error (unknown flag, missing args) |
| 3    | transport error (can't connect to server) |

## What building this surfaced

`sdf` was built to **probe the REST API as a real client would**. As of v0.1.3
it has fulfilled that purpose and surfaced two issues, now both fixed:

1. `order_by` over REST was broken (regression from PR #54's Jackson Scala
   module) — fixed in PR #56.
2. `describe_model` `expr` serialised as opaque lambda addresses — fixed in
   PR #58; `Dimension`/`Measure` now carry `exprString`.

`sdf` continues to serve as a **regression witness for the REST contract**:
re-run it against the live server after any change to `RestServer.scala` or
the `Envelope` schema. Its `maskExpr` heuristic is kept as a graceful-
degradation hook for older server versions.

## Why a separate CLI module, not bundled in semanticdf-mcp

The CLI is a **client**, not a server. Keeping it as a separate `examples/`
entry makes that boundary explicit and demonstrates the "any HTTP client can
drive the REST API" promise of the REST transport. It depends only on
jackson-databind + scala-library — no Spark, no semanticdf library, no MCP SDK.
