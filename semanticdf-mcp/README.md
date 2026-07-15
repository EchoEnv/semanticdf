# semanticdf-mcp

The MCP server for semanticdf. Exposes the 5 tools defined in
[`docs/agents/mcp-contract.md`](../docs/agents/mcp-contract.md) to any
MCP-compatible client (Claude Desktop, Cursor, Continue, etc.) over
stdio.

## Status

**MCP-1a**: server skeleton + `list_models`. The other 4 tools
(`describe_model`, `query`, `explain`, `introspect`) ship in subsequent PRs.

The contract is the source of truth — this server implements against it
incrementally.

## Build

```bash
# From the repo root, install the parent library to your local ~/.m2
mvn install -DskipTests

# Build this module
cd semanticdf-mcp
mvn package
```

## Run

```bash
# Start the server (blocks; reads JSON-RPC on stdin, writes to stdout)
mvn exec:java -Dexec.mainClass=io.semanticdf.mcp.Main \
  -Dexec.args="--models ../examples/starter/models/ --data ../examples/starter/data-config.yaml"
```

Both arguments are required:

| Argument | What |
|---|---|
| `--models <dir>` | directory of `*.yml` model files (passed to `YamlLoader.loadDir`) |
| `--data <file>`  | data-config YAML (the `data:` block, see below) |

### Data config format

```yaml
data:
  flights_csv:
    path: data/flights.csv
    format: csv              # parquet | csv | json | delta
    readOptions:
      header: "true"
      inferSchema: "true"
```

The server reads every entry once at startup and exposes each one under
its name to the model layer. Names must match the `table:` references in
your `*.yml` files.

## Test

```bash
mvn test
```

The current test suite covers the `list_models` handler (unit test only —
the JSON-RPC transport is covered by the upstream MCP SDK; the wire
contract is documented in `docs/agents/mcp-contract.md`).

## Architecture

```
Main              →  parses CLI, builds SparkSession, owns process lifecycle
Models            →  loads YAML model dir + data-config block into a Map
Server            →  wires the MCP SDK, stdio transport, tool registration
handlers/
  ListModels      →  pure function: Models → Envelope
Json              →  case classes for the result envelope
```

Every tool handler is a pure function from its typed input to an
`Envelope[T]`. The SDK adapter in `Server.registerListModels` is the only
place that bridges JSON-RPC to Scala types; everything below it is
transport-agnostic and unit-testable.
