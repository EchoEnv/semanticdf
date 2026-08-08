# semanticdf-myname — Template adapter skeleton

> **Dev-zone content.** This module is the canonical skeleton for adding
> a new engine / source-resolver / catalog adapter to the semanticdf
> library. It lives in `adapters/` (parallel to the 6 real adapter
> modules) so the build verifies the template still compiles against the
> current `Engine` / `SourceResolver` / `CatalogAdapter` contracts.

See [`docs/agents/adding-a-new-adapter.md`](../../docs/agents/adding-a-new-adapter.md)
for the full guide.

## What's in this skeleton

| File | Role |
|---|---|
| `pom.xml` | Maven config (parent + semanticdf-core dep) |
| `MyPlatformError.scala` | Sealed ADT (8 cases) for data-plane failures |
| `MyPlatformClient.scala` | Boundary trait + 3 portable data shapes |
| `HttpMyPlatformClient.scala` | JDK HttpClient impl (hand-rolled JSON) |
| `MyPlatformSourceResolver.scala` | `extends SourceResolver` |
| `MyPlatformCatalogAdapter.scala` | `extends CatalogAdapter` (CAS via `version`) |
| `MyPlatformResultEncoder.scala` | `MyPlatformResult` → `PortableQueryResult` |
| `FakeMyPlatformClient.scala` | Hand-driven test fixture |
| `MyPlatformCatalogAdapterSpec.scala` | 12 tests (publish modes × states + boundary) |
| `MyPlatformSourceResolverSpec.scala` | 6 tests (4 ResolvedSource cases) |
| `MyPlatformErrorMappingSpec.scala` | 6 tests (every error case maps correctly) |

## How to use this skeleton

```bash
# 1. Copy the skeleton to a new module
cp -r templates/example-adapter/ adapters/semanticdf-myname/
cd adapters/semanticdf-myname/

# 2. Rename MyPlatform → MyName everywhere
find . -type f -name "*.scala" -exec sed -i 's/MyPlatform/MyName/g' {} +
find . -type f -name "*.scala" -exec sed -i 's/myplatform/myname/g' {} +

# 3. Update the package declaration
sed -i 's|package io.semanticdf.myplatform|package io.semanticdf.myname|g' $(find . -name "*.scala")

# 4. Register the module in parent pom.xml
echo '<module>adapters/semanticdf-myname</module>' >> ../../pom.xml
#    (The template at adapters/semanticdf-template/ is already registered;
#     you only need to register your new copy.)

# 5. Implement the JSON parsing in HttpMyPlatformClient
#    (the template has TODO stubs that return placeholders)

# 6. Run the tests
cd ../.. && mvn -pl adapters/semanticdf-myname test
```

## What needs customization

1. **JSON parsing** (`HttpMyPlatformClient.parse*Result`): the template
   returns placeholder values. You MUST implement the actual JSON
   parsing based on your platform's response shape.
2. **Auth header** (`HttpMyPlatformClient.authHeader`): the template
   has a no-op constructor. Implement the actual auth header injection
   (Bearer / API key / OAuth2 / etc.).
3. **Realm resolution** (`MyPlatformClient.resolveRealmId`): the
   template returns the catalog name as the realm. Most platforms have
   a separate realm list endpoint; implement accordingly.
4. **Engine class** (`MyPlatformEngine extends Engine[Any]`): NOT in
   the template because every engine's SQL dialect is unique. Mirror
   `io.semanticdf.hera.HeraEngine` for a working example.