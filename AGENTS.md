# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this is

Scala/Java (Maven) library implementing openEO processes on the GeoTrellis Spark API.
It is consumed by the Python back-end [openeo-geopyspark-driver](https://github.com/Open-EO/openeo-geopyspark-driver)
through py4j, as the `geotrellis-extensions-*.jar` / `geotrellis-dependencies-*.jar`.
Scala 2.13, Spark 4, GeoTrellis 3.8 (custom snapshot), Java 11 bytecode (CI runs on JDK 21).

## Layout

| Module | Purpose |
|---|---|
| `openeo-geotrellis` | Main module: processes (`OpenEOProcesses`, `OpenEOProcessScriptBuilder`), layer providers, file writers (GTiff/NetCDF/Zarr), UDFs (JEP) |
| `geotrellis-common` | Shared types, e.g. `DataCubeParameters`, `TestConditions` |
| `sar-terrain-correction` | Native `sar_backscatter` implementation |
| `geotrellis-extensions` | Shaded jar used by the Python driver |
| `openeo-dependencies` | Shaded third-party dependency jar |
| `geotrellis-sentinelhub`, `geotrellis-s3-extensions`, `snap-integration`, `openeo-logging`, `geopyspark-geotrellis`, `openeo-geotrellis-healpix` | Supporting modules |
| `testdata` | Git submodule ([geotrellis-testdata](https://github.com/Open-EO/geotrellis-testdata)), binaries stored with Git LFS |
| `docs/` | Design notes (cube alignment, Sentinel-2 angle bands, ...) |

## Build and test

```bash
git submodule update --init --recursive && git -C testdata lfs pull   # test data (needs git-lfs)

mvn -B -q -pl openeo-geotrellis -am install -DskipTests                 # build a module + deps
mvn -B -q -o -pl openeo-geotrellis test -Dtest=TestOpenEOProcessScriptBuilder -Dsurefire.failIfNoSpecifiedTests=false
mvn -B -q -o -pl openeo-geotrellis test -Dtest='TestOpenEOProcessScriptBuilder#testSumBitTiles' -Dsurefire.failIfNoSpecifiedTests=false
```

- Run the smallest test selection that covers your change; the full suite is slow and partly needs network/Terrascope access (`/data/MTDA`) or Sentinel Hub credentials.
- Surefire output lands in `<module>/target/surefire-reports/`; `-q` hides failures, so check the `.txt` report.
- JEP (Python UDF) tests expect a virtualenv at `../venv` relative to the module (`venv/bin`, `venv/lib/python3.8/site-packages/jep`), see the surefire config in the root `pom.xml`. Without it they are skipped or fail with `UnsatisfiedLinkError`.
- Process-graph regression tests (`ProcessGraphRegressionTest`) need Docker and `-DRUN_PROCESS_GRAPH_REGRESSION_TESTS=true`.
- Test classes must match `Test*`, `*Test`, `*Tests` or `*TestCase` to be picked up.

## CI

- GitHub Actions: `.github/workflows/build.yml` (build, tests, PR pre-release tags, releases on `v*` tags).
- Jenkins (VITO): `Jenkinsfile` and `Jenkinsfile.pg_tests`.
- Downstream: openeo-geopyspark-driver CI tests against the latest snapshot jars, so changes here can break the driver build (see pitfalls below).

## Conventions

- Branch from `develop`; never push directly to `develop`. One branch/PR per issue.
- Add a `CHANGELOG.md` entry under `## Unreleased` (Added/Changed/Fixed/Removed) for user-visible changes, linking the issue. Driver issues are referenced as `Open-EO/openeo-geopyspark-driver#NNNN`.
- Keep changes surgical; match the existing style of the file (Scala and Java tests coexist under `src/test/scala` and `src/test/java`).
- If you use `pre-commit`, never run `--all-files` and then `git add -A`: the whitespace/end-of-file fixes in unrelated files (including test reference files such as `ehcek2_reference.json`) get committed and break tests. Only commit files of your change and check `git diff --stat` first.
- Large binary test inputs go into the `testdata` submodule via Git LFS, not into this repo; bump the submodule pointer in the same PR.

## Pitfalls

- **Spark closure serialization.** Process callbacks built by `OpenEOProcessScriptBuilder` are Java-serialized to executors. A lambda that calls an instance method captures the (serializable) builder, which in turn references the composed process: this creates a cycle that fails at deserialization with `ClassCastException: Cannot cast java.lang.invoke.SerializedLambda to scala.Function1`. Put helpers used inside such lambdas in the companion `object`, and cover new callbacks with a Java-serialization round-trip of `builder.generateFunction(context)`.
- **`toString` is part of the contract.** openeo-geopyspark-driver tests assert on e.g. `DataCubeParameters.toString`; changing it requires a matching driver-side test update.
- **Bit cell types.** Arithmetic on `BitCellType` tiles wraps around (1 + 1 = 0); widen to an integer cell type before reducing.
- **Public API used from Python.** Methods called via py4j from the driver (constructors, overloads, setters) are effectively public API: keep old signatures or coordinate the driver change.
