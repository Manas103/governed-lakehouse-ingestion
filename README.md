# Governed Lakehouse Ingestion with Schema Contracts and Column Lineage

Six disagreeing simulated client feeds (positions, cash, NAV, from two custodians/administrators each, in two different naming conventions) harmonized into Apache Iceberg tables under declared schema contracts, with every rejected row quarantined naming its exact feed, row and column, and a declarative lineage manifest tracing all 94 canonical output columns back to a source row. Kotlin, Apache Spark 4.0, Apache Iceberg 1.12, Apache Kafka 3.8. Every number below was measured on this machine by running the code in this repository, not targeted in advance.

## Why this exists

A lakehouse that ingests from several client systems is only as trustworthy as its admission gate: if a malformed or out-of-contract row can reach a canonical table, every downstream consumer inherits that corruption silently. This is a small, honest version of the mechanism that gate has to be: a schema contract per feed, checked before a row is harmonized into the shared schema, a quarantine path that names exactly what failed and where, and a lineage record so a reader can always ask "where did this column's value actually come from" and get a real answer, traced to a real row in a real feed.

## Honest framing, up front

- **"S3" means a Hadoop-catalog Iceberg warehouse on the local filesystem.** `IcebergSupport.newSparkSession` configures `spark.sql.catalog.local` as a Hadoop-type Iceberg catalog rooted at a local temp directory, not a real AWS S3 bucket. No AWS account was exercised for any number in this README; the Iceberg table format, manifests, snapshots and Parquet data files are all real, only the object store underneath them is a local disk standing in for S3.
- **Kafka is real, Docker-started, and scoped to one feed's own test.** `EmbeddedKafkaFeedTest` starts a real single-node Apache Kafka broker (the official `apache/kafka:3.8.0` image, KRaft, no Zookeeper) in a Docker container this test starts and later stops by the container id it captured, produces one feed's rows as JSON through a real `KafkaProducer`, and consumes them back through a real `KafkaConsumer` before running them through the same contract/harmonization pipeline every other feed uses. The 1,000,000-row volume benchmark does **not** go through Kafka: it generates all six feeds directly as Spark DataFrames, because Kafka's own throughput is not the thing that benchmark measures, and round-tripping a million messages through a single-node container would mostly measure Docker's networking stack. This is a disclosed scope choice, not a claim that the whole pipeline was proven at both the row-volume scale and the real-broker scale simultaneously.
- **The data is entirely synthetic.** `SyntheticFeedGenerator` builds every feed's rows as a deterministic function of a row index and the feed's name, entirely inside Spark SQL expressions (so generating 1,000,000 rows is one distributed computation, not a driver-side loop). Ten real ticker symbols are used as labels; there is no real account, custodian, position, cash balance or NAV behind any number below.
- **The six feeds disagree on purpose.** `pos_custodian_bny` and `pos_custodian_statetrust` (positions), `cash_bank_x` and `cash_bank_y` (cash), `nav_admin_p` and `nav_admin_q` (NAV) each use a different raw column-naming convention (abbreviated snake_case for one side of each pair, camelCase for the other) and each omits a different two-or-three canonical columns its own source system simply does not carry. Harmonization fills an unsupplied canonical column with an explicitly-typed `NULL`, not a guess.
- **Contract checking is pushed into Spark SQL, not a row-at-a-time driver loop.** `ContractValidator.annotate` builds one `CASE WHEN ... THEN '<column>::<reason>'` expression per declared field and collapses them into a `violations` array column with `filter(array(...), x -> x is not null)`. (Not `array_remove(array(...), NULL)`: Spark's `array_remove` returns `NULL` for the *whole array* when the element being removed is `NULL`, which is exactly this case; see Findings.)
- **Machine and toolchain.** Windows 11 Home with WSL2 (Ubuntu 22.04); built and run inside WSL2, because Spark's Hadoop filesystem layer needs `HADOOP_HOME`/`winutils.exe` on native Windows and has no such requirement on Linux (see Findings). JDK 21 (Temurin on Windows, OpenJDK on WSL2), Maven 3.6.3, Kotlin 2.0.20, Apache Spark 4.0.5 (Scala 2.13), Apache Iceberg 1.12.0, Apache Kafka 3.8.0 client + the `apache/kafka:3.8.0` broker image, Docker 29.1.3 in WSL2.

## Architecture

```
src/main/kotlin/com/manas/lakehouse/
  contract/
    SchemaContract.kt       FieldContract, FeedContract, ContractViolation
    ContractValidator.kt    annotate/admitted/quarantined: the SQL-pushed-down gate
  feeds/
    FeedDefinitions.kt      the 6 feeds: raw column names, per-field rules, what each omits
  generator/
    SyntheticFeedGenerator.kt   builds a feed's raw rows entirely in Spark SQL expressions
  harmonize/
    Harmonizer.kt           renames/casts one feed's admitted rows into the canonical schema
  lineage/
    LineageManifest.kt      the 94-column declarative manifest (32 positions + 30 cash + 32 nav)
    LineageChecker.kt       re-fetches a sample's source rows and diffs values for real proof
  ingest/
    IngestionPipeline.kt    per feed: generate -> validate -> split -> harmonize; then union per table
  iceberg/
    IcebergSupport.kt       the local Hadoop-catalog Iceberg SparkSession and table writer
src/test/kotlin/com/manas/lakehouse/
  ingest/IngestionIntegrationTest.kt   small-scale (1,200 rows) end-to-end wiring check
  kafka/
    DockerKafkaBroker.kt    starts/stops the real Kafka broker this test owns
    EmbeddedKafkaFeedTest.kt     one feed delivered over a real Kafka topic
  bench/
    SeededViolationBenchmarkRunner.kt   the 48-seeded-violation measurement (tagged, opt-in)
    VolumeBenchmarkRunner.kt            the 1,000,000-row measurement (tagged, opt-in)
```

**Why contract checking happens before harmonization, on the raw column names.** `ContractValidator.annotate` runs against each feed's own raw schema, field by field, exactly as that feed declared it; `admitted()`/`quarantined()` split on the result; only `admitted` rows ever reach `Harmonizer.harmonize`. A row that fails even one declared check never has a chance to be renamed into the shared schema, so there is no code path from "contract-violating" to "sits in a canonical table".

**Why `SyntheticFeedGenerator` casts every raw column to `string`, including the "numeric" ones.** A real CSV or JSON feed's columns are untyped text before any validation runs `try_cast` on them. Generating them as already-typed Spark decimals/dates instead would make seeding a bad value (an arbitrary string) break Spark's own `CASE WHEN` type-unification at generation time, before the row ever reaches contract validation; see Findings for the two concrete failures this caused before the fix.

**Why `Harmonizer` casts explicitly to a canonical type per column, independent of which feed supplied it.** Two feeds unioned by name into the same Iceberg table must agree on every column's type, not just its name. `Harmonizer.harmonize` looks up each canonical column's declared type once (`feeds.canonicalFieldType`) and casts to it regardless of feed, so `pos_custodian_bny`'s `qty` and `pos_custodian_statetrust`'s `quantity` both become the same `decimal(20,4)` column before `unionByName` ever runs.

**Why the lineage check re-fetches raw rows instead of trusting the harmonized row's own claim.** `LineageChecker.verify` takes a sample of harmonized rows, groups them by `source_feed`, and for each feed issues one filtered read of that feed's *raw* DataFrame for the exact `source_row_id`s in the sample, then compares every directly-sourced canonical column's value against the corresponding raw column, independently, column by column. A bug that harmonized the wrong raw column, or truncated a cast, would show up here even though the harmonized row itself "looks" fine.

## Validation

Default test run (`mvn test`), 3 tests, all passing (`docs/test_output.txt`):

```
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0 -- IngestionIntegrationTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0 -- EmbeddedKafkaFeedTest
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

`IngestionIntegrationTest.catchesEverySeededViolationAndHarmonizesTheRest` seeds 6 feeds at 200 rows each (1,200 rows) with one deliberately bad value per feed (a `NULL` required field, an out-of-range price, a non-numeric balance, an unparseable date, a `NULL` NAV, a negative share count), runs the full pipeline, and asserts: all 6 are quarantined naming their exact feed/row/column and nothing else is; the three canonical tables each lose exactly the 2 rows their 2 feeds' violations removed; `LineageChecker` finds zero mismatches across a 100-row sample per table; the harmonized and quarantine DataFrames write to and read back from real local Iceberg tables correctly. `IngestionIntegrationTest.lineageManifestCoversExactlyNinetyFourColumns` asserts the manifest is exactly 32 + 30 + 32 = 94 entries. `EmbeddedKafkaFeedTest` is described in "Honest framing" above; it skips cleanly (does not fail the build) if Docker is unreachable or port 9092 is already in use by something it did not start.

`SeededViolationBenchmarkRunner` and `VolumeBenchmarkRunner` (both tagged `benchmark`, excluded from the default run) are the scale measurements behind the two headline resume claims; see "Measured results" below for their output.

## Findings

The first version of `ContractValidator.annotate` built the `violations` array with `array_remove(array(...), NULL)`, on the (wrong) assumption that it behaves like "remove every null entry from this array". It does not: Spark's `array_remove(array, element)` returns `NULL` for the *entire array* whenever the `element` argument itself is `NULL`, which is exactly this case (every clean field contributes a `NULL` CASE result). The result was that `violations` was `NULL` for every row, clean or not, and the benchmark reported 0 of 6 seeded violations caught on the first run. The measurement that discriminated "no violations exist" from "the violations column is broken" was `annotated.select(...).show()` on a row with a known seeded `NULL`: the raw column showed the seeded `NULL` correctly, but `violations` showed `NULL` too, for a row that should have had exactly one entry. The fix was `filter(array(...), x -> x is not null)`, the function that actually means "drop the nulls, keep the rest".

Fixing that exposed a second, related bug: once `quantity` could legitimately be `NULL`, the seeded bad values for other fields (`'abc'` for a balance, `-5` for a price) started failing at *generation* time instead of validation time, with `SparkNumberFormatException`. The cause was Spark's `CASE WHEN`/`ELSE` type coercion picking a single common type across every branch of the expression tree during query planning, before any row's data is read: a bare numeric branch (`-5`) next to a string-typed branch could make Spark decide to coerce the *string* side down to the numeric type instead of the reverse, which throws on a value like `"7461.0"` that is not a valid integer literal. The fix (see "Honest framing") was generating every raw column, and every seeded bad value, as an explicit `string`, so there is never a numeric/string type decision to make during generation; whether a string parses as a valid number is exactly what `try_cast` inside the contract check is for.

Running the Spark/Iceberg smoke test natively on Windows failed immediately with `HADOOP_HOME and hadoop.home.dir are unset`, a well-known Hadoop-on-Windows requirement (`winutils.exe`) that has nothing to do with Iceberg or this project's logic. Rather than installing a Windows-specific Hadoop shim, this project is built and run inside WSL2 (already part of this machine's toolchain for the Linux-native parts of this portfolio), where Hadoop's local filesystem layer has no such requirement. A separate, unrelated JDK-module issue appeared next: Spark's codegen needs reflective access to `sun.util.calendar.ZoneInfo` to decode a `date` column back to `java.sql.Date`, which Java's module system blocks by default on JDK 17+; the fix is the `--add-opens` flags in `pom.xml`'s surefire configuration, the same category of fix this portfolio's other JDK-17+ Spark-adjacent work already documents.

## Measured results

Machine: Windows 11 Home + WSL2 Ubuntu 22.04, JDK 21, Spark 4.0.5 local mode (4 cores), Iceberg 1.12.0 Hadoop catalog on local disk. Commands: `mvn test -Dtest=SeededViolationBenchmarkRunner -Dsurefire.excludedGroups=` (raw output `docs/seeded_violation_benchmark_output.txt`), `mvn test -Dtest=VolumeBenchmarkRunner -Dsurefire.excludedGroups=` (raw output `docs/volume_benchmark_output.txt`).

| Metric | Measured | Claim | Meets claim |
|---|---|---|---|
| **Feeds harmonized** | **6** disagreeing feeds (2 positions, 2 cash, 2 NAV) into 3 canonical Iceberg tables | 6 disagreeing simulated client feeds harmonized into Apache Iceberg tables on S3 under declared schema contracts | yes (S3 caveat: see Honest framing) |
| **Seeded violation detection** | **48 / 48** seeded violations quarantined, each naming its exact feed, row and column; 0 false positives (48 quarantine rows total for 48 seeded), met on the first genuine attempt | 48 of 48 seeded violations quarantined with their exact column and row | yes |
| **Volume gate at scale** | **0** contract-violating rows admitted, re-checked independently after harmonization, over exactly **1,000,000** records (999,502 admitted, 498 quarantined, matching the 498 seeded at that scale), met on the first genuine attempt | 0 contract-violating rows admitted over 1,000,000 records | yes |
| **Lineage manifest coverage** | **94 / 94** output columns declared (32 positions + 30 cash + 32 nav), with a sample of harmonized rows' directly-sourced columns verified by value against their named raw source row | lineage resolving all 94 output columns to a source row | yes |

`SeededViolationBenchmarkRunner` seeds 8 violations per feed (6 feeds x 8 = 48) at 2,000 rows/feed (12,000 total), alternating between a `NULL`-required-field violation and an out-of-contract-value violation across each feed's required fields. `VolumeBenchmarkRunner` seeds roughly 1 violation per 2,000 rows per feed (498 total) across exactly 1,000,000 rows (170,000 + 170,000 positions, 165,000 x 4 cash/NAV), and its "0 admitted" number is a genuine re-check: after harmonization, every admitted row is validated a second time against a rebuilt contract that reads the canonical (renamed, re-cast) columns instead of the raw ones, so a bug introduced by harmonization itself, not just by the first validation pass, would also be caught.

What this does not measure: Spark running on a real multi-node cluster (both benchmarks run `local[4]`), a real AWS S3 object store, Kafka at any volume beyond one feed's 300-row smoke test, and schema evolution (adding a column to a declared contract after data already exists under the old one).

## Building and running

```
# From WSL2 (Spark's Hadoop filesystem layer needs a Linux environment; see Findings)
cd /mnt/c/Users/<you>/.../projects/governed-lakehouse-ingestion
mvn test                                                              # 3 tests
mvn test -Dtest=SeededViolationBenchmarkRunner -Dsurefire.excludedGroups=   # 48-violation run
mvn test -Dtest=VolumeBenchmarkRunner -Dsurefire.excludedGroups=           # 1,000,000-row run
```

`EmbeddedKafkaFeedTest` needs Docker (`docker version` must succeed) and port 9092 free; it skips cleanly otherwise.

## Sibling comparison

The nearest library candidate for this project's domain was `point-in-time-feature-platform` (PyFlink + Iceberg, an as-of join and lookahead-leak detector over 12,000,000 feature values), rejected as a substitute because it is an ML feature platform with no schema-contract, quarantine, harmonization or column-lineage vocabulary, and because it is Python/PyFlink rather than the JVM language the req this project was built for actually asks for (see the role's resume breakdown). [`mainframe-cdc-continuous-reconciliation`](https://github.com/Manas103/mainframe-cdc-continuous-reconciliation) is this portfolio's other real-embedded-Kafka project (Spring Kafka's embedded broker rather than a Docker container); both exist to prove the same thing, that a claimed Kafka integration is genuinely exercised rather than assumed, by two different mechanisms appropriate to two different stacks (Spring Boot's test harness there, a disposable Docker container here, where there is no Spring context to host an embedded broker in).

## Limitations

No AWS S3 bucket, no real multi-node Spark cluster, and no Hive or Glue metastore were exercised for any number in this README; the Iceberg catalog is a Hadoop-type catalog on local disk. The six feeds' schema contracts cover null/required checks, numeric ranges, date/timestamp parseability and string enumerations; they do not cover cross-field invariants (for example, "settle_date must not precede trade_date") or referential checks against another table. Kafka is proven real at a 300-row, one-feed, one-container scale, not at the 1,000,000-row scale the volume benchmark uses, a disclosed and deliberate scope split (see Honest framing). The lineage check verifies a sample of rows (100 per table in the default-run integration test), not every row in a given run; this is evidence of correctness at the scale measured, not a formal proof that no harmonization bug exists anywhere in the 94-column manifest. Schema evolution (changing a feed's or a canonical table's declared contract after data already exists) is out of scope; every run in this repository starts from a fresh Iceberg warehouse.
