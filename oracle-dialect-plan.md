# Oracle dialect plan

Tracking issue: [#531](https://github.com/apache/pekko-persistence-r2dbc/issues/531) (split out of #94).

Status: blocked on the open questions in #531. No implementation work starts until the licensing question is answered.

## Open questions

1. **Licensing.** `oracle-r2dbc` (UPL 1.0 / Apache 2.0) depends on `ojdbc11`, licensed under the Oracle Free Use Terms and Conditions (FUTC), which is not on the ASF resolved-licence list. The proposal is to keep both as test-scope dependencies only, following apache/pekko-persistence-jdbc#8. Waiting for advice in #531.
2. **Minimum Oracle version.** 23ai is proposed. 19c would need `NUMBER(1)` instead of `BOOLEAN`, no `CREATE TABLE IF NOT EXISTS`, and repeated expressions in `GROUP BY`.

## Approach

Follow the MySQL dialect: add `Dialect.Oracle` and Oracle subclasses of the journal, query, snapshot and durable state DAOs.

Oracle runs with the same requirements as MySQL: `use-app-timestamp = on` and `db-timestamp-monotonic-increasing = on`. That avoids most of the Postgres-specific SQL:

- With app timestamps, the journal ignores the inserted row's returned value (`JournalDao` write path), so the missing SQL `RETURNING` clause doesn't matter.
- With monotonic timestamps, the `'1 microsecond'::interval` subselects are not used.
- Tags are only written and read back, never queried in SQL, so the MySQL JSON helpers (`MySQLJournalDao.tagsToJson` / `tagsFromJson`) can be reused.

## Steps

### 1. Spike: confirm driver behaviour

Run `oracle-r2dbc` against `gvenzl/oracle-free:23-slim-faststart` and check:

- `Instant` binds to and reads back from a `TIMESTAMP(6)` column. If not, the Oracle DAOs need an `Instant` ↔ `LocalDateTime` / `OffsetDateTime` conversion at bind and read points.
- Payloads bound as `Array[Byte]` work with `BLOB` columns, and what `Row.get` returns for them.
- `Row.get("tags")` finds the column even though Oracle upper-cases unquoted identifiers.
- Positional `?` bind markers work.
- `Statement.add()` batch inserts work.
- `''` reads back as `NULL`.
- The dialect needs no Oracle driver classes at compile time, so the dependency can stay test-scope only.

### 2. Core wiring

- `R2dbcSettings.scala`: add `case object Oracle` to `Dialect`, parse `"oracle"`, update the "Supported dialects" error message.
- `Sql.scala`: Oracle keeps `?` bind markers unchanged, like MySQL.
- `JournalDao`, `QueryDao`, `SnapshotDao`, `DurableStateDao`: add `case Dialect.Oracle` to each `fromConfig`.
- `project/Dependencies.scala`: add `oracle-r2dbc` in test scope (scope subject to the licensing answer).
- `ConnectionFactoryProvider`: confirm `database` maps to the Oracle service name and the driver name is `oracle`.

### 3. Oracle DAOs

New `oracle` packages next to `mysql`:

| DAO | Changes |
|---|---|
| `OracleJournalDao` | Settings check, as `MySQLJournalDao.settingRequirements` does. Timestamp SQL `SYS_EXTRACT_UTC(SYSTIMESTAMP)`. Insert without `RETURNING`. JSON tags. A null `event_ser_manifest` / `adapter_manifest` reads back as `""`. |
| `OracleQueryDao` | `LIMIT ?` becomes `FETCH FIRST ? ROWS ONLY`. Behind-current-time condition uses `NUMTODSINTERVAL`. Bucket query computes epoch seconds without `extract(EPOCH ...)::BIGINT`. Same paging change in `persistenceIds` queries. `currentDbTimestamp` uses `Instant.now()`. |
| `OracleSnapshotDao` | `ON CONFLICT` upsert becomes `MERGE INTO ... USING (SELECT ... FROM dual)`. `LIMIT 1` becomes `FETCH FIRST 1 ROWS ONLY`. A null manifest reads back as `""`. |
| `OracleDurableStateDao` | Same bucket, slices-range, paging, tag and manifest changes. Insert and update need no changes because they don't use `ON CONFLICT`. |

SQL shared by `OracleQueryDao` and `OracleDurableStateDao` (buckets, slices range, persistence id paging) goes in an `OracleUtils` object, like `H2Utils`. The JSON tag helpers stay in `MySQLJournalDao`; moving them is a separate refactor.

### 4. DDL

`ddl-scripts/create_tables_oracle.sql` and `ddl-scripts/drop_tables_oracle.sql`:

- `VARCHAR2(255)`, `NUMBER(10)` / `NUMBER(19)`, `TIMESTAMP(6)`, `BLOB`, `BOOLEAN DEFAULT FALSE`.
- `tags` as `CLOB CHECK (tags IS JSON)`.
- Manifest columns nullable, because Oracle stores `''` as `NULL`.
- Same primary keys and slice indexes as MySQL.

### 5. Tests

- Add an `oracle` case to `TestConfig`, mirroring `mysql`.
- Review every MySQL-gated test (`PersistTagsSpec`, `EventsByPersistenceIdGapSpec`, `DurableStateStoreAdditionalColumnSpec`, `DurableStateStoreChangeHandlerSpec`, `R2dbcExecutorSpec`, `BatchedJournalDialectGate` and others) and decide whether Oracle shares the gate.
- New directional tests:
  - an empty manifest round-trips for events, snapshots and durable state;
  - the snapshot `MERGE` updates an existing row and inserts a new one;
  - the settings check rejects `use-app-timestamp = off`;
  - `"oracle"` parses to `Dialect.Oracle`.
- Full suite: `sbt -Dpekko.persistence.r2dbc.dialect=oracle test`.

### 6. Docker and CI

- `docker/docker-compose-oracle.yml` with `gvenzl/oracle-free:23-slim-faststart`, an application user and a healthcheck.
- `test-oracle` job in `.github/workflows/build-test.yml`, modelled on `test-mysql`. It needs the default schema plus `database1` and `database2`; in Oracle these are separate users or PDBs, which needs working out.

### 7. Docs

Update `getting-started.md` and `config.md`: Oracle setup, required settings, minimum version, and that users add the `oracle-r2dbc` driver themselves.

### 8. Before the PR

- `scalafmt`, `sbt javafmtAll`, `sbt headerCreateAll` for new files.
- `sbt +mimaReportBinaryIssues`: adding a `case object` to the sealed `Dialect` should be additive.
- Scala 2.13 / Scala 3 compatible syntax.
- Commit and PR bodies in the `AGENTS.md` format, with `Tests` and `References`.

## Out of scope

- The migration tool: `MigrationToolDao` uses `ON CONFLICT` unconditionally, and MySQL isn't supported there either.

## Risks

In the order the spike should check them:

1. `Instant` mapping for timestamp columns.
2. Empty manifests coming back as `NULL`.
3. `MERGE` correctness when two writers upsert the same snapshot.
4. Creating `database1` / `database2` in the CI container.
