# flink-multi-input-operator

## Purpose

Use this file as agent guidance, not as full project documentation. Keep it short, accurate, and limited to conventions that affect implementation decisions.

## Baseline

- Java 11
- Apache Flink 2.2
- Maven wrapper: `./mvnw`
- Main package root: `dev.ds_co.flink`

## Working rules

- Follow existing Flink-style APIs and naming instead of inventing parallel abstractions.
- Prefer small, targeted changes that fit the current structure.
- Do not add comments to production code or tests.
- Do not add new dependencies unless existing ones are clearly insufficient.

## Formatting and validation

- Formatting is enforced by Spotless with `google-java-format`.
- Style checks are enforced by Checkstyle in `tools/checkstyle.xml`.
- Avoid star imports and trailing whitespace.
- Use `./mvnw spotless:apply` to fix formatting.
- Use `./mvnw clean verify` before considering work complete.

## Libraries to reuse

- Use JUnit 5 for new or touched tests. Prefer migrating old tests rather than mixing styles.
- Use `flink-test-utils` for integration and MiniCluster-style tests before adding any extra test libraries.
- Lombok is available and should be used only for simple test fixtures or POJOs.
- Check current dependencies in `pom.xml` before proposing new ones.

## Generated KeyedMultiInput sources

- `KeyedMultiInputOperator4.java` through `KeyedMultiInputOperator25.java` come from `scripts/GenerateOperators.java`.
- `KeyedMultiInputOperatorTestHarness3.java` through `KeyedMultiInputOperatorTestHarness25.java` come from `scripts/GenerateTestHarnesses.java`.
- Do not edit those generated files by hand. Change the scripts, run `java scripts/GenerateOperators.java` and/or `java scripts/GenerateTestHarnesses.java` from the repo root, then `./mvnw spotless:apply` and `./mvnw clean verify`.
- For the full regeneration workflow, use the project skill `codegen-keyed-multi-input` under `.cursor/skills/codegen/`.

## Testing guidance

- Prefer deterministic tests over timing-sensitive ones.
- For integration-style Flink tests, prefer the standard Flink testing utilities already used in the repo.
- Keep test fixtures simple and place them in the existing `testing` package structure.

## Useful commands

- `./mvnw test`
- `./mvnw spotless:apply`
- `./mvnw clean verify`
