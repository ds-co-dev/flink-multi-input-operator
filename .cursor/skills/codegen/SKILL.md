---
name: codegen-keyed-multi-input
description: >-
  Regenerates KeyedMultiInputOperator4-25 and KeyedMultiInputOperatorTestHarness3-25 from Java
  scripts. Use when changing operator or harness templates, fixing generated output, or after
  edits to scripts/GenerateOperators.java or scripts/GenerateTestHarnesses.java.
---

# Regenerate KeyedMultiInput operators and harnesses

## Steps

1. Edit the template logic in `scripts/GenerateOperators.java` and/or `scripts/GenerateTestHarnesses.java` as needed.
2. From the repository root, run:
   - `java scripts/GenerateOperators.java`
   - `java scripts/GenerateTestHarnesses.java`
3. Run `./mvnw spotless:apply`.
4. Run `./mvnw clean verify`.

## Output locations

- Operators: `src/main/java/dev/ds_co/flink/streaming/api/operators/KeyedMultiInputOperator{N}.java` for N = 4..25.
- Harnesses: `src/test/java/dev/ds_co/flink/streaming/util/testing/KeyedMultiInputOperatorTestHarness{N}.java` for N = 3..25.

Do not hand-edit generated files; change the scripts instead.
