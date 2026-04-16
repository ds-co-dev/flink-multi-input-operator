# flink-multi-input-operator

Keyed multi-input operator for Apache Flink streaming jobs.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

Proudly tested at [Auvik](https://www.auvik.com/). <img alt=";-)" src="https://www.auvik.com/wp-content/uploads/2024/01/icon-wink-portage.svg" height="14">

Use *at your own risk*! 😀 

**Requirements:** Java 11, Flink 2.2. Uses Maven Wrapper (no Maven install needed).

## Purpose

This library is a companion implementation for [FLINK-39131: Multi-Input Processors](https://issues.apache.org/jira/browse/FLINK-39131).

Its goal is to provide a DataStream-level primitive for processing more than two keyed inputs within a single operator. In practice, this is useful for stateful multi-stream patterns such as multi-way joins, where expressing the logic as a chain of binary operators can create excessive intermediate state.

## Background

[FLIP-516: Multi-Way Join Operator](https://cwiki.apache.org/confluence/display/FLINK/FLIP-516%3A%2BMulti-Way%2BJoin%2BOperator) introduced a SQL/Table-runtime solution for multi-way joins in Apache Flink. That work addresses the state-explosion problem at the SQL layer by avoiding binary-join chains and their intermediate state. See more [here](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/tuning/#multiple-regular-joins).

This library explores the corresponding need at the DataStream layer: a built-in primitive that lets users process multiple keyed inputs in one operator without dropping down to Flink's low-level Operator API.

The relationship is:

- FLIP-516 addresses multi-way joins for SQL/Table workloads.
- [FLINK-37481: Multi way join operator](https://issues.apache.org/jira/browse/FLINK-37481) delivered the runtime operator used for that effort.
- FLINK-39131 proposes bringing the same kind of capability to the DataStream API as a reusable multi-input processing primitive.
- This repository exists as a concrete library and proving ground for that direction.

## Scope

This project is intentionally focused on a small, explicit abstraction: a keyed multi-input operator for DataStream jobs.

It is not an Apache Flink module, and it does not aim to mirror the whole internal Operator API. Instead, it packages a higher-level primitive that is easier to use in application code while staying aligned with the motivation behind FLINK-39131.

It is also adjacent to earlier discussions such as [FLIP-17: Side Inputs for DataStream API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-17%3A+Side+Inputs+for+DataStream+API), but it targets a different use case: multiple equally important inputs processed symmetrically within one keyed operator, rather than a main input augmented by side inputs.

## Build

    ./mvnw clean package

## Test

    ./mvnw test

## Code Style

This project follows [Apache Flink's code style conventions](https://nightlies.apache.org/flink/flink-docs-stable/docs/flinkdev/ide_setup/#required-plugins).

Formatting is enforced by **Spotless** (google-java-format) and **Checkstyle** (based on Flink's `tools/maven/checkstyle.xml`). Both run automatically during `mvn verify`.

To auto-fix formatting:

    ./mvnw spotless:apply

### IntelliJ IDEA Setup

1. Install the **google-java-format** plugin ([v1.24.0.0](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/631498)). Install via "Settings" → "Plugins" → gear icon → "Install Plugin from Disk". Make sure to never update this plugin. Enable it under "Settings" → "Other Settings" → "google-java-format Settings".
2. Install the **Checkstyle-IDEA** plugin. Under "Settings" → "Tools" → "Checkstyle", add `tools/checkstyle.xml` as a configuration file.
3. Enable "Optimize imports" and "Reformat code" under "Settings" → "Tools" → "Actions on Save".

## CI

GitHub Actions runs tests on every push/PR. Publish workflow deploys to GitHub Packages when a release is created.

For manual publish: configure `~/.m2/settings.xml` with PAT (scopes: read:packages, write:packages, repo) and run `./mvnw deploy`.
