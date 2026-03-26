# flink-multi-input-operator

Keyed multi-input operator for Apache Flink streaming jobs.

**Requirements:** Java 11, Flink 2.2. Uses Maven Wrapper (no Maven install needed).

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
