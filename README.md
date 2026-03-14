# flink-multi-input-operator

Keyed multi-input operator for Apache Flink streaming jobs.

**Requirements:** Java 11, Flink 2.2. Uses Maven Wrapper (no Maven install needed).

## Build

    ./mvnw clean package

## Test

    ./mvnw test

## CI

GitHub Actions runs tests on every push/PR. Publish workflow deploys to GitHub Packages when a release is created.

For manual publish: configure `~/.m2/settings.xml` with PAT (scopes: read:packages, write:packages, repo) and run `./mvnw deploy`.
