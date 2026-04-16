# How to contribute to flink-multi-input-operator

Thank you for your intention to contribute to flink-multi-input-operator. External contributions are welcome.

To make the process smooth for maintainers and contributors, there are a few rules to follow.

## Contribution Guidelines

Please open an issue or start from an existing issue before sending a larger change so the approach can be aligned early.

For code changes:

- Keep changes small and targeted to the problem being solved.
- Follow the existing Flink-style APIs and naming used in this repository.
- Do not edit generated sources by hand. Update the generator scripts and regenerate instead.
- Run `./mvnw spotless:apply`.
- Run `./mvnw clean verify`.

For pull requests:

- Explain the problem and the approach clearly.
- Include tests for behavior changes when applicable.
- Keep the branch rebased on the current default branch.
- Make sure CI is green before requesting review.
