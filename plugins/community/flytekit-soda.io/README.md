# Soda.io Flyte Plugin

This plugin integrates Flyte with [Soda.io](https://soda.io/), enabling users to run data quality checks as part of their Flyte workflows. With this plugin, data engineers and analysts can embed data monitoring tasks directly into Flyte, improving data reliability and visibility.

## Features

- **Soda.io Integration**: Leverages Soda.io for running data quality scans within Flyte tasks.
- **Configurable Scan Definitions**: Users can define custom scan configurations to monitor data quality based on Soda.io's definitions.
- **API Integration**: Enables configuration of Soda.io credentials and other Soda-specific settings for secure and dynamic data quality checks.

## Installation

This plugin is intended to be installed as part of the Flyte ecosystem. To install it, ensure you have Flytekit installed:

```bash
pip install flytekit