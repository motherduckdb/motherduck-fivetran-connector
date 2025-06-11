---
name: MotherDuck
title: Fivetran for MotherDuck | Configuration requirements and documentation
description: Connect data sources to MotherDuck in minutes using Fivetran. Explore documentation and start syncing your applications, databases, events, files, and more.
menuPosition: 80
---

# MotherDuck {% badge text="Partner-Built" /%} {% availabilityBadge connector="motherduck" /%} 

[MotherDuck](https://motherduck.com/) is a DuckDB-powered serverless data warehouse with a unique architecture that combines the power and scale of the cloud with the efficiency and convenience of DuckDB.
Fivetran supports MotherDuck as a destination.

-----

{% partial file="destinations/saas-supported-deployment-models.template.md" /%}

-----

## Setup guide

Follow our [step-by-step MotherDuck setup guide](/docs/destinations/motherduck/setup-guide) to connect your MotherDuck data warehouse with Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to MotherDuck destination and its documentation, contact [MotherDuck Support](mailto:support@motherduck.com).

------

## Type transformation mapping

The data types in your MotherDuck data warehouse follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

We use the following data type conversions:

| Fivetran Data Type | Destination Data Type |
|--------------------|-----------------------|
| BOOLEAN            | BOOLEAN               |
| SHORT              | SMALLINT              |
| INT                | INTEGER               |
| LONG               | BIGINT                |
| FLOAT              | FLOAT                 |
| DOUBLE             | DOUBLE                |
| DECIMAL            | DECIMAL               |
| LOCALDATE          | DATE                  |
| LOCALDATETIME      | TIMESTAMP             |
| INSTANT            | TIMESTAMP             |
| STRING             | VARCHAR               |
| JSON               | VARCHAR               |
| BINARY             | BIT                   |

------------

## Limitations

[History mode](/docs/core-concepts/sync-modes/history-mode) is not supported for MotherDuck destinations.



