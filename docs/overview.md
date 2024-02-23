---
name: MotherDuck
title: Fivetran for MotherDuck | Configuration requirements and documentation
description: Connect data sources to MotherDuck in minutes using Fivetran. Explore documentation and start syncing your applications, databases, events, files, and more.
---

# MotherDuck

[MotherDuck](https://motherduck.com/) is a DuckDB-powered serverless data warehouse with a unique architecture that combines the power and scale of the cloud with the efficiency and convenience of DuckDB.
Fivetran supports MotherDuck as a destination.

-----

## Setup guide

Follow our [step-by-step MotherDuck setup guide](/docs/destinations/motherduck/setup-guide) to connect your MotherDuck data warehouse with Fivetran.

------

## Type transformation mapping

The data types in your MotherDuck data warehouse follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

We use the following data type conversions:

| Fivetran Data Type | Destination Data Type | Notes |
|--------------------|---|--|
|  BOOLEAN           | BOOLEAN | |
| SHORT              | SMALLINT | |
| INT                | INTEGER | |
| LONG               | BIGINT | |
| FLOAT              | FLOAT | |
| DOUBLE             | DOUBLE | |
| DECIMAL            | DECIMAL | |
| LOCALDATE          | DATE | |
| LOCALDATETIME      | TIMESTAMP | |
| INSTANT            | TIMESTAMP | |
| STRING             | VARCHAR | |
| JSON               | STRUCT | |
| BINARY             | BIT | |

--------

