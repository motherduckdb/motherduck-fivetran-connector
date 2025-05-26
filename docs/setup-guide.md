---
name: Setup Guide
title: Fivetran for MotherDuck | Destination Setup Guide
description: Connect data sources to MotherDuck in minutes using Fivetran. Explore documentation and start syncing your applications, databases, events, files, and more.
menuPosition: 0
---


# MotherDuck Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="motherduck" /%}

Follow our setup guide to connect your MotherDuck destination to Fivetran.

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to MotherDuck destination and its documentation, contact [MotherDuck Support](mailto:support@motherduck.com).

-------

## Prerequisites

To connect MotherDuck to Fivetran, you need the following:

- A [MotherDuck account](https://motherduck.com/docs/getting-started/)
- A Fivetran role with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#destinationpermissions) permissions

-------

### <span class="step-item"> Complete Fivetran configuration </span>

1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Enter a **Destination name** of your choice and then click **Add**.
4. Select **MotherDuck** as the destination type.
5. Enter your **Authentication Token**. Refer to [MotherDuck's documentation](https://motherduck.com/docs/authenticating-to-motherduck#fetching-the-service-token) for details on how to get the authentication token.
6. Enter the **Database** name of an existing MotherDuck database you want to replicate to.
7. Click **Save and Test**.

Fivetran tests that you are able to connect to MotherDuck with the provided token.
On successful completion of the setup test, you can sync your data into the MotherDuck destination database using Fivetran connectors.

In addition, Fivetran automatically configures a [Fivetran Platform Connector](/docs/logs/fivetran-platform) to transfer the connector logs and account metadata to a schema in this destination. The Fivetran Platform Connector enables you to monitor your connectors, track your usage, and audit changes. The connector sends all these details at the destination level.

> IMPORTANT: If you are an Account Administrator, you can manually add the Fivetran Platform Connector on an account level so that it syncs all the metadata and logs for all the destinations in your account to a single destination. If an account-level Fivetran Platform Connector is already configured in a destination in your Fivetran account, then we don't add destination-level Fivetran Platform Connectors to the new destinations you create.

-------

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Destination Overview](/docs/destinations/motherduck)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
