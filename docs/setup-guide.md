---
name: Setup Guide
title: Fivetran for MotherDuck | Destination Setup Guide
description: Connect data sources to MotherDuck in minutes using Fivetran. Explore documentation and start syncing your applications, databases, events, files, and more.
menuPosition: 0
---


# MotherDuck Setup Guide

Follow our setup guide to connect your MotherDuck destination to Fivetran.

-------

## Prerequisites

To connect MotherDuck to Fivetran, you need the following:

- An [account with MotherDuck](https://motherduck.com/docs/getting-started/)
- Fivetran role with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#destinationpermissions) permissions

-------

### <span class="step-item"> Complete Fivetran configuration </span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), then click **+ Add Destination**.
3. On the Add destination to your account page, enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **MotherDuck** as the destination type.
6. Enter your **Authentication Token** ([how to get the token](https://motherduck.com/docs/authenticating-to-motherduck#fetching-the-service-token)).
7. Enter the **Database** name that you would like to send your data to. Note that if this database does not exist, it will be created the first time you connect to it.
8. Click **Save and Test**.
9. Done!

Fivetran tests that you are able to connect to MotherDuck with the provided token.
On successful completion of the setup test, you can sync your data into the MotherDuck destination database using Fivetran connectors.

-------

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Destination Overview](/docs/destinations/motherduck)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
