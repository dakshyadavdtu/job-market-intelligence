# QuickSight: India city dataset — geospatial hierarchy (coordinates)

Athena views `v2_in_geo_city_points_monthly` expose plain `latitude` / `longitude` columns (`double`). QuickSight only treats them as map coordinates after **dataset preparation**: assign **geospatial types** and create a **geospatial hierarchy** that groups latitude + longitude (often shown as one “Coordinates” group in the field list).

## Prerequisites

- Dataset source: `jmi_analytics_v2.v2_in_geo_city_points_monthly` (refresh after view changes).
- In **Prepare data**, you should see fields including `latitude`, `longitude`, `country`, `state_geo`, `city`, `job_count`.

## Step 1 — Set geospatial field types

For each field:

1. Open the dataset → **Edit** / **Prepare data**.
2. In the **Fields** list, find **`latitude`**.
3. Open the **⋯** (ellipsis) menu on **`latitude`** → choose the correct **geospatial** type → **Latitude**.  
   Confirm the **place marker** icon appears on the field.
4. Repeat for **`longitude`** → **Longitude**.

(You can also click the type under the field name in the data preview and change it there.)

## Step 2 — Create the coordinates hierarchy (latitude + longitude)

This is what makes **Points on map** accept the **GEOSPATIAL** well: one hierarchy that contains both coordinates.

1. In **Fields**, open the **⋯** menu on **`latitude`** or **`longitude`**.
2. Choose **Add to a hierarchy** → **Create a new geospatial hierarchy**.
3. On **Create hierarchy**:
   - **Name** the hierarchy (e.g. `Coordinates` or `City point coordinates`).
   - QuickSight should show **Field to use for latitude** = `latitude` and **Field to use for longitude** = `longitude`. Fix if either is wrong.
4. Choose **Update** / **Add** to save.

You should now see a **single hierarchy** (often expandable) that contains **latitude** and **longitude** — that is the “coordinates” group.

## Step 3 — Build the point map

1. Add a visual → **Points on map**.
2. Drag the **geospatial hierarchy** you created (e.g. **Coordinates**) into the **GEOSPATIAL** field well — **not** the raw numeric fields separately if the well rejects them.
3. Drag **`job_count`** to **SIZE** (and optional dimensions to **COLOR**).

## Optional: country / state / city hierarchy

For disambiguation (same city name in different states), after latitude/longitude are set:

1. Set **`country`** → **Country**, **`state_geo`** → **State or region**, **`city`** → **City** (geospatial types).
2. **⋯** on **`country`** → **Add to a hierarchy** → **Create new geospatial hierarchy** (or add to existing if QuickSight allows mixing — often you keep **one** hierarchy for lat/lon and a **separate** hierarchy for country → state → city for filled maps).

Single-country data: when creating a geographic hierarchy, choose **This hierarchy is for a single country** and select **India**.

## Filled map (states)

Use dataset `v2_in_geo_state_monthly`: set **`country`** → **Country**, **`state_geo`** → **State or region** (filter **`state_geo` is not null** for mapped states only).
