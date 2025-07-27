# Converted from get-locations.ipynb
# Original notebook: starbucks/get-locations.ipynb

# ===== CELL 0 (MARKDOWN) =====
# # Get Starbucks locations

# ===== CELL 1 (MARKDOWN) =====
# #### Load Python tools and Jupyter config

# ===== CELL 2 (CODE) =====
%load_ext lab_black

# ===== CELL 3 (CODE) =====
import json
import requests
import pandas as pd
import altair as alt
import geopandas as gpd
from tqdm.notebook import tqdm, trange
from vega_datasets import data

# ===== CELL 4 (CODE) =====
pd.options.display.max_rows = 1000
pd.options.display.max_columns = 1000
pd.options.display.max_colwidth = None
alt.data_transformers.disable_max_rows()

# ===== CELL 5 (CODE) =====
place = "Starbucks"

# ===== CELL 6 (MARKDOWN) =====
# ## Read data

# ===== CELL 7 (MARKDOWN) =====
# #### ZIP Codes for reference, sorted by population

# ===== CELL 8 (CODE) =====
zips_df = pd.read_csv("../_reference/data/zips_reference.csv").sort_values(
    "pop2010", ascending=False
)

# ===== CELL 9 (MARKDOWN) =====
# #### Make a list of the ZIP Codes

# ===== CELL 10 (CODE) =====
zips = list(zips_df.zip)

# ===== CELL 11 (MARKDOWN) =====
# #### Set headers for requests

# ===== CELL 12 (CODE) =====
headers = {
    "authority": "www.starbucks.com",
    "accept": "application/json",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}

# ===== CELL 13 (MARKDOWN) =====
# #### Loop through the list of ZIP Codes, grab results and place into a list of dataframes

# ===== CELL 14 (CODE) =====
stores_zips_list = []

for z in tqdm(zips[0:10000]):
    params = {
        "place": f"{z}",
    }
    response = requests.get(
        "https://www.starbucks.com/bff/locations", params=params, headers=headers
    )
    try:
        src = pd.DataFrame(response.json()["stores"])
    except:
        continue
    stores_zips_list.append(src)

# ===== CELL 15 (MARKDOWN) =====
# #### Concatenate the dataframes, keeping only the columns we need and eliminating duplicate locations (from contiguous ZIP Codes)

# ===== CELL 16 (CODE) =====
src_df = (
    pd.concat(stores_zips_list)[
        [
            "storeNumber",
            "name",
            "coordinates",
            "address",
            "timeZoneInfo",
            "ownershipTypeCode",
            "addressLines",
            "slug",
        ]
    ]
    .drop_duplicates(subset="storeNumber")
    .reset_index(drop=True)
)

# ===== CELL 17 (MARKDOWN) =====
# #### How many stores?

# ===== CELL 18 (CODE) =====
len(src_df)

# ===== CELL 19 (MARKDOWN) =====
# #### Flatten nested columns

# ===== CELL 20 (CODE) =====
src_df[
    [
        "streetAddressLine1",
        "streetAddressLine2",
        "streetAddressLine3",
        "city",
        "countrySubdivisionCode",
        "countryCode",
        "postalCode",
    ]
] = pd.json_normalize(src_df["address"])

# ===== CELL 21 (CODE) =====
src_df["timezone"] = pd.json_normalize(src_df["timeZoneInfo"])["olsonTimeZoneId"]

# ===== CELL 22 (CODE) =====
src_df[["latitude", "longitude"]] = pd.json_normalize(src_df["coordinates"])

# ===== CELL 23 (MARKDOWN) =====
# #### Five-digit ZIP Codes

# ===== CELL 24 (CODE) =====
src_df["zip"] = src_df["postalCode"].str[:5]

# ===== CELL 25 (MARKDOWN) =====
# #### Clean dataframe

# ===== CELL 26 (CODE) =====
df = (
    src_df.drop(
        [
            "address",
            "timeZoneInfo",
            "addressLines",
            "streetAddressLine2",
            "streetAddressLine3",
            "coordinates",
            "postalCode",
        ],
        axis=1,
    )
    .rename(
        columns={
            "countrySubdivisionCode": "state",
            "streetAddressLine1": "address",
            "storeNumber": "store_number",
            "countryCode": "country",
            "ownershipTypeCode": "ownership_type",
        }
    )[
        [
            "store_number",
            "name",
            "ownership_type",
            "slug",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "timezone",
            "latitude",
            "longitude",
        ]
    ]
    .copy()
)

# ===== CELL 27 (MARKDOWN) =====
# ## Geography

# ===== CELL 28 (MARKDOWN) =====
# #### Make it a geodataframe

# ===== CELL 29 (CODE) =====
df_geo = df.copy()

# ===== CELL 30 (CODE) =====
gdf = gpd.GeoDataFrame(
    df_geo, geometry=gpd.points_from_xy(df_geo.longitude, df_geo.latitude)
)

# ===== CELL 31 (CODE) =====
locations_gdf = gdf.set_crs("EPSG:4326").copy()

# ===== CELL 32 (MARKDOWN) =====
# ---

# ===== CELL 33 (MARKDOWN) =====
# ## Maps

# ===== CELL 34 (MARKDOWN) =====
# #### US states background

# ===== CELL 35 (CODE) =====
background = (
    alt.Chart(alt.topo_feature(data.us_10m.url, feature="states"))
    .mark_geoshape(fill="#e9e9e9", stroke="white")
    .properties(width=800, height=500, title=f"{place} locations")
    .project("albersUsa")
)

# ===== CELL 36 (MARKDOWN) =====
# #### Location points map

# ===== CELL 37 (CODE) =====
points = (
    alt.Chart(gdf)
    .mark_circle(size=1, color="#006241")
    .encode(
        longitude="longitude:Q",
        latitude="latitude:Q",
    )
)

point_map = background + points
point_map.configure_view(stroke=None)

# ===== CELL 38 (MARKDOWN) =====
# #### Location proportional symbols map

# ===== CELL 39 (CODE) =====
symbols = (
    alt.Chart(gdf)
    .transform_aggregate(
        latitude="mean(latitude)",
        longitude="mean(longitude)",
        count="count()",
        groupby=["state"],
    )
    .mark_circle()
    .encode(
        longitude="longitude:Q",
        latitude="latitude:Q",
        size=alt.Size("count:Q", title="Count by state"),
        color=alt.value("#006241"),
        tooltip=["state:N", "count:Q"],
    )
    .properties(title=f"Number of {place} in US, by average lon/lat of locations")
)

symbol_map = background + symbols
symbol_map.configure_view(stroke=None)

# ===== CELL 40 (MARKDOWN) =====
# ---

# ===== CELL 41 (MARKDOWN) =====
# ## Exports

# ===== CELL 42 (MARKDOWN) =====
# #### JSON

# ===== CELL 43 (CODE) =====
df.to_json(f"data/processed/{place.lower()}_locations.json", indent=4, orient="records")

# ===== CELL 44 (MARKDOWN) =====
# #### CSV

# ===== CELL 45 (CODE) =====
df.to_csv(f"data/processed/{place.lower()}_locations.csv", index=False)

# ===== CELL 46 (MARKDOWN) =====
# #### GeoJSON

# ===== CELL 47 (CODE) =====
locations_gdf.to_file(
    f"data/processed/{place.lower()}_locations.geojson", driver="GeoJSON"
)

# ===== CELL 48 (CODE) =====
len(locations_gdf)

