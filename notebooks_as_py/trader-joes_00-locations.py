# Converted from 00-locations.ipynb
# Original notebook: trader-joes/00-locations.ipynb

# ===== CELL 0 (MARKDOWN) =====
# # Get Trader Joe's locations

# ===== CELL 1 (CODE) =====
import us
import requests
import pandas as pd
import geopandas as gpd
import altair as alt
from vega_datasets import data
from tqdm.notebook import tqdm, trange

# ===== CELL 2 (CODE) =====
pd.options.display.max_columns = 100
pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = None

# ===== CELL 3 (MARKDOWN) =====
# ---

# ===== CELL 4 (MARKDOWN) =====
# ## Read data

# ===== CELL 5 (CODE) =====
place = "Trader Joe's"

# ===== CELL 6 (CODE) =====
headers = {
    "authority": "alphaapi.brandify.com",
    "accept": "application/json, text/plain, */*",
    "referer": "https://www.traderjoes.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
}

# ===== CELL 7 (MARKDOWN) =====
# #### Trader Joe's has nice json data without limits. Thanks, Trader Joe's. 

# ===== CELL 8 (CODE) =====
json_data = {
    "request": {
        "appkey": "8BC3433A-60FC-11E3-991D-B2EE0C70A832",
        "formdata": {
            "geoip": False,
            "dataview": "store_default",
            "limit": 1000,
            "geolocs": {
                "geoloc": [
                    {
                        "addressline": "90066",
                        "country": "US",
                        "latitude": "",
                        "longitude": "",
                    },
                ],
            },
            "searchradius": "5000",
            "where": {
                "warehouse": {
                    "distinctfrom": "1",
                },
            },
            "false": "0",
        },
    },
}

response = requests.post(
    "https://alphaapi.brandify.com/rest/locatorsearch", headers=headers, json=json_data
)

# ===== CELL 9 (MARKDOWN) =====
# #### Dataframe

# ===== CELL 10 (CODE) =====
src = pd.DataFrame(response.json()["response"]["collection"])

# ===== CELL 11 (MARKDOWN) =====
# #### How many locations did we capture? 

# ===== CELL 12 (CODE) =====
len(src)

# ===== CELL 13 (MARKDOWN) =====
# #### Tidy up

# ===== CELL 14 (CODE) =====
df = src[
    [
        "uid",
        "name",
        "address1",
        "city",
        "state",
        "postalcode",
        "phone",
        "latitude",
        "longitude",
        "beer",
        "liquor",
        "wineshop",
        "regions",
    ]
].copy()

# ===== CELL 15 (CODE) =====
df.rename(columns={"address1": "address", "postalcode": "zip"}, inplace=True)

# ===== CELL 16 (CODE) =====
df["uid"] = df["uid"].abs().astype(str)[0]

# ===== CELL 17 (CODE) =====
df["store_number"] = (
    df["name"]
    .str[-4:]
    .str.strip()
    .str.replace(")", "", regex=False)
    .str.replace("(", "", regex=False)
    .astype(str)
)

# ===== CELL 18 (CODE) =====
df["name"] = df["name"].str.split("(", expand=True)[0]

# ===== CELL 19 (MARKDOWN) =====
# #### The result: 

# ===== CELL 20 (CODE) =====
df.query('state == "CA"')

# ===== CELL 21 (MARKDOWN) =====
# ---

# ===== CELL 22 (MARKDOWN) =====
# ## Geography

# ===== CELL 23 (MARKDOWN) =====
# #### Make it a geodataframe

# ===== CELL 24 (CODE) =====
df_geo = df.copy()

# ===== CELL 25 (CODE) =====
gdf = gpd.GeoDataFrame(
    df_geo, geometry=gpd.points_from_xy(df_geo.longitude, df_geo.latitude)
)

# ===== CELL 26 (CODE) =====
locations_gdf = gdf.set_crs("EPSG:4326").copy()

# ===== CELL 27 (MARKDOWN) =====
# #### Counties

# ===== CELL 28 (CODE) =====
counties = gpd.read_file("data/raw/usa_counties_esri_simple.json")

# ===== CELL 29 (CODE) =====
counties_gdf = counties[
    [
        "fid",
        "name",
        "state_name",
        "state_fips",
        "cnty_fips",
        "fips",
        "population",
        "geometry",
    ]
].copy()

# ===== CELL 30 (CODE) =====
tj_locations_counties = locations_gdf.sjoin(counties, how="left", predicate="within")

# ===== CELL 31 (CODE) =====
counties_grouped = (
    (
        tj_locations_counties.groupby(["name_right", "cnty_fips", "city", "state"])[
            "uid"
        ]
        .count()
        .reset_index()
        .sort_values("uid", ascending=False)
    )
    .rename(columns={"uid": "count"})
    .reset_index(drop=True)
)

# ===== CELL 32 (CODE) =====
counties_grouped.head()

# ===== CELL 33 (MARKDOWN) =====
# ---

# ===== CELL 34 (MARKDOWN) =====
# ## Maps

# ===== CELL 35 (MARKDOWN) =====
# #### US states background

# ===== CELL 36 (CODE) =====
background = (
    alt.Chart(alt.topo_feature(data.us_10m.url, feature="states"))
    .mark_geoshape(fill="#e9e9e9", stroke="white")
    .properties(width=800, height=500, title=f"{place} locations")
    .project("albersUsa")
)

# ===== CELL 37 (MARKDOWN) =====
# #### Location points map

# ===== CELL 38 (CODE) =====
points = (
    alt.Chart(gdf)
    .mark_circle(size=10, color="red")
    .encode(
        longitude="longitude:Q",
        latitude="latitude:Q",
    )
)

point_map = background + points
point_map.configure_view(stroke=None)

# ===== CELL 39 (MARKDOWN) =====
# #### Location proportional symbols map

# ===== CELL 40 (CODE) =====
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
        color=alt.value("red"),
        tooltip=["state:N", "count:Q"],
    )
    .properties(title=f"Number of {place} in US, by average lon/lat of locations")
)

symbol_map = background + symbols
symbol_map.configure_view(stroke=None)

# ===== CELL 41 (MARKDOWN) =====
# ---

# ===== CELL 42 (MARKDOWN) =====
# ## Exports

# ===== CELL 43 (MARKDOWN) =====
# #### CSV

# ===== CELL 44 (CODE) =====
df.to_csv("data/processed/trader_joes_locations.csv", index=False)

# ===== CELL 45 (MARKDOWN) =====
# #### JSON

# ===== CELL 46 (CODE) =====
df.to_json("data/processed/trader_joes_locations.json", indent=4, orient="records")

# ===== CELL 47 (MARKDOWN) =====
# #### GeoJSON

# ===== CELL 48 (CODE) =====
locations_gdf.to_file("data/processed/trader_joes_locations.geojson", driver="GeoJSON")

