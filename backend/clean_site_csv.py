import pandas as pd

# ------------------------------------------
# INPUT FILES
# ------------------------------------------
clean_meta_path = "data/site_metadata/site_meta_clean.csv"
lane_only_path = "data/site_metadata/site_lane_counts_1763212913124.csv"

# ------------------------------------------
# LOAD FILES
# ------------------------------------------
df_clean = pd.read_csv(clean_meta_path)
df_lanes = pd.read_csv(lane_only_path)

# Normalize column names
df_clean.columns = df_clean.columns.str.strip()
df_lanes.columns = df_lanes.columns.str.strip()

# ------------------------------------------
# CLEAN METADATA MUST HAVE THESE COLUMNS
# ------------------------------------------
final_cols = [
    "location",
    "geolatitude",
    "geolongitude",
    "number_of_lanes",
    "Direction",
    "Streat_Name_Arabic",
    "Streat_Name_English"
]

# Ensure columns exist
for col in final_cols:
    if col not in df_clean.columns:
        df_clean[col] = ""

# ------------------------------------------
# HANDLE LANES FILE
# It contains:  site , max_lanes
# Convert to expected: location , number_of_lanes
# ------------------------------------------
df_lanes.rename(columns={
    "site": "location",
    "max_lanes": "number_of_lanes"
}, inplace=True)

# ------------------------------------------
# MERGE BOTH FILES
# ------------------------------------------
df_merged = pd.merge(
    df_clean,
    df_lanes[["location", "number_of_lanes"]],
    on="location",
    how="outer",
    suffixes=("", "_lanes")
)

# ------------------------------------------
# PRIORITY:
# - use clean metadata number_of_lanes if exists
# - else use lane-only number_of_lanes
# ------------------------------------------
def pick_lanes(row):
    if pd.notnull(row["number_of_lanes"]):
        return row["number_of_lanes"]
    return row["number_of_lanes_lanes"]

df_merged["number_of_lanes"] = df_merged.apply(pick_lanes, axis=1)

df_merged.drop(columns=["number_of_lanes_lanes"], inplace=True)

# ------------------------------------------
# CLEAN UP OTHER FIELDS
# ------------------------------------------
for col in final_cols:
    if col != "number_of_lanes":
        df_merged[col] = df_merged[col].fillna("")

df_merged["number_of_lanes"] = pd.to_numeric(df_merged["number_of_lanes"], errors="coerce")

# ------------------------------------------
# SAVE FINAL OUTPUT
# ------------------------------------------
output_path = "data/site_metadata/site_meta_final.csv"
df_merged.to_csv(output_path, index=False)

print("✓ Merged metadata saved to:", output_path)
print("Total sites:", len(df_merged))
print(df_merged.head())

