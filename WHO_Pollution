import requests
import pandas as pd



#Fetch all existing indicators. 


def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        print("Fetching Indicator page:", url)
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@odata.nextLink")  # pagination link (or None)

    ind_df = pd.DataFrame(all_rows)
    print("Total indicators:", len(ind_df))
    return ind_df


#Filter through pollution indicators

def find_pollution_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("pollution", case=False, na=False)
    poll_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\nPollution indicators found:")
    print(poll_inds)
    print("Number of pollution indicators:", len(poll_inds))
    return poll_inds



#Fetch data for each pollution indicator


def fetch_pollution_data(pollution_inds):
    pollution_data = []

    for code in pollution_inds["IndicatorCode"]:
        print("\nFetching data for:", code)
        # IMPORTANT: new GHO API pattern is /api/{IndicatorCode}
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)

        if resp.status_code != 200:
            print(f"  -> status {resp.status_code}, skipping {code}")
            continue

        js = resp.json()
        rows = js.get("value", [])
        if not rows:
            print("  -> no data, skipping", code)
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        pollution_data.append(df_i)

    if not pollution_data:
        raise SystemExit("No pollution data collected. Check API connectivity or filters.")

    pollution_df = pd.concat(pollution_data, ignore_index=True)
    print("\nCombined pollution data shape:", pollution_df.shape)
    print(pollution_df.head())
    return pollution_df




def clean_and_reshape(pollution_df):
    #In the GHO O Data API, Country is often called SpatialDim. 
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in pollution_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in pollution_df")

    pollution_clean = (
        pollution_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    #ensure year is numeric
    pollution_clean["YEAR"] = pollution_clean["YEAR"].astype(int)

    print("\nClean long-format pollution data (first 5 rows):")
    print(pollution_clean.head())

    # Wide format: one row per COUNTRY, multi-index columns (IndicatorCode, YEAR)
    pollution_wide = pollution_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )

    print("\nWide table (COUNTRY x (IndicatorCode, YEAR)) - first 5 rows:")
    print(pollution_wide.head())

    return pollution_clean, pollution_wide


#Save CSV files 

def save_outputs(pollution_clean, pollution_wide):
    pollution_clean.to_csv("pollution_all_long.csv", index=False)
    pollution_wide.to_csv("pollution_all_wide.csv")

    print("\nSaved files:")
    print(" - pollution_all_long.csv")
    print(" - pollution_all_wide.csv")



if __name__ == "__main__":
    ind_df = fetch_all_indicators()


    pollution_inds = find_pollution_indicators(ind_df)

  
    pollution_raw = fetch_pollution_data(pollution_inds)


    pollution_long, pollution_wide = clean_and_reshape(pollution_raw)

    save_outputs(pollution_long, pollution_wide)