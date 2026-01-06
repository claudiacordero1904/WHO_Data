import requests
import pandas as pd

def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get('value', []))
        url = js.get('@odata.nextLink')

    ind_df = pd.DataFrame(all_rows)
    print(f"Fetched all indicators, total count: {len(ind_df)}")
    return ind_df

def find_all_SDG_indicators(ind_df):
    search_words = ["Financial hardship", "UHC Service Coverage"]
    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)
    SDG_inds = ( 
        ind_df.loc[mask, ["IndicatorName", "IndicatorCode"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print(f"Found {len(SDG_inds)} SDG-related indicators:")
    print(SDG_inds)
    return SDG_inds


def fetch_SDG_data(SDG_inds):
    SDG_data = []

    for code in SDG_inds["IndicatorCode"]:
        print(f"\n Fetching data for: {code}")
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        SDG_data.append(df_i)

    if not SDG_data:
        raise SystemExit("No data collected. Check API connection for error.")

    SDG_df = pd.concat(SDG_data, ignore_index=True)
    print(SDG_df.head())
    return SDG_df

def clean_and_reshape(SDG_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]

    missing = [c for c in needed_cols if c not in SDG_df.columns]

    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in SDG_df")
    
    SDG_clean = (
        SDG_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY/REGION", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY/REGION", "YEAR", "IndicatorCode"])
    )

    SDG_clean["YEAR"] =  SDG_clean["YEAR"].astype(int)

    SDG_wide = SDG_clean.pivot_table (
        index="COUNTRY/REGION",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )

    print("\n Cleaned SDG Data")
    print(SDG_clean.head())

    return SDG_clean, SDG_wide

def save_outputs(SDG_clean, SDG_wide):
    SDG_clean.to_csv("SDG_long.csv", index=False)
    SDG_wide.to_csv("SDG_wide.csv")

    print("\n Saved SDG long format and wide format data (SDG_long.csv, SDG_wide.csv)")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()
    
    SDG_inds = find_all_SDG_indicators(ind_df)

    SDG_df = fetch_SDG_data(SDG_inds)

    SDG_long, SDG_wide = clean_and_reshape(SDG_df)
    
    save_outputs(SDG_long, SDG_wide)


