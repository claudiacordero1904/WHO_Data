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
        url = js.get('@data.nextLink')

    ind_df = pd.DataFrame(all_rows)
    print(f"Fetched {len(ind_df)} indicators")
    return ind_df

def find_AMR_indicators(ind_df):
    search_words = ['AMR', 'GLASS', 'national reference laboratory', 'NCC', 'Antimicrobial Susceptibility Testing']
    pattern = '|'.join(search_words)
    mask = ind_df['IndicatorName'].str.contains(pattern, case=False, na=False)

    AMR_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values(by="IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Antimicrobial Resistance (AMR) Indicators found:")
    print(AMR_inds)
    return AMR_inds

def fetch_AMR_data(AMR_inds):
    AMR_data = []

    for code in AMR_inds["IndicatorCode"]:
        print(f"\n Fetching data for: {code}")
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        AMR_data.append(df_i)

    if not AMR_data:
        raise SystemExit("No data collected. Check API connection for error.")

    AMR_df = pd.concat(AMR_data, ignore_index=True)
    print(AMR_df.head())
    return AMR_df


def clean_and_reshape(AMR_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in AMR_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in AMR_df")
    
    AMR_clean = (
        AMR_df[needed_cols]
        .rename(columns={"SpatialDim": "REGION", "TimeDim": "YEAR"})
        .dropna(subset=["NumericValue"])
    )

    AMR_clean["YEAR"] = AMR_clean["YEAR"].astype(int)

    print("\n Cleaned AMR Data:")
    print(AMR_clean.head())

    AMR_wide = AMR_clean.pivot_table(
        index="REGION",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )
    return AMR_clean, AMR_wide


def save_outputs(AMR_clean, AMR_wide):
    AMR_clean.to_csv("AMR_long.csv", index=False)
    AMR_wide.to_csv("AMR_wide.csv")

    print("\n Saved AMR long format and wide format data (AMR_long.csv, AMR_wide.csv)")
    

    



if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    AMR_inds = find_AMR_indicators(ind_df)

    AMR_df = fetch_AMR_data(AMR_inds)

    AMR_long, AMR_wide = clean_and_reshape(AMR_df)

    save_outputs(AMR_long, AMR_wide)


        