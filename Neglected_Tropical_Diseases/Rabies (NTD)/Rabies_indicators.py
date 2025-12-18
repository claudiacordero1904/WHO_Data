import requests 
import pandas as pd

def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        print("Fetching indicators page: ", url)
        resp = requests.get(url)
        resp.raise_for_status
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    print("Number of indicators found: ", len(ind_df))
    return ind_df

def find_all_rabies_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("Rabies", case=False, na=False)

    rabies_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)

    )

    print("\nAll rabies indicators found: ")
    print(rabies_inds)
    return rabies_inds

def fetch_rabies_data(rabies_inds):
    rabies_data = []

    for code in rabies_inds["IndicatorCode"]:

        print("\nFetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)

        if resp.status_code != 200:
            continue


        js = resp.json()

        rows = js.get("value", [])

        if not rows:
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        rabies_data.append(df_i)

    if not rabies_data:
        raise SystemExit("No data found. Check API for connectivity issues")
    
    rabies_df = pd.concat(rabies_data, ignore_index=True)
    print(rabies_df.head())
    return rabies_df


def clean_and_reshape(rabies_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in rabies_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols} but missing {missing} in leprosy_df")
    
    rabies_clean = (
        rabies_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    rabies_clean["YEAR"] = rabies_clean["YEAR"].astype(int)

    print("Cleaned up rabies data: ")
    print(rabies_clean.head())

    rabies_wide = rabies_clean.pivot_table (
        index = "COUNTRY",
        columns = ["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print(rabies_wide.head())



    return rabies_clean, rabies_wide

def save_outputs(rabies_clean, rabies_wide):
    rabies_clean.to_csv("rabies_long.csv", index=False)
    rabies_wide.to_csv("rabies_wide.csv")

    print("CSV files created for both long and wide-format rabies data")



if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    rabies_inds = find_all_rabies_indicators(ind_df)

    rabies_df = fetch_rabies_data(rabies_inds)

    rabies_long, rabies_wide = clean_and_reshape(rabies_df)

    save_outputs(rabies_long, rabies_wide)


