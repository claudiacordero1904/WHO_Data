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

def find_all_leishmaniasis_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("leishmaniasis", case=False, na=False)

    Leishmaniasis_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)

    )

    print("\nAll Leishmaniasis indicators found: ")
    print(Leishmaniasis_inds)
    return Leishmaniasis_inds

def fetch_leishmaniasis_data(Leishmaniasis_inds):
    Leishmaniasis_data = []

    for code in Leishmaniasis_inds["IndicatorCode"]:

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
        Leishmaniasis_data.append(df_i)

    if not Leishmaniasis_data:
        raise SystemExit("No data found. Check API for connectivity issues")
    
    Leishmaniasis_df = pd.concat(Leishmaniasis_data, ignore_index=True)
    print(Leishmaniasis_df.head())
    return Leishmaniasis_df


def clean_and_reshape(Leishmaniasis_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in Leishmaniasis_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols} but missing {missing} in leishmaniasis_df")
    
    Leishmaniasis_clean = (
        Leishmaniasis_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    Leishmaniasis_clean["YEAR"] = Leishmaniasis_clean["YEAR"].astype(int)

    print("Cleaned up leishmaniasis data: ")
    print(Leishmaniasis_clean.head())

    Leishmaniasis_wide = Leishmaniasis_clean.pivot_table (
        index = "COUNTRY",
        columns = ["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print(Leishmaniasis_wide.head())



    return Leishmaniasis_clean, Leishmaniasis_wide

def save_outputs(Leishmaniasis_clean, Leishmaniasis_wide):
    Leishmaniasis_clean.to_csv("Leishmaniasis_long.csv", index=False)
    Leishmaniasis_wide.to_csv("Leishmaniasis_wide.csv")

    print("CSV files created for both long and wide-format leishmaniasis data")



if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    Leishmaniasis_inds = find_all_leishmaniasis_indicators(ind_df)

    Leishmaniasis_df = fetch_leishmaniasis_data(Leishmaniasis_inds)

    Leishmaniasis_long, Leishmaniasis_wide = clean_and_reshape(Leishmaniasis_df)

    save_outputs(Leishmaniasis_long, Leishmaniasis_wide)


