import requests
import pandas as pd

def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        print("Fetching indicators page: ", url)
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get('value', []))
        url = js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    print("Number of indicators found: ", len(ind_df))
    return ind_df

def find_all_alcohol_indicators(ind_df):
    search_words = ['Alcohol', 'alcoholic beverage', 'first drink', 'Alcohol-attributable']
    pattern = "|".join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    alcohol_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)

    )

    print("\nAlcohol and health related indicators found: ")
    print(alcohol_inds)
    print(len(alcohol_inds))
    return alcohol_inds


def fetch_alcohol_data(alcohol_inds):
    alcohol_data = []

    for code in alcohol_inds["IndicatorCode"]:
        print("\n Fetching data for: ", code)
        url =  f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        rows = js.get('value', [])
        #print(code, "rows:", len(rows))

        if not rows: 
            continue 

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        alcohol_data.append(df_i)

    if not alcohol_data:
        raise SystemExit("No data found. Check API for connectivity issues")
    
    alcohol_df = pd.concat(alcohol_data, ignore_index=True)
    print(alcohol_df.head(3))
    return alcohol_df


def clean_and_reshape(alcohol_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in alcohol_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trachoma_df ")
    
    alcohol_clean = (
        alcohol_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
     )
    
    alcohol_clean["YEAR"] = alcohol_clean["YEAR"].astype(int)
    
    print("\n Cleaned alcohol dataframe w/ long-format data:")
    print(alcohol_clean.head(3))

    alcohol_wide = alcohol_clean.pivot_table (
        index = "COUNTRY",
        columns = ["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide-format alcohol dataframe")
    print(alcohol_wide.head(3))

    return alcohol_clean, alcohol_wide

def save_outputs(alcohol_clean, alcohol_wide):
    alcohol_clean.to_csv("alcohol_all_long.csv", index=False)
    alcohol_wide.to_csv("alcohol_all_wide.csv")

    print("Created a csv file for long and wide-format data (alcohol_all_long.csv and alcohol_all_wide.csv)")



if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    alcohol_inds = find_all_alcohol_indicators(ind_df)

    alcohol_df = fetch_alcohol_data(alcohol_inds)

    alcohol_long, alcohol_wide = clean_and_reshape(alcohol_df)

    save_outputs(alcohol_long, alcohol_wide)

