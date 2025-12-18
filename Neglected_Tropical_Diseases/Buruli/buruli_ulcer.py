import requests
import pandas as pd

#Fetch all indicators from GHO database
def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    #Fetch JSON data from all indicators 
    while url:
        print("\n Fetching indicators page", url)
        resp = requests.get(url)
        resp.raise_for_status
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@data.nextLink")

    #Convert rows of JSON indicator data into a dataframe
    ind_df = pd.DataFrame(all_rows)
    print("Number of indicators: ", len(ind_df))
    return ind_df


def find_buruli_indicators(ind_df):
    #Filter through indicators with key word "buruli" in them to find all buruli ulcer related indicators
    mask = ind_df["IndicatorName"].str.contains("buruli", case=False, na=False)

    #Create new dataframe with only unique buruli ulcer indicators
    buruli_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Buruli Ulcer indicators found: ")
    print(buruli_inds)
    return buruli_inds


def fetch_buruli_data(buruli_inds):
    buruli_data = []

    #Retrieve JSON data from all buruli indicators and insert into a dataframe
    for code in buruli_inds["IndicatorCode"]:
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
        buruli_data.append(df_i)

    if not buruli_data:
        raise SystemExit("No data found. Please check API for connectivity issues.")
    

    buruli_df = pd.concat(buruli_data, ignore_index=True)
    print(buruli_df.head())
    return buruli_df


def clean_and_reshape(buruli_df):
    #Only select necessary columns for our new clean dataframe (Country, year, indicator code, and numeric data value)
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]

    #Check for indicators missing any of the necessary columns 
    missing = [c for c in needed_cols if c not in buruli_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in leprosy_df ")
    
    #Create a new clean dataframe with renamed columns and no missing countries, years, or indicator codes
    buruli_clean = (
        buruli_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])

    )

    #Ensure year is numeric 
    buruli_clean["YEAR"] = buruli_clean["YEAR"].astype(int)

    print("\n Printing cleaned buruli ulcer data frame")
    print(buruli_clean.head())

    #Create a wide-format table of the cleaned data sorted by countries
    buruli_wide = buruli_clean.pivot_table (
        index = "COUNTRY",
        columns = ["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Printing transposed clean dataframe:")
    print(buruli_wide.head())

    return buruli_clean, buruli_wide

#Create CSV files of both the long and wide format data 
def save_outputs(buruli_clean, buruli_wide):
    buruli_clean.to_csv("buruli_long_data.csv", index=False)
    buruli_wide.to_csv("buruli_wide_data.csv")

    print("Printed CSV long and wide-format files for buruli ulcer data (buruli_long_data.csv and buruli_wide_data.csv)")

    






if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    buruli_inds = find_buruli_indicators(ind_df)

    buruli_df = fetch_buruli_data(buruli_inds)

    buruli_long, buruli_wide = clean_and_reshape(buruli_df)

    save_outputs(buruli_long, buruli_wide)

