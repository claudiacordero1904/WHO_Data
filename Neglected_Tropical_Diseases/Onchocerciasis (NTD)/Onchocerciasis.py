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


def find_onchocerciasis_indicators(ind_df):
    #Filter through indicators with key word "Onchocerciasis" in them to find all onchocerciasis related indicators
    mask = ind_df["IndicatorName"].str.contains("Onchocerciasis", case=False, na=False)

    #Create new dataframe with only unique onchocerciasis indicators
    onchocerciasis_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Onchocerciasis: ")
    print(onchocerciasis_inds)
    return onchocerciasis_inds


def fetch_onchocerciasis_data(onchocerciasis_inds):
    onchocerciasis_data = []

    #Retrieve JSON data from all onchocerciasis indicators and insert into a dataframe
    for code in onchocerciasis_inds["IndicatorCode"]:
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
        onchocerciasis_data.append(df_i)

    if not onchocerciasis_data:
        raise SystemExit("No data found. Please check API for connectivity issues.")
    

    onchocerciasis_df = pd.concat(onchocerciasis_data, ignore_index=True)
    print(onchocerciasis_df.head())
    return onchocerciasis_df


def clean_and_reshape(onchocerciasis_df):
    #Only select necessary columns for our new clean dataframe (Country, year, indicator code, and numeric data value)
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]

    #Check for indicators missing any of the necessary columns 
    missing = [c for c in needed_cols if c not in onchocerciasis_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in leprosy_df ")
    
    #Create a new clean dataframe with renamed columns and no missing countries, years, or indicator codes
    onchocerciasis_clean = (
        onchocerciasis_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])

    )

    #Ensure year is numeric 
    onchocerciasis_clean["YEAR"] = onchocerciasis_clean["YEAR"].astype(int)

    print("\n Printing cleaned onchocerciasis data frame")
    print(onchocerciasis_clean.head())

    #Create a wide-format table of the cleaned data sorted by countries
    onchocerciasis_wide = onchocerciasis_clean.pivot_table (
        index = "COUNTRY",
        columns = ["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Printing transposed clean dataframe:")
    print(onchocerciasis_wide.head())

    return onchocerciasis_clean, onchocerciasis_wide

#Create CSV files of both the long and wide format data 
def save_outputs(onchocerciasis_clean, onchocerciasis_wide):
    onchocerciasis_clean.to_csv("onchocerciasis_long_data.csv", index=False)
    onchocerciasis_wide.to_csv("onchocerciasis_wide_data.csv")

    print("Printed CSV long and wide-format files for onchocerciasis data (onchocerciasis_long_data.csv and onchocerciasis_wide_data.csv)")

    






if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    onchocerciasis_inds = find_onchocerciasis_indicators(ind_df)

    onchocerciasis_df = fetch_onchocerciasis_data(onchocerciasis_inds)

    onchocerciasis_long, onchocerciasis_wide = clean_and_reshape(onchocerciasis_df)

    save_outputs(onchocerciasis_long, onchocerciasis_wide)