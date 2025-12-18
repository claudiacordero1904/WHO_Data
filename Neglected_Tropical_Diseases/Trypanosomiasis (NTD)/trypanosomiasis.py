import requests
import pandas as pd


#Fetches all indicators from the GHO data
def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    #Fetch JSON data of all indicators available from GHO and convert it into a dataframe
    while url:
        print("Fetching indicators page", url)
        resp = requests.get(url)
        resp.raise_for_status
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    print("Total indicators found: ", len(ind_df))
    return ind_df


#Looks for Human African trypanosomiasis specific indicators
def find_trypanosomiasis_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("trypanosomiasis", case=False, na=False)

    #Create dataframe of only unique trypanosomiasis-specific indicators sorted by indicator codes
    trypanosomiasis_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Human Africa trypanosomiasis Indicators found: ")
    print(trypanosomiasis_inds)

    return trypanosomiasis_inds

#Fetch data for Human African trypanosomiasis indicators
def fetch_trypanosomiasis_data(trypanosomiasis_inds):
    trypanosomiasis_data = []

    #Fetches JSON data from each trypanosomiasis indicator using the indicator code
    for code in trypanosomiasis_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing trypanosomiasis data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        trypanosomiasis_data.append(df_i)

    #No data exists for this indicator code.
    if not trypanosomiasis_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    trypanosomiasis_df = pd.concat(trypanosomiasis_data, ignore_index=True)
    print(trypanosomiasis_df.head())
    return trypanosomiasis_df


def clean_and_reshape(trypanosomiasis_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trypanosomiasis dataframe
    missing = [c for c in needed_cols if c not in trypanosomiasis_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trypanosomiasis_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    trypanosomiasis_clean = (
        trypanosomiasis_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    trypanosomiasis_clean["YEAR"] = trypanosomiasis_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format trypanosomiasis data")
    print(trypanosomiasis_clean.head())

    #Transposes data to create a wide table 
    trypanosomiasis_wide = trypanosomiasis_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(trypanosomiasis_wide.head())

    return trypanosomiasis_clean, trypanosomiasis_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(trypanosomiasis_clean, trypanosomiasis_wide):
    trypanosomiasis_clean.to_csv("trypanosomiasis_all_long.csv", index=False)
    trypanosomiasis_wide.to_csv("trypanosomiasis_all_wide.csv")

    print("Saved trypanosomiasis_all_long.csv and trypanosomiasis_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    trypanosomiasis_inds = find_trypanosomiasis_indicators(ind_df)

    trypanosomiasis_df = fetch_trypanosomiasis_data(trypanosomiasis_inds)

    trypanosomiasis_long, trypanosomiasis_wide = clean_and_reshape(trypanosomiasis_df)

    save_outputs(trypanosomiasis_long, trypanosomiasis_wide)