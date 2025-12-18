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


#Looks for Dementia specific indicators 
def find_dementia_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("dementia", case=False, na=False)

    #Create dataframe of only unique dementia-specific indicators sorted by indicator codes
    dementia_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Dementia Indicators found: ")
    print(dementia_inds)

    return dementia_inds

#Fetch data for dementia indicators
def fetch_dementia_data(dementia_inds):
    dementia_data = []

    #Fetches JSON data from each dementia indicator using the indicator code
    for code in dementia_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing dementia data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        dementia_data.append(df_i)

    #No data exists for this indicator code.
    if not dementia_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    dementia_df = pd.concat(dementia_data, ignore_index=True)
    print(dementia_df.head())
    return dementia_df


def clean_and_reshape(dementia_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in dementia_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trachoma_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    dementia_clean = (
        dementia_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    dementia_clean["YEAR"] = dementia_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format dementia data")
    print(dementia_clean.head())

    #Transposes data to create a wide table 
    dementia_wide = dementia_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(dementia_wide.head())

    return dementia_clean, dementia_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(dementia_clean, dementia_wide):
    dementia_clean.to_csv("dementia_all_long.csv", index=False)
    dementia_wide.to_csv("dementia_all_wide.csv")

    print("Saved dementia_all_long.csv and dementia_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    dementia_inds = find_dementia_indicators(ind_df)

    dementia_df = fetch_dementia_data(dementia_inds)

    dementia_long, dementia_wide = clean_and_reshape(dementia_df)

    save_outputs(dementia_long, dementia_wide)