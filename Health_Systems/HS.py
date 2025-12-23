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


#Looks for healthcare system specific indicators 
def find_HS_indicators(ind_df):
    search_words = ['density per 100 000 population', 
                    'median availability of selected generic medicines', 
                    'median consumer price ratio of selected generic medicines', 
                    'care-seeking by type of patient and source of care', 
                    'beds, hospital beds']
    
    pattern = '|'.join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique healthcare system-specific indicators sorted by indicator codes
    HS_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Healthcare System Indicators found: ")
    print(HS_inds)

    return HS_inds

#Fetch data for Healthcare System indicators
def fetch_HS_data(HS_inds):
    HS_data = []

    #Fetches JSON data from each healthcare system indicator using the indicator code
    for code in HS_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing healthcare system data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        HS_data.append(df_i)

    #No data exists for this indicator code.
    if not HS_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    HS_df = pd.concat(HS_data, ignore_index=True)
    print(HS_df.head())
    return HS_df


def clean_and_reshape(HS_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in HS_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in HS_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    HS_clean = (
        HS_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    HS_clean["YEAR"] = HS_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format healthcare system data")
    print(HS_clean.head())

    #Transposes data to create a wide table 
    HS_wide = HS_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(HS_wide.head())

    return HS_clean, HS_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(HS_clean, HS_wide):
    HS_clean.to_csv("HS_all_long.csv", index=False)
    HS_wide.to_csv("HS_all_wide.csv")

    print("Saved HS_all_long.csv and HS_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    HS_inds = find_HS_indicators(ind_df)

    HS_df = fetch_HS_data(HS_inds)

    HS_long, HS_wide = clean_and_reshape(HS_df)

    save_outputs(HS_long, HS_wide)