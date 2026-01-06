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


#Looks for patient safety specific indicators 
def find_PS_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("patient safety", case=False, na=False)

    #Create dataframe of only unique patient safety-specific indicators sorted by indicator codes
    PS_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Patient Safety Indicators found: ")
    print(PS_inds)

    return PS_inds

#Fetch data for patient safety indicators
def fetch_PS_data(PS_inds):
    PS_data = []

    #Fetches JSON data from each patient safety indicator using the indicator code
    for code in PS_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing patient safety data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        PS_data.append(df_i)

    #No data exists for this indicator code.
    if not PS_data:
        raise SystemExit("No data collected. Check API connection for error.")

    PS_df = pd.concat(PS_data, ignore_index=True)
    print(PS_df.head())
    return PS_df


def clean_and_reshape(PS_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the patient safety dataframe
    missing = [c for c in needed_cols if c not in PS_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in PS_df ")

    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    PS_clean = (
        PS_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY/REGION", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY/REGION", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    PS_clean["YEAR"] = PS_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format patient safety data")
    print(PS_clean.head())

    #Transposes data to create a wide table 
    PS_wide = PS_clean.pivot_table(
        index="COUNTRY/REGION",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY/REGION x (IndicatorCode, YEAR))")
    print(PS_wide.head())

    return PS_clean, PS_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(PS_clean, PS_wide):
    PS_clean.to_csv("patient_safety_all_long.csv", index=False)
    PS_wide.to_csv("patient_safety_all_wide.csv")

    
    print("Saved patient_safety_all_long.csv and patient_safety_all_wide.csv")
if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    PS_inds = find_PS_indicators(ind_df)

    PS_df = fetch_PS_data(PS_inds)

    PS_long, PS_wide = clean_and_reshape(PS_df)

    save_outputs(PS_long, PS_wide)