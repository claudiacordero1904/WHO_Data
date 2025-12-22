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


#Looks for Electrification of Health Facilities specific indicators 
def find_EHF_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("electricity supply", case=False, na=False)

    #Create dataframe of only unique Electrificatin of Health Facilities-specific indicators sorted by indicator codes
    EHF_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Electrification of Health Facilities Indicators found: ")
    print(EHF_inds)

    return EHF_inds

#Fetch data for EHF indicators
def fetch_EHF_data(EHF_inds):
    EHF_data = []

    #Fetches JSON data from each EHF indicator using the indicator code
    for code in EHF_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing EHF data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        EHF_data.append(df_i)

    #No data exists for this indicator code.
    if not EHF_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    EHF_df = pd.concat(EHF_data, ignore_index=True)
    print(EHF_df.head())
    return EHF_df


def clean_and_reshape(EHF_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in EHF_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in EHF_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    EHF_clean = (
        EHF_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    EHF_clean["YEAR"] = EHF_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format EHF data")
    print(EHF_clean.head())

    #Transposes data to create a wide table 
    EHF_wide = EHF_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(EHF_wide.head())

    return EHF_clean, EHF_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(EHF_clean, EHF_wide):
    EHF_clean.to_csv("EHF_all_long.csv", index=False)
    EHF_wide.to_csv("EHF_all_wide.csv")

    print("Saved EHF_all_long.csv and EHF_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    EHF_inds = find_EHF_indicators(ind_df)

    EHF_df = fetch_EHF_data(EHF_inds)

    EHF_long, EHF_wide = clean_and_reshape(EHF_df)

    save_outputs(EHF_long, EHF_wide)