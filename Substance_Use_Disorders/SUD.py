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


#Looks for substance-use disorder specific indicators 
def find_SUD_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("substance use disorders", case=False, na=False)

    #Create dataframe of only unique substance-use-specific indicators sorted by indicator codes
    SUD_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Substance Use Indicators found: ")
    print(SUD_inds)

    return SUD_inds

#Fetch data for substance use indicators
def fetch_substance_use_data(SUD_inds):
    SUD_data = []

    #Fetches JSON data from each substance use indicator using the indicator code
    for code in SUD_inds["IndicatorCode"]:
        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing substance use data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        SUD_data.append(df_i)

    #No data exists for this indicator code.
    if not SUD_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    SUD_df = pd.concat(SUD_data, ignore_index=True)
    print(SUD_df.head())
    return SUD_df


def clean_and_reshape(SUD_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the substance use dataframe
    missing = [c for c in needed_cols if c not in SUD_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in SUD_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    SUD_clean = (
        SUD_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    SUD_clean["YEAR"] = SUD_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format SUD data")
    print(SUD_clean.head())

    #Transposes data to create a wide table 
    SUD_wide = SUD_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(SUD_wide.head())

    return SUD_clean, SUD_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(SUD_clean, SUD_wide):
    SUD_clean.to_csv("SUD_all_long.csv", index=False)
    SUD_wide.to_csv("SUD_all_wide.csv")

    print("Saved SUD_all_long.csv and SUD_all_wide.csv")

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    SUD_inds = find_SUD_indicators(ind_df)

    SUD_df = fetch_substance_use_data(SUD_inds)

    SUD_long, SUD_wide = clean_and_reshape(SUD_df)
    
    save_outputs(SUD_long, SUD_wide)
