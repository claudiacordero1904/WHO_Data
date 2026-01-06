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


#Looks for Oral health specific indicators 
def find_OH_indicators(ind_df):
    search_words = ["Dentists", "Oral health", "Dental", "oral health"]
    pattern = "|".join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique oral health-specific indicators sorted by indicator codes
    OH_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Oral Health Indicators found: ")
    print(OH_inds)

    return OH_inds

#Fetch data for oral health indicators
def fetch_OH_data(OH_inds):
    OH_data = []

    #Fetches JSON data from each oral health indicator using the indicator code
    for code in OH_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing oral health data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        OH_data.append(df_i)

    #No data exists for this indicator code.
    if not OH_data:
        raise SystemExit("No data collected. Check API connection for error.")

    OH_df = pd.concat(OH_data, ignore_index=True)
    print(OH_df.head())
    return OH_df


def clean_and_reshape(OH_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the oral health dataframe
    missing = [c for c in needed_cols if c not in OH_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in OH_df ")

    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    OH_clean = (
        OH_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    OH_clean["YEAR"] = OH_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format oral health data")
    print(OH_clean.head())

    #Transposes data to create a wide table 
    OH_wide = OH_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(OH_wide.head())

    return OH_clean, OH_wide


#Save data as csv files in both long and wide format data 
def save_outputs(OH_clean, OH_wide):
    OH_clean.to_csv("oral_health_all_long.csv", index=False)
    OH_wide.to_csv("oral_health_all_wide.csv")

    print("Saved oral_health_all_long.csv and oral_health_all_wide.csv")        

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    OH_inds = find_OH_indicators(ind_df)

    OH_df = fetch_OH_data(OH_inds)

    OH_long, OH_wide = clean_and_reshape(OH_df)            
    
    save_outputs(OH_long, OH_wide)