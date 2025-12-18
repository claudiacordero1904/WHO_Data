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


#Looks for Leprosy specific indicators
def find_Leprosy_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("Leprosy", case=False, na=False)

    #Create dataframe of only unique leprosy-specific indicators sorted by indicator codes
    leprosy_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Lepropsy Indicators found: ")
    print(leprosy_inds)

    return leprosy_inds

#Fetch data for leprosy indicators
def fetch_leprosy_data(leprosy_inds):
    leprosy_data = []

    #Fetches JSON data from each leprosy indicator using the indicator code
    for code in leprosy_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing leprosy data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        leprosy_data.append(df_i)

    #No data exists for this indicator code.
    if not leprosy_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    leprosy_df = pd.concat(leprosy_data, ignore_index=True)
    print(leprosy_df.head())
    return leprosy_df


def clean_and_reshape(leprosy_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the leprosy dataframe
    missing = [c for c in needed_cols if c not in leprosy_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in leprosy_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    leprosy_clean = (
        leprosy_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    leprosy_clean["YEAR"] = leprosy_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format leprosy data")
    print(leprosy_clean.head())

    #Transposes data to create a wide table 
    leprosy_wide = leprosy_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(leprosy_wide.head())

    return leprosy_clean, leprosy_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(leprosy_clean, leprosy_wide):
    leprosy_clean.to_csv("leprosy_all_long.csv", index=False)
    leprosy_wide.to_csv("leprosy_all_wide.csv")

    print("Saved leprosy_all_long.csv and leprosy_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    leprosy_inds = find_Leprosy_indicators(ind_df)

    leprosy_df = fetch_leprosy_data(leprosy_inds)

    leprosy_long, leprosy_wide = clean_and_reshape(leprosy_df)

    save_outputs(leprosy_long, leprosy_wide)

    