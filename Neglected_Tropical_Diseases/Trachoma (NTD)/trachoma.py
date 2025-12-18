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


#Looks for Trachoma specific indicators
def find_trachoma_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("trachoma", case=False, na=False)

    #Create dataframe of only unique trachoma-specific indicators sorted by indicator codes
    trachoma_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Trachoma Indicators found: ")
    print(trachoma_inds)

    return trachoma_inds

#Fetch data for trachoma indicators
def fetch_trachoma_data(trachoma_inds):
    trachoma_data = []

    #Fetches JSON data from each trachoma indicator using the indicator code
    for code in trachoma_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing trachoma data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        trachoma_data.append(df_i)

    #No data exists for this indicator code.
    if not trachoma_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    trachoma_df = pd.concat(trachoma_data, ignore_index=True)
    print(trachoma_df.head())
    return trachoma_df


def clean_and_reshape(trachoma_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in trachoma_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trachoma_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    trachoma_clean = (
        trachoma_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    trachoma_clean["YEAR"] = trachoma_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format trachoma data")
    print(trachoma_clean.head())

    #Transposes data to create a wide table 
    trachoma_wide = trachoma_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(trachoma_wide.head())

    return trachoma_clean, trachoma_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(trachoma_clean, trachoma_wide):
    trachoma_clean.to_csv("trachoma_all_long.csv", index=False)
    trachoma_wide.to_csv("trachoma_all_wide.csv")

    print("Saved trachoma_all_long.csv and trachoma_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    trachoma_inds = find_trachoma_indicators(ind_df)

    trachoma_df = fetch_trachoma_data(trachoma_inds)

    trachoma_long, leprosy_wide = clean_and_reshape(trachoma_df)

    save_outputs(trachoma_long, leprosy_wide)