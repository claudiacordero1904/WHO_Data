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


#Looks for Yaws specific indicators
def find_yaws_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("yaws", case=False, na=False)

    #Create dataframe of only unique yaws-specific indicators sorted by indicator codes
    yaws_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Yaws Indicators found: ")
    print(yaws_inds)

    return yaws_inds

#Fetch data for trachoma indicators
def fetch_yaws_data(yaws_inds):
    yaws_data = []

    #Fetches JSON data from each yaws indicator using the indicator code
    for code in yaws_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing yaws data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        yaws_data.append(df_i)

    #No data exists for this indicator code.
    if not yaws_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    yaws_df = pd.concat(yaws_data, ignore_index=True)
    print(yaws_df.head())
    return yaws_df


def clean_and_reshape(yaws_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the Yaws dataframe
    missing = [c for c in needed_cols if c not in yaws_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in yaws_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    yaws_clean = (
        yaws_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    yaws_clean["YEAR"] = yaws_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format yaws data")
    print(yaws_clean.head())

    #Transposes data to create a wide table 
    yaws_wide = yaws_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(yaws_wide.head())

    return yaws_clean, yaws_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(yaws_clean, yaws_wide):
    yaws_clean.to_csv("yaws_all_long.csv", index=False)
    yaws_wide.to_csv("yaws_all_wide.csv")

    print("Saved yaws_all_long.csv and yaws_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    yaws_inds = find_yaws_indicators(ind_df)

    yaws_df = fetch_yaws_data(yaws_inds)

    yaws_long, yaws_wide = clean_and_reshape(yaws_df)

    save_outputs(yaws_long, yaws_wide)