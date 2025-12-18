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


#Looks for HIV specific indicators 
def find_HIV_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("HIV", case=False, na=False)

    #Create dataframe of only unique HIV-specific indicators sorted by indicator codes
    HIV_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n HIV Indicators found: ")
    print(HIV_inds)

    return HIV_inds

#Fetch data for HIV indicators
def fetch_HIV_data(HIV_inds):
    HIV_data = []

    #Fetches JSON data from each HIV indicator using the indicator code
    for code in HIV_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing HIV data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        HIV_data.append(df_i)

    #No data exists for this indicator code.
    if not HIV_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    HIV_df = pd.concat(HIV_data, ignore_index=True)
    print(HIV_df.head())
    return HIV_df


def clean_and_reshape(HIV_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in HIV_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trachoma_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    HIV_clean = (
        HIV_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    HIV_clean["YEAR"] = HIV_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format HIV data")
    print(HIV_clean.head())

    #Transposes data to create a wide table 
    HIV_wide = HIV_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(HIV_wide.head())

    return HIV_clean, HIV_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(HIV_clean, HIV_wide):
    HIV_clean.to_csv("HIV_all_long.csv", index=False)
    HIV_wide.to_csv("HIV_all_wide.csv")

    print("Saved HIV_all_long.csv and HIV_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    HIV_inds = find_HIV_indicators(ind_df)

    HIV_df = fetch_HIV_data(HIV_inds)

    HIV_long, HIV_wide = clean_and_reshape(HIV_df)

    save_outputs(HIV_long, HIV_wide)