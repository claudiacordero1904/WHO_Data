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


#Looks for Malaria specific indicators 
def find_malaria_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("malaria", case=False, na=False)

    #Create dataframe of only unique malaria-specific indicators sorted by indicator codes
    malaria_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Malaria Indicators found: ")
    print(malaria_inds)

    return malaria_inds

#Fetch data for malaria indicators
def fetch_malaria_data(malaria_inds):
    malaria_data = []

    #Fetches JSON data from each malaria indicator using the indicator code
    for code in malaria_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing malaria data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        malaria_data.append(df_i)

    #No data exists for this indicator code.
    if not malaria_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    malaria_df = pd.concat(malaria_data, ignore_index=True)
    print(malaria_df.head())
    return malaria_df


def clean_and_reshape(malaria_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in malaria_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in malaria_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    malaria_clean = (
        malaria_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    malaria_clean["YEAR"] = malaria_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format malaria data")
    print(malaria_clean.head())

    #Transposes data to create a wide table 
    malaria_wide = malaria_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(malaria_wide.head())

    return malaria_clean, malaria_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(malaria_clean, malaria_wide):
    malaria_clean.to_csv("malaria_all_long.csv", index=False)
    malaria_wide.to_csv("malaria_all_wide.csv")

    print("Saved malaria_all_long.csv and malaria_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    malaria_inds = find_malaria_indicators(ind_df)

    malaria_df = fetch_malaria_data(malaria_inds)

    malaria_long, malaria_wide = clean_and_reshape(malaria_df)

    save_outputs(malaria_long, malaria_wide)

