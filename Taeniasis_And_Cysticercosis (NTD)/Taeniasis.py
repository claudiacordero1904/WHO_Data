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


#Looks for Taenia solium specific indicators (all indicators under the Taeniasis and cysticerocosis category
#include a quantiative measure of pigs)
def find_taenia_indicators(ind_df):
    mask = ind_df["IndicatorName"].str.contains("pigs", case=False, na=False)

    #Create dataframe of only unique taenia-specific indicators sorted by indicator codes
    taenia_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Taenia Indicators found: ")
    print(taenia_inds)

    return taenia_inds

#Fetch data for Taenia indicators
def fetch_taenia_data(taenia_inds):
    taenia_data = []

    #Fetches JSON data from each taenia indicator using the indicator code
    for code in taenia_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing taenia data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        taenia_data.append(df_i)

    #No data exists for this indicator code.
    if not taenia_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    taenia_df = pd.concat(taenia_data, ignore_index=True)
    print(taenia_df.head())
    return taenia_df


def clean_and_reshape(taenia_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in taenia_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in trachoma_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    taenia_clean = (
        taenia_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    taenia_clean["YEAR"] = taenia_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format taenia data")
    print(taenia_clean.head())

    #Transposes data to create a wide table 
    taenia_wide = taenia_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(taenia_wide.head())

    return taenia_clean, taenia_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(taenia_clean, taenia_wide):
    taenia_clean.to_csv("taenia_all_long.csv", index=False)
    taenia_wide.to_csv("taenia_all_wide.csv")

    print("Saved taenia_all_long.csv and taenia_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    taenia_inds = find_taenia_indicators(ind_df)

    taenia_df = fetch_taenia_data(taenia_inds)

    taenia_long, taenia_wide = clean_and_reshape(taenia_df)

    save_outputs(taenia_long, taenia_wide)



