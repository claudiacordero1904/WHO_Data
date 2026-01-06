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


#Looks for maternal and reproductive health specific indicators 
def find_MRH_indicators(ind_df):
    search_words = ["births delivered in a health facility",
                    "Births by caesarean section",
                    "Adolescent birth rate",
                    "Family planning",
                    "Antenatal care coverage",
                    "anaemia in pregnant women",
                    "anaemia in women of reproductive age",
                    "Births attended by skilled health personnel",
                    "Maternal mortality ratio"]
    
    pattern = "|".join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique maternal and reproductive health-specific indicators sorted by indicator codes
    MRH_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Maternal and Reproductive Health Indicators found: ")
    print(MRH_inds)

    return MRH_inds

#Fetch data for maternal and reproductive health indicators
def fetch_MRH_data(MRH_inds):
    MRH_data = []

    #Fetches JSON data from each maternal and reproductive health indicator using the indicator code
    for code in MRH_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing maternal and reproductive health data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        MRH_data.append(df_i)

    #No data exists for this indicator code.
    if not MRH_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    MRH_df = pd.concat(MRH_data, ignore_index=True)
    print(MRH_df.head())
    return MRH_df


def clean_and_reshape(MRH_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the maternal and reproductive health dataframe
    missing = [c for c in needed_cols if c not in MRH_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in MRH_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    MRH_clean = (
        MRH_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    MRH_clean["YEAR"] = MRH_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format maternal and reproductive health data")
    print(MRH_clean.head())

    #Transposes data to create a wide table 
    MRH_wide = MRH_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(MRH_wide.head())

    return MRH_clean, MRH_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(MRH_clean, MRH_wide):
    MRH_clean.to_csv("MRH_all_long.csv", index=False)
    MRH_wide.to_csv("MRH_all_wide.csv")

    print("Saved MRH_all_long.csv and MRH_all_wide.csv")

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    MRH_inds = find_MRH_indicators(ind_df)

    MRH_df = fetch_MRH_data(MRH_inds)

    MRH_long, MRH_wide = clean_and_reshape(MRH_df)

    save_outputs(MRH_long, MRH_wide)