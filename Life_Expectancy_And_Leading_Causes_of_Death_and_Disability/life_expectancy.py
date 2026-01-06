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


#Looks for life expectancy and death or disability specific indicators 
def find_LE_indicators(ind_df):
    search_words = ["child deaths",
                    "adolescent mortality rate",
                    "child mortality",
                    "stillbirth rate",
                    "Adult mortality",
                    "Poisoning (unintentional)",
                    "Suicide rates",
                    "NCD deaths",
                    "Dying between the exact ages",
                    "Life expectancy",
                    "Road traffic deaths",
                    "Road traffic death rate"
                    "Homicides",
                    "Maternal deaths",
                    "Maternal mortality"]

    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique life expectancy and death or disability-specific indicators sorted by indicator codes
    LE_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Life Expectancy and Death/Disability Indicators found: ")
    print(LE_inds)

    return LE_inds

#Fetch data for life expectancy and death/disability indicators
def fetch_LE_data(LE_inds):
    LE_data = []

    #Fetches JSON data from each life expectancy and death/disability indicator using the indicator code
    for code in LE_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing life expectancy and death/disability data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        LE_data.append(df_i)

    #No data exists for this indicator code.
    if not LE_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    LE_df = pd.concat(LE_data, ignore_index=True)
    print(LE_df.head())
    return LE_df


def clean_and_reshape(LE_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in LE_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in LE_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    LE_clean = (
        LE_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    LE_clean["YEAR"] = LE_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format life expectancy data")
    print(LE_clean.head())

    #Transposes data to create a wide table 
    LE_wide = LE_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(LE_wide.head())

    return LE_clean, LE_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(LE_clean, LE_wide):
    LE_clean.to_csv("life_expectancy_all_long.csv", index=False)
    LE_wide.to_csv("life_expectancy_all_wide.csv")

    print("Saved life_expectancy_all_long.csv and life_expectancy_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    LE_inds = find_LE_indicators(ind_df)

    LE_df = fetch_LE_data(LE_inds)

    LE_long, LE_wide = clean_and_reshape(LE_df) 
    
    save_outputs(LE_long, LE_wide)