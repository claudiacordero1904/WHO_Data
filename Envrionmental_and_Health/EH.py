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


#Looks for Environment and health specific indicators 
def find_EH_indicators(ind_df):
    search_words = ["Lead paint",
                    "Poisoning (unintentional)",
                    "Diarrhoea",
                    "Water-, sanitation",
                    "Cooking fuels",
                    "Environment attributable",
                    "Environment-attributable",
                    "Sunbed regulations",
                    "UV radiation",
                    "Solar ultraviolet",
                    "Air pollution",
                    "Second-hand smoke",
                    "Beverage tax",
                    "Electrification of health-care facilities",
                    "Deaths attributable to the environment",
                    "Occupational airborne",
                    "Occupational noise",
                    "Occupational injuries",
                    "Occupatinal ergonomic",
                    "Occupational carcinogens",
                    "Radon",
                    "WASH",
                    "Wastewater flows",
                    "Electromagentic fields",
                    "Electric field",
                    ]
    
    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique environment and health-specific indicators sorted by indicator codes
    EH_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Environment and Health Indicators found: ")
    print(EH_inds)

    return EH_inds

#Fetch data for environment and health indicators
def fetch_EH_data(EH_inds):
    EH_data = []

    #Fetches JSON data from each environment and health indicator using the indicator code
    for code in EH_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing environment and health data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        EH_data.append(df_i)

    #No data exists for this indicator code.
    if not EH_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    EH_df = pd.concat(EH_data, ignore_index=True)
    print(EH_df.head())
    return EH_df


def clean_and_reshape(EH_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the environment and health dataframe
    missing = [c for c in needed_cols if c not in EH_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in EH_df ")

    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    EH_clean = (
        EH_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY/REGION", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY/REGION", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    EH_clean["YEAR"] = EH_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format Environment and Health data")
    print(EH_clean.head())

    #Transposes data to create a wide table 
    EH_wide = EH_clean.pivot_table(
        index="COUNTRY/REGION",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY/REGION x (IndicatorCode, YEAR))")
    print(EH_wide.head())

    return EH_clean, EH_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(EH_clean, EH_wide):
    EH_clean.to_csv("EH_all_long.csv", index=False)
    EH_wide.to_csv("EH_all_wide.csv")

    print("Saved EH_all_long.csv and EH_all_wide.csv")

if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    EH_inds = find_EH_indicators(ind_df)

    EH_df = fetch_EH_data(EH_inds)

    EH_long, EH_wide = clean_and_reshape(EH_df)

    save_outputs(EH_long, EH_wide)