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


#Looks for healthcare workforce statistics specific indicators 
def find_HWS_indicators(ind_df):
    search_words = ['mental health sector', 
                    'biomedical engineers', 
                    'biomedical technicians', 
                    'qualified actively working', 
                    'nursing personnel',
                    'midwifery personnel',
                    'pharmacists',
                    'medical doctors',
                    'medical practicioners',
                    'dentists',
                    'dental assistants',
                    'environmental and occupational health',
                    'community health workers',
                    'dental prosthetic technicians',
                    'medicine professionals']
    
    pattern = '|'.join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique healthcare workforce statistics-specific indicators sorted by indicator codes
    HWS_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Healthcare Workforce Statistics Indicators found: ")
    print(HWS_inds)

    return HWS_inds

#Fetch data for Healthcare Workforce Statistics indicators
def fetch_HWS_data(HWS_inds):
    HWS_data = []

    #Fetches JSON data from each healthcare workforce statistic indicator using the indicator code
    for code in HWS_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing healthcare workforce statistic data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        HWS_data.append(df_i)

    #No data exists for this indicator code.
    if not HWS_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    HWS_df = pd.concat(HWS_data, ignore_index=True)
    print(HWS_df.head())
    return HWS_df


def clean_and_reshape(HWS_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the trachoma dataframe
    missing = [c for c in needed_cols if c not in HWS_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in HWS_df ")
    
    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    HWS_clean = (
        HWS_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    HWS_clean["YEAR"] = HWS_clean["YEAR"].astype(int)

    print("\n Cleaned up long-format healthcare system data")
    print(HWS_clean.head())

    #Transposes data to create a wide table 
    HWS_wide = HWS_clean.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY x (IndicatorCode, YEAR))")
    print(HWS_wide.head())

    return HWS_clean, HWS_wide 


#Save data as csv files in both long and wide format data 
def save_outputs(HWS_clean, HWS_wide):
    HWS_clean.to_csv("HWS_all_long.csv", index=False)
    HWS_wide.to_csv("HWS_all_wide.csv")

    print("Saved HWS_all_long.csv and HWS_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    HWS_inds = find_HWS_indicators(ind_df)

    HWS_df = fetch_HWS_data(HWS_inds)

    HWS_long, HWS_wide = clean_and_reshape(HWS_df)

    save_outputs(HWS_long, HWS_wide)