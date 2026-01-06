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


#Looks for Global Dementia Observatory specific indicators 
def find_GDO_indicators(ind_df):
    search_words = ["Dementia inclusion risk reduction",
                    "Dementia: implementation level",
                    "Cholesterol, mean total",
                    "Physical activity",
                    "Obesity among adults",
                    "Hypertension among adults",
                    "Overweight among adults",
                    "Alcohol, total per capita",
                    "Depression, population-based prevalence",
                    "Dementia national plan",
                    "Dementia inclusion",
                    "Dementia plans",
                    "Inclusion of human rights as a guiding principle of the dementia plan",
                    "Dementia plan",
                    "Dementia: legislation",
                    "Percentage of sub-national regions covered by dementia",
                    "Dementia sub-national plan(s)",
                    "Dementia care",
                    "Dementia facilities",
                    "Dementia paliative",
                    "Dementia diagnostic",
                    "Dementia standards",
                    "Dementia workforce",
                    "Psychiatrists working in mental health sector",
                    "Dementia psychosocial",
                    "Dementia training",
                    "Dementia: Adult day centres",
                    "Dementia nongovernmental organization",
                    "Pharmacists",
                    "Medical doctors",
                    "Nursing and midwifery personnel",
                    "Dementia diagnosis",
                    "Dementia assistive technology",
                    "Dementia treatment",
                    "Beds, hospital beds",
                    "Health infrastructure",
                    "Dementia research",
                    "Demential integrated",
                    "Dementia iplementation",
                    "Dementia carer",
                    "Dementia campaign",
                    "Dementia-friendly",
                    "Dementia routine monitoring",
                    "Dementia reporting"]

    pattern = "|".join(search_words)

    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    #Create dataframe of only unique Global Dementia Observatory-specific indicators sorted by indicator codes
    GDO_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    print("\n Global Dementia Observatory Indicators found: ")
    print(GDO_inds)

    return GDO_inds

#Fetch data for GDO indicators
def fetch_GDO_data(GDO_inds):
    GDO_data = []       
    #Fetches JSON data from each GDO indicator using the indicator code
    for code in GDO_inds["IndicatorCode"]:

        print("\n Fetching data for: ", code)
        url = f"https://ghoapi.azureedge.net/api/{code}"
        resp = requests.get(url)
        js = resp.json()
        rows = js.get("value", [])

        if not rows:
            continue

        #Creates dataframe of rows containing GDO data for all indicators
        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        GDO_data.append(df_i)

    #No data exists for this indicator code.
    if not GDO_data:
        raise SystemExit("No data collected. Check API connection for error.")
    
    GDO_df = pd.concat(GDO_data, ignore_index=True)
    print(GDO_df.head())
    return GDO_df


def clean_and_reshape(GDO_df):

    #Columns that will appear on final CSV files
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    
    #Checks if any of the neccessary columns are missing in the GDO dataframe
    missing = [c for c in needed_cols if c not in GDO_df.columns]
    if missing:
        raise ValueError(f"Expected columns {needed_cols}, but missing {missing} in GDO_df ")

    #Creates new dataframe with cleaned data
    #Countries and regions are referred to as SpatialDim in GHO data, which is why they're being renamed
    GDO_clean = (
        GDO_df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY/REGION", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY/REGION", "YEAR", "IndicatorCode"])
        )
    
    #Ensure year is numeric
    GDO_clean["YEAR"] = GDO_clean["YEAR"].astype(int)
    print("\n Cleaned up long-format GDO data")
    print(GDO_clean.head())

    #Transposes data to create a wide table 
    GDO_wide = GDO_clean.pivot_table(
        index="COUNTRY/REGION",
        columns=["IndicatorCode", "YEAR"],
        values = "NumericValue"
    )

    print("\n Wide table (COUNTRY/REGION x (IndicatorCode, YEAR))")
    print(GDO_wide.head())

    return GDO_clean, GDO_wide 

#Save data as csv files in both long and wide format data 
def save_outputs(GDO_clean, GDO_wide):
    GDO_clean.to_csv("GDO_all_long.csv", index=False)
    GDO_wide.to_csv("GDO_all_wide.csv")
    print("Saved GDO_all_long.csv and GDO_all_wide.csv")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    GDO_inds = find_GDO_indicators(ind_df)

    GDO_df = fetch_GDO_data(GDO_inds)

    GDO_long, GDO_wide = clean_and_reshape(GDO_df)

    save_outputs(GDO_long, GDO_wide)