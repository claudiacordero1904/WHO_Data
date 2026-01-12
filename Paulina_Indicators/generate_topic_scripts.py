from pathlib import Path

TEMPLATE = """\
import requests
import pandas as pd
from pathlib import Path


def fetch_all_indicators():
    base_url = "https://ghoapi.azureedge.net/api/Indicator"
    all_rows = []
    url = base_url

    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@odata.nextLink") or js.get("@data.nextLink")

    ind_df = pd.DataFrame(all_rows)
    return ind_df


def find_all_topic_indicators(ind_df):
    search_words = {search_words}

    pattern = "|".join(search_words)
    mask = ind_df["IndicatorName"].str.contains(pattern, case=False, na=False)

    topic_inds = (
        ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]]
        .drop_duplicates()
        .sort_values("IndicatorCode")
        .reset_index(drop=True)
    )

    return topic_inds


def fetch_topic_data(topic_inds):
    topic_data = []

    for code in topic_inds["IndicatorCode"]:
        url = f"https://ghoapi.azureedge.net/api/{{code}}"
        resp = requests.get(url)
        resp.raise_for_status()
        rows = resp.json().get("value", [])

        if not rows:
            continue

        df_i = pd.DataFrame(rows)
        df_i["IndicatorCode"] = code
        topic_data.append(df_i)

    if not topic_data:
        raise SystemExit("No data found")

    topic_df = pd.concat(topic_data, ignore_index=True)
    return topic_df


def clean_and_reshape(topic_df):
    needed_cols = ["SpatialDim", "TimeDim", "IndicatorCode", "NumericValue"]
    missing = [c for c in needed_cols if c not in topic_df.columns]

    if missing:
        if missing == ["NumericValue"]:
            topic_df = topic_df.copy()
            topic_df["NumericValue"] = pd.NA
        else:
            raise ValueError(f"Missing columns: {{missing}}")

    topic_long = (
        topic_df[needed_cols]
        .rename(columns={{"SpatialDim": "COUNTRY", "TimeDim": "YEAR"}})
        .dropna(subset=["COUNTRY", "YEAR", "IndicatorCode"])
    )

    topic_long["YEAR"] = topic_long["YEAR"].astype(int)

    topic_wide = topic_long.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )

    return topic_long, topic_wide


def save_outputs(topic_long, topic_wide):
    outdir = Path(__file__).resolve().parent / "output"
    outdir.mkdir(parents=True, exist_ok=True)

    topic_long.to_csv(outdir / "{long_name}", index=False)
    topic_wide.to_csv(outdir / "{wide_name}")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()
    topic_inds = find_all_topic_indicators(ind_df)
    topic_df = fetch_topic_data(topic_inds)
    topic_long, topic_wide = clean_and_reshape(topic_df)
    save_outputs(topic_long, topic_wide)
"""

TOPICS = {
    "Hepatitis": ["Hepatitis", "HBV", "HCV", "HBsAg"],
    "Assistive_Technology": ["Assistive technology", "assistive", "assistive products", "ATA"],
    "Health_Financing": ["Health financing", "health expenditure", "out-of-pocket", "OOP", "CHE", "GGHE", "EXT", "PVT"],
    "Road_Safety": ["Road safety", "road traffic", "traffic deaths", "seat-belt", "helmet", "drink-driving", "BAC"],
    "Nutrition": ["Nutrition", "Breastfeeding", "infant feeding", "stunting", "wasting", "underweight", "overweight", "obesity", "anaemia", "anemia", "low birthweight"],
    "Priority_Health_Technologies": ["Medical devices", "health technology", "biomedical", "MRI", "CT", "radiotherapy", "mammography", "health infrastructure", "hospitals density"],
    "Health_Taxes": ["Health taxes", "alcoholic beverages", "non-alcoholic beverages", "sugar", "excise", "VAT", "sales tax", "earmarked"],
    "Tobacco_Control": ["Tobacco", "MPOWER", "cigarettes", "ENDS", "heated tobacco", "smokeless", "tobacco tax"],
    "Food_Safety": ["Food safety", "Foodborne", "foodborne", "IHR core capacity on food safety", "JEE food safety"],
    "Child_Mortality": ["Child mortality", "under-five", "neonatal", "infant mortality", "stillbirth", "adolescent mortality"],
    "Tuberculosis": ["Tuberculosis", "TB", "MDR-TB", "RR-TB", "XDR-TB", "TB/HIV"],
    "Vaccine_Preventable_Diseases": ["Vaccine-preventable", "vaccine preventable", "measles", "polio", "diphtheria", "tetanus", "pertussis", "rubella", "mumps", "yellow fever", "rotavirus", "BCG", "HPV"],
    "Mental_Health": ["Mental health", "psychiatrists", "psychologists", "mental hospitals", "suicide", "outpatient", "community residential"],
    "Immunization_Coverage": ["Immunization", "immunisation", "coverage", "HepB3", "DTP3", "MCV1", "MCV2", "Pol3", "PCV3", "Hib3", "RotaC", "PAB"],
    "Violence_Prevention": ["Violence prevention", "child protection", "medico-legal", "dating violence", "life-skills", "home-visiting", "youth violence", "elder abuse"],
    "Sexually_Transmitted_Infections": ["Sexually Transmitted", "STI", "syphilis", "gonorrhoea", "gonorrhea", "chlamydia", "trichomoniasis", "GASP", "Neisseria"],
    "International_Health_Regulations_2005": ["International Health Regulations", "IHR", "SPAR", "JEE"],
    "Water_Sanitation_and_Hygiene_WASH": ["WASH", "water", "sanitation", "hygiene", "handwashing", "open defecation", "diarrhoea", "diarrhea", "unsafe WASH"],
    "Violence_Against_Women": ["Violence against women", "intimate partner violence", "non-partner sexual violence", "IPV"],
}


def main():
    base = Path(__file__).resolve().parent
    for folder, words in TOPICS.items():
        topic_dir = base / folder
        topic_dir.mkdir(parents=True, exist_ok=True)

        script_path = topic_dir / f"{folder}.py"
        long_name = f"{folder.lower()}_all_long.csv"
        wide_name = f"{folder.lower()}_all_wide.csv"

        script_path.write_text(
            TEMPLATE.format(
                search_words=repr(words),
                long_name=long_name,
                wide_name=wide_name,
            ),
            encoding="utf-8"
        )

    print("Created topic scripts for:")
    for k in TOPICS:
        print(" -", k)


if __name__ == "__main__":
    main()
