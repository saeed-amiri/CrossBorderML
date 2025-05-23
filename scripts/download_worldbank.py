"""
Downloading data and updating README file
"""

import os
import typing
from datetime import datetime

import requests


INDICATORS = {
    # Economic & Investment
    "gdp": "NY.GDP.MKTP.CD",
    "gdp_per_capita": "NY.GDP.PCAP.CD",
    "gni_per_capita": "NY.GNP.PCAP.CD",
    "gross_capital_formation_pct_gdp": "NE.GDI.TOTL.ZS",
    "current_account_balance_pct_gdp": "BN.CAB.XOKA.GD.ZS",
    "fdi_inflows": "BX.KLT.DINV.CD.WD",

    # Prices & Inflation
    "inflation": "FP.CPI.TOTL.ZG",

    # Population & Demographics
    "population_total": "SP.POP.TOTL",
    "population_growth": "SP.POP.GROW",
    "urban_population_pct": "SP.URB.TOTL.IN.ZS",
    "population_15_64_pct": "SP.POP.1564.TO.ZS",
    "population_65_plus_pct": "SP.POP.65UP.TO.ZS",

    # Health
    "life_expectancy": "SP.DYN.LE00.IN",
    "infant_mortality": "SP.DYN.IMRT.IN",
    "health_expenditure_per_capita": "SH.XPD.CHEX.PC.CD",

    # Education
    "school_enrollment_primary": "SE.PRM.ENRR",
    "school_enrollment_secondary": "SE.SEC.ENRR",
    "adult_literacy_rate": "SE.ADT.LITR.ZS",

    # Labor
    "labor_participation_rate": "SL.TLF.CACT.ZS",
    "unemployment_total": "SL.UEM.TOTL.ZS",

    # Environment
    "co2_emissions_per_capita": "EN.ATM.CO2E.PC",
    "renewable_energy_pct": "EG.FEC.RNEW.ZS",
    "forest_area_pct": "AG.LND.FRST.ZS",

    # Infrastructure & Technology
    "access_to_electricity": "EG.ELC.ACCS.ZS",
    "internet_users_pct": "IT.NET.USER.ZS",
}


BASE_URL = \
    "https://api.worldbank.org/v2/en/indicator/{code}?downloadformat=csv"

README: str = "data/README.md"


def download_indicator(name: str,
                       code: str,
                       log: typing.TextIO,
                       dest_dir: str = "data/raw"
                       ) -> None:
    """Download the data"""
    url = BASE_URL.format(code=code)
    log.write(f"- Downloading '{name}' from:\n")
    log.write(f"> {url}\n")
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        print(f"\nError! Failed to download '{name}'\n")

    zip_path = os.path.join(dest_dir, f"{name}.zip")
    with open(zip_path, "wb") as f:
        f.write(response.content)
    log.write(f"> Saved to {zip_path}\n")


if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    with open(README, 'a', encoding='utf-8') as LOG:
        LOG.write(f'\n## [{datetime.now()}]:\n')
        for NAME, CODE in INDICATORS.items():
            download_indicator(NAME, CODE, LOG)
