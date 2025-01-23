import pandas as pd
import pyreadstat
from config import root, rais
import pyarrow.parquet as pq


# Set base directory paths
base_path =  root + "/Code/replicate_mayara"
monopsonies_path = f"{base_path}/monopsonies"
ipea_path = f"{base_path}/publicdata/IPEA/IPEA_minwage"
tariffs_path = f"{base_path}/publicdata/Tariffs"
harmonized_path = f"{base_path}/raisdictionaries/harmonized"
other_path = f"{base_path}/publicdata/other"


#######################################################
# Data sets we don't have 


# Read Minimum Wage dataset
ipea_minwage, meta = pyreadstat.read_dta(f"{ipea_path}/annual_min_wage.dta")
ipea_minwage.to_pickle(f"{monopsonies_path}/sas/IPEA_minwage.pkl")

# Read Importers and Exporters dataset
importers_exporters, meta = pyreadstat.read_dta(
    f"{monopsonies_path}/dta/fakeid_importers_exporters_allyears_20191213.dta"
)
importers_exporters.to_pickle(f"{monopsonies_path}/sas/importers_exporters.pkl")


# Read crosswalk IBGE subsector to indmatch
crosswalk_ibgesubsector_indmatch = pd.read_csv(
    f"{harmonized_path}/indmatch_ibgesubsector.csv"
)
crosswalk_ibgesubsector_indmatch.to_parquet(f"{monopsonies_path}/sas/crosswalk_ibgesubsector_indmatch.parquet")


# Read crosswalk CNAE95 to IBGE subsector
# XX I'm commenting out he origina lcode b/c we don't have the raw data. Instead, computing the crosswalk myself
#crosswalk_cnae95_ibgesubsector, meta = pyreadstat.read_dta(
#    f"{harmonized_path}/rais_cnae10_to_ibgesubsector.dta"
#)
table = pq.read_table(rais + "parquet_novos/brasil1995.parquet", columns=['subs_ibge','clas_cnae'])
temp = table.to_pandas()
# Assuming your DataFrame is named df
temp = temp.groupby(['subs_ibge', 'clas_cnae']).size().reset_index(name='count')
temp = temp.sort_values(
    by=['clas_cnae', 'count'],
    ascending=[True, False]
)

crosswalk_cnae95_ibgesubsector = temp.drop_duplicates(subset='clas_cnae', keep='first').drop(columns='count')
crosswalk_cnae95_ibgesubsector.rename(columns={"clascnae": "cnae95",'subs_ibge':'ibgesubsector'}, inplace=True)
crosswalk_cnae95_ibgesubsector.to_pickle(f"{monopsonies_path}/sas/crosswalk_cnae95_ibgesubsector.pkl")
del temp

# Read Tariff shocks
cnae95_tariff_changes, meta = pyreadstat.read_dta(
    f"{tariffs_path}/cnae10_tariff_changes_1990_1994.dta"
)
cnae95_tariff_changes.rename(columns={"cnae10": "cnae95"}, inplace=True)
cnae95_tariff_changes.to_pickle(f"{monopsonies_path}/sas/cnae95_tariff_changes_1990_1994.pkl")
cnae95_tariff_changes.to_parquet(f"{monopsonies_path}/sas/cnae95_tariff_changes_1990_1994.parquet")

crosswalk_municipality_to_mmc, meta = pyreadstat.read_dta(
    f"{harmonized_path}/municipality_to_microregion.dta"
)
crosswalk_municipality_to_mmc.to_pickle(f"{monopsonies_path}/sas/crosswalk_municipality_to_mmc.pkl")


# Read Concla CBO02 to CBO94 pairings
crosswalk_CONCLA_cbo02_cbo94 = pd.read_csv(
    f"{other_path}/CBO/raw/CBO94 - CBO2002 - Conversao com 90_noX.csv"
)
crosswalk_CONCLA_cbo02_cbo94.rename(columns={"CBO94": "cbo94", "CBO2002": "cbo02"}, inplace=True)
crosswalk_CONCLA_cbo02_cbo94.to_pickle(f"{monopsonies_path}/sas/crosswalk_CONCLA_cbo02_cbo94.pkl")

# Read mappings to task content of occupations from Gonzaga
crosswalk_cbo02_cbo94_plus, meta = pyreadstat.read_dta(
    f"{other_path}/Gonzaga/crosswalk_cbo02_cbo94_plus.dta"
)
crosswalk_cbo02_cbo94_plus.to_pickle(f"{monopsonies_path}/sas/crosswalk_cbo02_cbo94_plus.pkl")

valid_cbo94_plus, meta = pyreadstat.read_dta(f"{other_path}/Gonzaga/valid_cbo94_plus.dta")
valid_cbo94_plus.to_pickle(f"{monopsonies_path}/sas/valid_cbo94_plus.pkl")

#######################################################
# Data sets we have 


# Read valid CNAE95 list
valid_cnae95 = pd.read_csv(f"{harmonized_path}/valid_cnae95.csv")
valid_cnae95 = valid_cnae95.drop_duplicates()
valid_cnae95.to_parquet(f"{monopsonies_path}/sas/valid_cnae95.parquet")

# Read valid CBO94 list
valid_cbo94 = pd.read_csv(f"{harmonized_path}/valid_cbo94.csv")
valid_cbo94.to_parquet(f"{monopsonies_path}/sas/valid_cbo94.parquet")

tariffs_maindataset_long, meta = pyreadstat.read_dta(
    f"{tariffs_path}/tariffs_maindataset_long.dta"
)
tariffs_maindataset_long.rename(
    columns={"cnae10": "cnae95", "cnae10_des": "cnae95_des"}, inplace=True
)
tariffs_maindataset_long.to_pickle(f"{monopsonies_path}/sas/tariffs_maindataset_long.pkl")


# Read theta_indmatch
theta_indmatch, meta = pyreadstat.read_dta(
    f"{other_path}/DK (2017)/ReplicationFiles/Data_Other/theta_indmatch.dta"
)
theta_indmatch.to_parquet(f"{monopsonies_path}/sas/theta_indmatch.parquet")

# Read municipality to microregion mappings
crosswalk_muni_to_mmc_DK17, meta = pyreadstat.read_dta(
    f"{harmonized_path}/rais_codemun_to_mmc_1991_2010.dta"
)
crosswalk_muni_to_mmc_DK17.to_parquet(f"{monopsonies_path}/sas/crosswalk_muni_to_mmc_DK17.parquet")


# XX Created this
# Read agegroup to age mappings
crosswalk_agegroup_to_age, meta = pyreadstat.read_dta(
    f"{harmonized_path}/agegroup_to_age.dta"
)
crosswalk_agegroup_to_age.to_pickle(f"{monopsonies_path}/sas/crosswalk_agegroup_to_age.pkl")

