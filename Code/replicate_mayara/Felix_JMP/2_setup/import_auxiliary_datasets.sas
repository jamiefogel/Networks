/*
	Import auxiliary datasets to SAS

*/

libname monopsas			'/proj/patkin/projects/monopsonies/sas';

/* Minimum wage */

proc import datafile="/proj/patkin/publicdata/IPEA/IPEA_minwage/annual_min_wage.dta"
	     out=monopsas.IPEA_minwage
	     dbms=dta
	     replace;
run;

/* List of exporters and importers */
proc import datafile="/proj/patkin/projects/monopsonies/dta/fakeid_importers_exporters_allyears_20191213.dta"
	     out=monopsas.importers_exporters
	     dbms=dta
	     replace;
run;

/* Valid list of CNAE95 */
proc import datafile="/proj/patkin/raisdictionaries/harmonized/valid_cnae95.csv"
	     out=monopsas.valid_cnae95
	     dbms=csv
	     replace;
run;

proc import datafile="/proj/patkin/raisdictionaries/harmonized/rais_cnae10_to_ibgesubsector.dta"
	     out=monopsas.crosswalk_cnae95_ibgesubsector
	     dbms=dta
	     replace;
run;

data monopsas.crosswalk_cnae95_ibgesubsector;
	set monopsas.crosswalk_cnae95_ibgesubsector;
	rename cnae10=cnae95;
run;

/* Valid list of CBO94 */
proc import datafile="/proj/patkin/raisdictionaries/harmonized/valid_cbo94.csv"
	     out=monopsas.valid_cbo94
	     dbms=csv
	     replace;
run;

/* Tariff shocks */
proc import datafile="/proj/patkin/publicdata/Tariffs/cnae10_tariff_changes_1990_1994.dta"
	     out=monopsas.cnae95_tariff_changes_1990_1994
	     dbms=dta
	     replace;
run;

data monopsas.cnae95_tariff_changes_1990_1994;
	set monopsas.cnae95_tariff_changes_1990_1994;
	rename cnae10=cnae95;
run;

proc import datafile="/proj/patkin/publicdata/Tariffs/tariffs_maindataset_long.dta"
	     out=monopsas.tariffs_maindataset_long
	     dbms=dta
	     replace;
run;

data monopsas.tariffs_maindataset_long;
	set monopsas.tariffs_maindataset_long;
	rename cnae10=cnae95;
	rename cnae10_des=cnae95_des;
run;

proc import datafile="/proj/patkin/raisdictionaries/harmonized/indmatch_ibgesubsector.csv"
	     out=monopsas.crosswalk_ibgesubsector_indmatch
	     dbms=csv
	     replace;
run;

proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_other/theta_indmatch.dta"
	     out=monopsas.theta_indmatch
	     dbms=dta
	     replace;
run;

/* Municipality to microregion */
/* DK (2017) municipality -- mmc mapping */
proc import datafile="/proj/patkin/raisdictionaries/harmonized/rais_codemun_to_mmc_1991_2010.dta"
	     out=monopsas.crosswalk_muni_to_mmc_DK17
	     dbms=dta
	     replace;
run;


proc import datafile="/proj/patkin/raisdictionaries/harmonized/municipality_to_microregion.dta"
	     out=monopsas.crosswalk_municipality_to_mmc
	     dbms=dta
	     replace;
run;

/* Agegroup to age */
proc import datafile="/proj/patkin/raisdictionaries/harmonized/agegroup_to_age.dta"
	     out=monopsas.crosswalk_agegroup_to_age
	     dbms=dta
	     replace;
run;

/* Concla CBO02 to CBO94 pairings */
proc import datafile="/proj/patkin/publicdata/CBO/raw/CBO94 - CBO2002 - Conversao com 90_noX.csv"
	     out=monopsas.crosswalk_CONCLA_cbo02_cbo94
	     dbms=csv
	     replace;
run;

data monopsas.crosswalk_CONCLA_cbo02_cbo94;
	set monopsas.crosswalk_CONCLA_cbo02_cbo94;
	rename CBO94=cbo94 CBO2002=cbo02;
run;

/* Mappings to task content of occupations from Gonzaga */
proc import datafile="/proj/patkin/publicdata/other/Gonzaga/crosswalk_cbo02_cbo94_plus.dta"
	     out=monopsas.crosswalk_cbo02_cbo94_plus
	     dbms=dta
	     replace;
run;

proc import datafile="/proj/patkin/publicdata/other/Gonzaga/valid_cbo94_plus.dta"
	     out=monopsas.valid_cbo94_plus
	     dbms=dta
	     replace;
run;
