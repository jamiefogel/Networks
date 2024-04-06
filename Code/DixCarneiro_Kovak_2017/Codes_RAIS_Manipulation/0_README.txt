***************************
Codes for RAIS Manipulation
***************************

These codes read the RAIS micro-data and generate data at the regional level.
The processed data will be saved in folder ProcessedData_RAIS.

***********************************
Given confidentiality agreements with the Brazilian Ministry of Labor, we cannot
make available the micro-data from RAIS. 

Our replication files make available all of the do-files we have used to process
the micro-data and generate data at the regional level which are then used in our
empirical specifications. All of the log-files of these codes are made available
in these replication files. See codes for details. All of the regional-level 
data are available in these replication files, so that users are able to run
all of the final empirical specifications using regional-level data.

Access to RAIS data from 1992 to 2010 was granted to us by the Brazilian Ministry
We reached Rosangela Farias at rosangela.farias@mte.gov.br and
cget.sppe@mte.gov.br

Access to RAIS data from 1986 to 1991 was granted to us by Joao De Negri from
Instituto de Pesquisa Economica Aplicada. He can be reached at 
joao.denegri@ipea.gov.br
***********************************

The following files must be run SEQUENTIALLY.

***********************************

0a_Sampling1986_2010.do
Samples 3 percent of all individual worker ID's (PIS) that appear in RAIS between 1986 and 2010.
We follow these workers over time.
Creates raw panel am_rais_final.dta

***********************************

0b_Panel_1986_2010.do
Cleans panel constructed in the previous do-file.
Creates final worker panel new_panel.dta
This panel will be used in the estimation of regional premia controlling for worker fixed effects

***********************************

1a_RegionalEarningsPremia.do
Estimates regional earnings premia
Creates data file mmcEarnPremia_main_1986_2010.dta
This will serve as input for the main analyses in the paper

***********************************

1b_RegionalEarningsPremia_Manuf.do
Estimates regional earnings premia -- restricting sample to manufacturing jobs.
Creates data file mmcEarnPremia_manuf_1986_2010.dta
This will serve as input for robustness checks

***********************************

1c_RegionalEarningsPremia_MMC1970.do
Estimates regional earnings premia at a more aggregate level (micro-regions consistent between 1970 and 2010)
Creates data file mmcEarnPremia_mmc1970_1986_2010.dta
This will serve as input for robustness checks

***********************************

1d_ReginalEarningsPremia_RawAvg.do
Computes regional log-earnings averages -- no controls
Creates data file mmcEarnPremia_rawavg_1986_2010.dta
This will serve as input for robustness checks

***********************************

1e_RegionalEarningsPremia_NoIndFE.do
Estimates regional earnings premia -- no industry fixed effect controls
Creates data file mmcEarnPremia_noindfe_1986_2010.dta
This will serve as input for robustness checks

***********************************

1f_RegionalAvgEarnings.do
Estimates regional raw average earnings
Creates data file mmcAvgEarnings_1991_2010.dta
This will serve as input for robustness checks

***********************************

1g_RegionalEarningsPremiaFixedEffects.do
Estimates regional earnings premia controlling for worker fixed effects
Creates data file mmcEarnPremia_wrkrFE_1986_2010.dta
This will serve as input for robustness checks

Codes for regional premia controlling for worker fixed effects and allowing
for time varying returns on these fixed effects can be found in folder
EarnWrkrFE_VaryingReturns
These codes must be run run in sequence and they generate file 
RegionYearFE_all_est_withse.dta which serves as input for robustness checks
Specifically, it is used by file "Codes_RAIS_Regressions\3_Mechanisms_WorkerComp.do"

***********************************

2_StateOwnedFirms.do
Computes regional share of employment in state owned firms
Creates data file share_SOF.dta

***********************************

3a_RegionalEmployment.do
Computes regional formal employment
Creates data file mmcEmployment_main_1986_2010.dta
This will serve as input for the main analyses of the paper

***********************************

3b_RegionalEmployment_MMC1970.do
Computes regional formal employment at a more aggregate level  (micro-regions consistent between 1970 and 2010)
Creates data file mmcEmployment_mmc1970_1986_2010.dta
This will serve as input for robustness checks

***********************************

3c_RegionalEmployment_BySector.do
Computes formal employment at the region x industry level
Creates data file mmcEmployment_bysector_1986_2010
this will serve as the main input when we test for the presence of agglomeration economies

***********************************

4a_PanelOfPlants.do
Constructs a panel of plants/establishments between 1986 and 2010
Creates panel PanelPlantsFinal.dta

***********************************

4b_NumberPlantsMMC.do
Computes the number of plants/establishments between 1986 and 2010
Generates data files NumberPlants.dta and PlantSize.dta

***********************************

5a_PanelOfPlantMMCpairs.do
Panel of region x plant pairs -- an establishment is now considered to be a region x plant pair.
If a plant moves from a region to another, this means a plant exit from the original region / job destruction in
the original region 
This will serve as input for the computation of job destruction and creation rates, plant entry and exit rates.

***********************************

5b_JobDestruction_JobCreation.do
Computes Job Destruction and Job Creation rates between 1986 and t >= 1987 and t <= 1991
Computes Job Destruction and Job Creation rates between 1991 and t >= 1992 and t <= 2010
Creates data files JobCreationDestruction_Base1986.dta and JobCreationDestruction_Base1991.dta

***********************************

5c_PlantEntryExit.do
Computes Plant Entry and Exit rates between 1986 and t >= 1987 and t <= 1991
Computes Plant Entry and Exit rates between 1991 and t >= 1992 and t <= 2010
Creates data files PlantEntryExit_Base1986.dta and PlantEntryExit_Base1991.dta

***********************************