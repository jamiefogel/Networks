* Generates datafiles with number of plants in each mmc, as well as the average
* size of plants by mmc.

clear

set more off

global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data   "${root}ProcessedData_RAIS\Plants\PanelOfPlants\"
global result "${root}ProcessedData_RAIS\Plants\"

log using ${result}NumberPlantsMMC.log, replace

use ${data}PanelPlantsFinal

* Manufacturing
gen Manuf = 0
replace Manuf = 1 if subs_ibge0 == "4506" | subs_ibge0 == "4507" | subs_ibge0 == "4508" | ///
                     subs_ibge0 == "4509" | subs_ibge0 == "4510" | subs_ibge0 == "4511" | ///
					 subs_ibge0 == "4512" | subs_ibge0 == "4513" | subs_ibge0 == "4514" | ///
					 subs_ibge0 == "4515" | subs_ibge0 == "4516" | subs_ibge0 == "4517"				 
* Primary (Agriculture/Mining)
gen Primary = 0
replace Primary = 1 if subs_ibge0 == "1101" | subs_ibge0 == "4405" 
gen Traded = Primary + Manuf
* Non-Traded jobs
gen NonTraded = 0
replace NonTraded = 1 if subs_ibge0 == "2202" | subs_ibge0 == "2203" | subs_ibge0 == "3304" | ///
                         subs_ibge0 == "4618" | subs_ibge0 == "5719" | subs_ibge0 == "5820" | ///
						 subs_ibge0 == "5821" | subs_ibge0 == "5822" | subs_ibge0 == "5823" | ///
						 subs_ibge0 == "5824" | subs_ibge0 == "5825" | subs_ibge0 == "9999"
						 
drop if subs_ibge0 == "5719" | subs_ibge0 == "9999"						 
						 
********************************************************************************
********************************************************************************

keep if emp > 0 & emp ~= .
keep mmc cnpj emp year Manuf Traded NonTraded Primary
duplicates drop

gen size1 = (emp < 5)
gen size2 = (emp >= 5 & emp < 10)
gen size3 = (emp >= 10 & emp < 20)
gen size4 = (emp >= 20 & emp < 50)
gen size5 = (emp >= 50 & emp < 100)
gen size6 = (emp >= 100)

sort mmc year
gen ones = 1
by mmc year: egen nplants = total(ones)
by mmc year: egen nplants_size1_ = total(size1)
by mmc year: egen nplants_size2_ = total(size2)
by mmc year: egen nplants_size3_ = total(size3)
by mmc year: egen nplants_size4_ = total(size4)
by mmc year: egen nplants_size5_ = total(size5)
by mmc year: egen nplants_size6_ = total(size6)
by mmc year: egen nworkers = total(emp)
gen avg_plant_size = nworkers / nplants

by mmc year: egen nplants_Manuf = total(ones*Manuf)
by mmc year: egen nworkers_Manuf = total(emp*Manuf)
gen avg_plant_size_Manuf = nworkers_Manuf / nplants_Manuf

by mmc year: egen nplants_Tr = total(ones*Traded)
by mmc year: egen nworkers_Tr = total(emp*Traded)
gen avg_plant_size_Tr = nworkers_Tr / nplants_Tr

by mmc year: egen nplants_NT = total(ones*NonTraded)
by mmc year: egen nworkers_NT = total(emp*NonTraded)
gen avg_plant_size_NT = nworkers_NT / nplants_NT

by mmc year: egen nplants_Primary = total(ones*Primary)
by mmc year: egen nworkers_Primary = total(emp*Primary)
gen avg_plant_size_Primary = nworkers_Primary / nplants_Primary

keep mmc year nplants avg_plant_size nplants_Manuf avg_plant_size_Manuf ///
     nplants_Tr avg_plant_size_Tr nplants_NT avg_plant_size_NT nplants_Primary avg_plant_size_Primary nplants_size*
duplicates drop
sort mmc year
reshape wide nplants* avg_plant_size*, i(mmc) j(year)

preserve
	keep mmc nplants*
	sort mmc
	save ${result}NumberPlants, replace
restore

preserve
	keep mmc avg_plant_size*
	sort mmc
	save ${result}PlantSize, replace
restore

log close
