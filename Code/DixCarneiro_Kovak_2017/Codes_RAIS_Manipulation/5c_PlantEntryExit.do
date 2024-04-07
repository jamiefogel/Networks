
* Computes establishment entry and exit rates at the regional level.

clear

global root "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code/DixCarneiro_Kovak_2017/"

global result "${root}ProcessedData_RAIS/JobDestruction_JobCreation/"

log using ${result}PlantEntryExit.log, replace

******************************************************
* Entry/Exit between 1986 and year t
******************************************************

clear

use ${result}PanelPlantsFinal

drop if year >= 1992

drop if subs_ibge == "5719" | subs_ibge == "9999"

drop imputed cnpj

gen active = (emp > 0)

gen aux86 = active if year == 1986
sort cnpj_mmc
by cnpj_mmc: egen active86 = max(aux86)
drop aux86

gen x_et = (active86 + active)/2
gen g_et = (active - active86)/x_et
sort mmc year
by mmc year: egen active_mmc = total(active)
by mmc: egen active_mmc86 = total(active*(year == 1986))

preserve
keep mmc year active_mmc active_mmc86
duplicates drop
gen X_rt = (active_mmc86 + active_mmc)/2
keep mmc year X_rt
sort mmc year
save ${result}temp_mmc, replace
restore

sort mmc year
merge m:1 mmc year using ${result}temp_mmc
drop _merge
erase ${result}temp_mmc.dta

gen pos_g = (g_et > 0)&(g_et ~= .)
gen pos_term = (x_et/X_rt)*g_et*pos_g

gen neg_g = (g_et < 0)&(g_et ~= .)
gen neg_term = (x_et/X_rt)*abs(g_et)*neg_g

sort mmc year
by mmc year: egen POS_rt = total(pos_term)
by mmc year: egen NEG_rt = total(neg_term)
gen SUM_rt = POS_rt + NEG_rt

keep mmc year POS_rt NEG_rt SUM_rt
duplicates drop
drop if mmc == "." | mmc == ""
sort mmc year

rename POS_rt POS
rename NEG_rt NEG
rename SUM_rt SUM
reshape wide POS NEG SUM, i(mmc) j(year)

sort mmc

save ${result}PlantEntryExit_Base1986, replace



*********************************************
* Firm Entry and Exit between 1991 and year t
*********************************************

clear

use ${result}PanelPlantsFinal

drop imputed cnpj

keep if year >= 1991

drop if subs_ibge == "5719" | subs_ibge == "9999"

gen active = (emp > 0)

gen aux91 = active if year == 1991
sort cnpj_mmc
by cnpj_mmc: egen active91 = max(aux91)
drop aux91

gen x_et = (active91 + active)/2
gen g_et = (active - active91)/x_et
sort mmc year
by mmc year: egen active_mmc = total(active)
by mmc: egen active_mmc91 = total(active*(year == 1991))

preserve
keep mmc year active_mmc active_mmc91
duplicates drop
gen X_rt = (active_mmc91 + active_mmc)/2
keep mmc year X_rt
sort mmc year
save ${result}temp_mmc, replace
restore

sort mmc year
merge m:1 mmc year using ${result}temp_mmc
drop _merge
erase ${result}temp_mmc.dta

gen pos_g = (g_et > 0)&(g_et ~= .)
gen pos_term = (x_et/X_rt)*g_et*pos_g

gen neg_g = (g_et < 0)&(g_et ~= .)
gen neg_term = (x_et/X_rt)*abs(g_et)*neg_g

sort mmc year
by mmc year: egen POS_rt = total(pos_term)
by mmc year: egen NEG_rt = total(neg_term)
gen SUM_rt = POS_rt + NEG_rt

keep mmc year POS_rt NEG_rt SUM_rt
duplicates drop
drop if mmc == "." | mmc == ""
sort mmc year

rename POS_rt POS
rename NEG_rt NEG
rename SUM_rt SUM
reshape wide POS NEG SUM, i(mmc) j(year)

sort mmc

save ${result}PlantEntryExit_Base1991, replace

log close
