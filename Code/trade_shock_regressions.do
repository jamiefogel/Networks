

tempfile iotas gammas
import delimited \\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\sbm_output\model_trade_shock_wblocks.csv, colrange(2:3) clear 
tostring wid, format("%011.0f") replace
rename worker_blocks_level_0 iota
save `iotas'
import delimited \\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\sbm_output\model_trade_shock_jblocks.csv, colrange(2:3) clear 
rename job_blocks_level_0 iota
save `gammas'

global keepvars "id_estab subs_ibge codemun pis cbo1994 rem_dez mmc log_rem female educ1 educ2 educ3 educ4 educ5 educ6 educ7 educ7 educ8 educ9 age1 age2 age3 age4 age5 mmc2"
use "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\ProcessedData_RAIS\RegionalEarnPremia\processed_rais_pull1990.dta", clear

* Define wid and jid
rename pis wid
gen jid = id_estab + "_" + substr(cbo1994, 1, 4)
* Merge on iotas and gammas
merge 1:1 wid using `iotas' 
keep if inlist(_merge,1,3)
drop _merge
merge m:1 jid using `gammas' 
keep if inlist(_merge,1,3)
drop _merge

* Merge on the tariff changes in ../Data/tariff_chg_kume_subsibge by subs_ibge
merge m:1 subs_ibge using "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\Data_Other\subsibge_to_subsibge_rais.dta", keep(3) nogen
merge m:1 subsibge using "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\Data\tariff_chg_kume_subsibge.dta", keepusing(dlnonetariff_1990_1995)
* Change in tariffs will be missing for nontradables
replace dlnonetariff_1990_1995 = 0 if missing(dlnonetariff_1990_1995)


* Append data from a later year, say 2000, and compute changes in earnings for individual workers
tab subsibge _merge