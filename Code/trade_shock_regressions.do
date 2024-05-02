

tempfile iotas gammas
import delimited \\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\sbm_output\model_trade_shock_wblocks.csv, colrange(2:3) clear 
tostring wid, format("%011.0f") replace
rename worker_blocks_level_0 iota
save `iotas'
import delimited \\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\sbm_output\model_trade_shock_jblocks.csv, colrange(2:3) clear 
rename job_blocks_level_0 gamma
save `gammas'

global keepvars "id_estab subs_ibge codemun pis cbo1994 rem_dez mmc log_rem female uf educ1 educ2 educ3 educ4 educ5 educ6 educ7 educ7 educ8 educ9 age1 age2 age3 age4 age5 mmc2"
use $keepvars using "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\ProcessedData_RAIS\RegionalEarnPremia\processed_rais_pull1990.dta", clear

gen year = 1990

* Define wid and jid
gen wid = pis
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
drop _merge
* Change in tariffs will be missing for nontradables
replace dlnonetariff_1990_1995 = 0 if missing(dlnonetariff_1990_1995)


* Merge data from a later year, say 2000, and compute changes in earnings for individual workers

rename log_rem log_rem_1990
forval year = 1991/2010{
	* 1999 missing for some reason
	if `year'!=1999{
	di "`year'"
	merge 1:1 pis using "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\ProcessedData_RAIS\RegionalEarnPremia\processed_rais_pull`year'.dta", keepusing(log_rem) keep(1 3)
	drop _merge
	rename log_rem log_rem_`year'
	gen d_log_rem_1990_`year' = log_rem_`year' - log_rem_1990
	}
}

* The independent variable here is the log difference in tariffs from 1990 to 1995 by industry as defined by subsibge. This will be zero for nontradables. 
* The dependent variable here is the log difference in wages, where wages are defined in terms of multiples of the minimum wage. This is equivalent to adjusting for inflation using the minimum wage as a price index. 


destring mmc, replace
destring cbo1994, replace 
gen occ4 = int(cbo1994/10)

save "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\trade_shock\trade_shock_regression_data.dta", replace


eststo clear	
forval year = 1991/2005{
	if `year'!=1999{
		eststo: reg d_log_rem_1990_`year' dlnonetariff_1990_1995
	}
}
esttab 

reg d_log_rem_1990_2000 dlnonetariff_1990_1995

eststo iota_reg: reg d_log_rem_1990_2000 c.dlnonetariff_1990_1995#i.iota
gen iota_sample = e(sample)
mat beta_iota = e(b)
mat beta_iota = beta_iota'
svmat beta_iota

eststo mmc_reg: reg d_log_rem_1990_2000 c.dlnonetariff_1990_1995#i.mmc if iota_sample
mat beta_mmc = e(b)
mat beta_mmc = beta_mmc'
svmat beta_mmc

eststo occ_reg: reg d_log_rem_1990_2000 c.dlnonetariff_1990_1995#i.cbo1994 if iota_sample
mat beta_occ =  e(b)
mat beta_occ = beta_occ'
svmat beta_occ

eststo occ4_reg: reg d_log_rem_1990_2000 c.dlnonetariff_1990_1995#i.occ4 if iota_sample
mat beta_occ4 =  e(b)
mat beta_occ4 = beta_occ4'
svmat beta_occ4


su beta_iota1, d
su beta_mmc1, d
su beta_occ1, d
su beta_occ41, d

twoway (histogram beta_iota1, fraction bin(20) fcolor(blue)) ///
       (histogram beta_mmc1, fraction bin(20) lcolor(red) fcolor(none)), legend(order(1 "Iota" 2 "MMC"))

/* Next steps:
 - Can I do some sort of weighting? Do we see larger variances for occupations and mmcs just because some of them are tiny which increases variance?
 - Are there outliers in earnings that should be dropped?
 - We are implicitly conditioning on being employed in both periods. Should we also try employment in period 2 as an outcome? Or include non-employment as 0 earnings (not sure how to handle this with logs)
 - Put the analysis in a loop or something and repeat it for multiple years of outcomes. 
*/
