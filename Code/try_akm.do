
* This is uncleaned data from the balanced dataframe
use "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\akm_data_for_stata.dta", clear

egen jid_num = group(jid)
egen wid_num = group(wid) 

drop if missing(jid_num, wid_num)
reghdfe ln_real_hrly_wage_dec, absorb(i.wid_num i.jid_num, savefe) resid su
reghdfe ln_real_hrly_wage_dec, absorb(i.wid_num i.jid_num i.gamma_level_0, savefe) resid su


* This is cleaned using the pytwoway function
use "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\bdf_akm_data_for_stata.dta", clear
reghdfe y, absorb(i.i i.j, savefe) resid su

foreach v in _reghdfe_resid __hdfe1__ __hdfe2__ y {
	su `v'
}	

su y if e(sample)
local y_var = r(Var)
su __hdfe1__ if e(sample)
local __hdfe1___var = r(Var)
su __hdfe2__ if e(sample)
local __hdfe2___var = r(Var)
su _reghdfe_resid if e(sample)
local _reghdfe_resid_var = r(Var)
corr __hdfe1__ __hdfe2__ if e(sample)
local rho = r(rho)

di `y_var'
di `__hdfe1___var' + `__hdfe2___var' + 2 * `rho' * `__hdfe1___var'^.5 * `__hdfe2___var'^.5 + `_reghdfe_resid_var'
di "Worker Variance"
di `__hdfe1___var' 
di "Job Variance"
di `__hdfe2___var' 
di "Worker-Job Covariance"
di 2 * `rho' * `__hdfe1___var'^.5 * `__hdfe2___var'^.5 
di "Residual Variance"
di `_reghdfe_resid_var'




reghdfe y, absorb(i.i i.j i.cat_controls, savefe) resid su

su y if e(sample)
local y_var = r(Var)
su __hdfe1__ if e(sample)
local __hdfe1___var = r(Var)
su __hdfe2__ if e(sample)
local __hdfe2___var = r(Var)
su __hdfe3__ if e(sample)
local __hdfe3___var = r(Var)
su _reghdfe_resid if e(sample)
local _reghdfe_resid_var = r(Var)
corr __hdfe1__ __hdfe2__ __hdfe3__ if e(sample)
mat corrmat = r(C)

di `y_var'
di `__hdfe1___var' + `__hdfe2___var' + `__hdfe3___var' + 2 * corrmat[1,2] * `__hdfe1___var'^.5 * `__hdfe2___var'^.5  + 2 * corrmat[1,3] * `__hdfe1___var'^.5 * `__hdfe3___var'^.5  + 2 * corrmat[2,3] * `__hdfe2___var'^.5 * `__hdfe3___var'^.5 + `_reghdfe_resid_var' 
di "Worker Variance"
di `__hdfe1___var' 
di "Job Variance"
di `__hdfe2___var' 
di "Gamma Variance"
di `__hdfe3___var' 
di "Worker-Job Covariance"
di 2 * corrmat[1,2] * `__hdfe1___var'^.5 * `__hdfe2___var'^.5  
di "Worker-Gamma Covariance"
di 2 * corrmat[1,3] * `__hdfe1___var'^.5 * `__hdfe3___var'^.5  
di "Job-Gamma Covariance"
di 2 * corrmat[2,3] * `__hdfe2___var'^.5 * `__hdfe3___var'^.5 
di "Residual Variance"
di `_reghdfe_resid_var' 

/*
Worker Variance
.31347218

Job Variance
.05311939

Worker-Job Covariance
.07494352

Residual Variance
.03048261



Worker Variance
.29943667

Job Variance
.05226235

Gamma Variance
.00284424

Worker-Job Covariance
.07162213

Worker-Gamma Covariance
.01356233

Job-Gamma Covariance
.0022082

Residual Variance
.0300682
*/

