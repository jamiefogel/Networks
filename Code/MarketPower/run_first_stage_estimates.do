
clear
set more off
log using "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code\MarketPower\run_first_stage_estimates.log", replace
use "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower_reghdfe_data3.dta", clear
reghdfe ln_ell_iota_gamma ln_wage_ces_index, absorb(iota)

clear
set obs 1
gen theta_hat = _b[ln_wage] - 1

save "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower_reghdfe_results3.dta", replace
