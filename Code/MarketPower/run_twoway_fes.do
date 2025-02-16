
clear
set more off
log using "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code\MarketPower\run_twoway_fes.log", replace
use "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower_reghdfe_data.dta", clear
reghdfe y_tilde, absorb(jid_masked_fes=jid_masked iota_gamma_fes=iota_gamma_id, savefe) residuals(resid)

save "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower_reghdfe_results.dta", replace
