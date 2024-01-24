
use \\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Data\derived\MarketPower\df_emp_wage_ji.dta, clear

gen log_mean_wage_ji = log(mean_wage_ji)
gen log_employment_share_ji = log(employment_share_ji)

egen iota_gamma = group(iota gamma)

* If all goes well, this gives us eta_tilde = eta + 1
reghdfe log_employment_share_ji log_mean_wage_ji, absorb(iota_gamma, savefe)
rename __hdfe1__ mu_ig_hat
gen eta_tilde_hat = _b[log_mean_wage_ji]
bysort gamma: egen temp = total(mean_wage_ji^eta_tilde_hat)
gen L_ig_hat = log(temp)
drop temp

* I don't think we actually need to collapse because the relevant varaibles are already at the iota-gamma level
keep iota gamma mu_ig_hat L_ig_hat
duplicates drop
isid gamma iota


reghdfe mu_ig_hat L_ig_hat, absorb(iota)
