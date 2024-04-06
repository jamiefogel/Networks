

* Set the number of iterations
* Note: one iteration takes about 10-12 hours to run!
* local B = 5
local B = 500

forvalues b=1/`B' {
	di _n "***********************************************"
	di _n "***** Working on bootstrap iteration `b' ******"
	di _n "***********************************************"

	do 5_est_nlfe_bootstrap "`b'"
}

di "Done"
