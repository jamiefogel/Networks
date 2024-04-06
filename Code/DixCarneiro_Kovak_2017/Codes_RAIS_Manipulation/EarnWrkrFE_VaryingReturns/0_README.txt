################################################################################
### This archive contains estimates of the model with variable returns       ###
### to ability. See documentation for description of the model               ###
### Data: RAIS 1986-2010                                                     ###
### Estimation sample: new_panel.dta                                         ###
################################################################################

##########################
###                    ###
### List of .do files: ###
###                    ###
##########################

These files must be run sequentially.

1_est_fe_actual_data.do
		Estimates FE using reghdfe and felsdvreg and saves the estimates,
2_est_nlfe_actual_data.do
		Estimates FE using the iterative procedure,
		allowing for variable returns to ability
3_est_fe_actual_data_cp_renormalize.do
		Renormalizes all estimates to be comparable with felsdvreg,	
4_boostrap_master_file_loop.do
		Runs bootstrap procedure, by sequentially calling...
5_est_nlfe_bootstrap.do
6_est_nlfe_bootstrap_collect.do
Calculate bootstrap s.e. and save the file
		with all the results

Final output file of interest:
RegionYearFE_all_est_withse.dta
It will be used by do file "Codes_RAIS_Regressions\3_Mechanisms_WorkerComp.do",
used to construct Table 5.