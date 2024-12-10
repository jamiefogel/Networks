/*
	Code order
	
	Independent codes to be run first
		import_auxiliary_datasets.sas
		rais_annual_files.sas:  Note that this file only keeps workers employed as of Dec 31,
								and their highest paid occupation within the firm
		census.sas

	Dependent codes
		1. rais_cbo02_to_cbo94_crosswalk.sas

		2. rais_earliest_estab_location.sas
		2. rais_earliest_firm_cnae.sas

		3. rais_worker_flows.sas
		3. rais_firm_collapsed.sas

		4. rais_market_collapsed.sas
		
		5. rais_unique_producer_firms.sas




*/