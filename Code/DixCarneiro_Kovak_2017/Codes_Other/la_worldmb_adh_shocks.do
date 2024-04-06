******************************************************************************
* la_worldmb_adh_shocks.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates Latin American import and export shocks for the world except
* Brazil by MMC for use in ADH style analysis
*
* Output: /Data/la_worldmb_adh_shocks.dta
******************************************************************************

cd "${root}Codes_Other"
log using la_worldmb_adh_shocks.txt, text replace

****************************************
* Aggregate Comtrade trade flow data to the Industry level

local argentina_base = 1993
local argentina_base_plus1 = 1994
local chile_base = 1991
local chile_base_plus1 = 1992
local colombia_base = 1991
local colombia_base_plus1 = 1992
local paraguay_base = 1991
local paraguay_base_plus1 = 1992
local peru_base = 1994
local peru_base_plus1 = 1995
local uruguay_base = 1994
local uruguay_base_plus1 = 1995

local labase = 1994
local labaseplus1 = `labase' + 1

foreach ctry in argentina chile colombia paraguay peru uruguay {
	forvalues yr = ``ctry'_base'/2010 {
	  foreach f in imports exports {
	  
		disp ("year: `yr', flow: `f'")
		use ../Data_Other/comtrade/`ctry'/`ctry'_all_`f'_`yr', clear
	  
		sort hs1992
		merge m:1 hs1992 using ../Data_Other/hs1992_to_industry
		list hs1992 if _merge == 1 // should be none
		  
		keep if _merge == 3
		drop _merge
		collapse (sum) `f', by(partnercode industry)
		gen year = `yr'
		sort partnercode industry
		save `ctry'_all_`f'_industry_`yr', replace  
	  }
	}

	* Deflate to 2005 dollars and combine years
	foreach f in imports exports {
	  use `ctry'_all_`f'_industry_``ctry'_base', clear
	  forvalues yr = ``ctry'_base_plus1'/2010 {
		append using `ctry'_all_`f'_industry_`yr'
	  }
	  sort partnercode industry year
	  merge m:1 year using ../Data_Other/cpi
	  keep if _merge == 3 // drops earlier years
	  drop _merge
	  replace `f' = `f' / cpi2005
	  keep partnercode industry year `f'
	  reshape wide `f', i(partnercode industry) j(year)
	  forvalues yr = ``ctry'_base'/2010 {
		replace `f'`yr' = 0 if `f'`yr' >= .
	  }
	  drop if industry >= .
	  order industry
	  sort industry
	  save `ctry'_all_`f'_industry, replace
	}
	foreach f in imports exports {
	  forvalues yr = ``ctry'_base'/2010 {
		rm `ctry'_all_`f'_industry_`yr'.dta
	  }
	}

****************************************
* Further aggregate to the IndMatch level

	foreach f in imports exports {
		* merge with indmatch concordance
		use `ctry'_all_`f'_industry, clear
		sort industry
		merge m:1 industry using ../Data_Other/industry_to_indmatch
		tab industry if _merge < 3
		keep if _merge == 3
		drop _merge
		
		* aggregate to indmatch level
		reshape long `f', i(partnercode industry) j(year)
		collapse (sum) `f', by(partnercode indmatch year)
		reshape wide `f', i(partnercode indmatch) j(year)
		forvalues yr = ``ctry'_base'/2010 {
			replace `f'`yr' = 0 if `f'`yr' >= .
		}
			
		* save
		sort indmatch
		save `ctry'_all_`f'_indmatch, replace
		rm `ctry'_all_`f'_industry.dta
	}	
	
****************************************
* Calculate IndMatch level world minus Latin America trade flows
	
	use `ctry'_all_imports_indmatch, clear
	sort partnercode indmatch
	merge 1:1 partnercode indmatch using `ctry'_all_exports_indmatch
	drop _merge // perfect match
	merge m:1 indmatch using ../Data/indmatch_employment_1991
	tab indmatch if _merge < 3
	keep if _merge == 3 // drops nontradables
	drop _merge
	
	reshape wide imports* exports*, i(indmatch) j(partnercode)
	forvalues yr = `labase'/2010 {
		if("`ctry'"=="argentina"){
			foreach p in 76 152 156 170 600 604 858 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
		if("`ctry'"=="chile"){
			foreach p in 32 76 156 170 600 604 858 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
		if("`ctry'"=="colombia"){
			foreach p in 32 76 152 156 600 604 858 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
		if("`ctry'"=="paraguay"){
			foreach p in 32 76 152 156 170 604 858 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
		if("`ctry'"=="peru"){
			foreach p in 32 76 152 156 170 600 858 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
		if("`ctry'"=="uruguay"){
			foreach p in 32 76 152 156 170 600 604 {
				replace imports`yr'`p' = 0 if imports`yr'`p' >= .
				replace exports`yr'`p' = 0 if exports`yr'`p' >= .
			}
		}
	}

	forvalues yr = `labase'/2010 { // world trade minus Latin America
		if("`ctry'"=="argentina"){
			gen imports_la`yr' = imports`yr'76  + imports`yr'152 + imports`yr'170 + imports`yr'600 + imports`yr'604 + imports`yr'858 // Brazil, Chile, Colombia, Paraguay, Peru, Uruguay
			gen exports_la`yr' = exports`yr'76  + exports`yr'152 + exports`yr'170 + exports`yr'600 + exports`yr'604 + exports`yr'858 // Brazil, Chile, Colombia, Paraguay, Peru, Uruguay
		}
		if("`ctry'"=="chile"){
			gen imports_la`yr' = imports`yr'32 + imports`yr'76  + imports`yr'170 + imports`yr'600 + imports`yr'604 + imports`yr'858 // Argentina, Brazil, Colombia, Paraguay, Peru, Uruguay
			gen exports_la`yr' = exports`yr'32 + exports`yr'76  + exports`yr'170 + exports`yr'600 + exports`yr'604 + exports`yr'858 // Argentina, Brazil, Colombia, Paraguay, Peru, Uruguay
		}
		if("`ctry'"=="colombia"){
			gen imports_la`yr' = imports`yr'32 + imports`yr'76  + imports`yr'152 + imports`yr'600 + imports`yr'604 + imports`yr'858 // Argentina, Brazil, Chile, Paraguay, Peru, Uruguay
			gen exports_la`yr' = exports`yr'32 + exports`yr'76  + exports`yr'152 + exports`yr'600 + exports`yr'604 + exports`yr'858 // Argentina, Brazil, Chile, Paraguay, Peru, Uruguay
		}
		if("`ctry'"=="paraguay"){
			gen imports_la`yr' = imports`yr'32 + imports`yr'76  + imports`yr'152 + imports`yr'170 + imports`yr'604 + imports`yr'858 // Argentina, Brazil, Chile, Colombia, Peru, Uruguay
			gen exports_la`yr' = exports`yr'32 + exports`yr'76  + exports`yr'152 + exports`yr'170 + exports`yr'604 + exports`yr'858 // Argentina, Brazil, Chile, Colombia, Peru, Uruguay
		}
		if("`ctry'"=="peru"){
			gen imports_la`yr' = imports`yr'32 + imports`yr'76  + imports`yr'152 + imports`yr'170 + imports`yr'600 + imports`yr'858 // Argentina, Brazil, Chile, Colombia, Paraguay, Uruguay
			gen exports_la`yr' = exports`yr'32 + exports`yr'76  + exports`yr'152 + exports`yr'170 + exports`yr'600 + exports`yr'858 // Argentina, Brazil, Chile, Colombia, Paraguay, Uruguay
		}
		if("`ctry'"=="uruguay"){
			gen imports_la`yr' = imports`yr'32 + imports`yr'76  + imports`yr'152 + imports`yr'170 + imports`yr'600 + imports`yr'604  // Argentina, Brazil, Chile, Colombia, Paraguay, Peru
			gen exports_la`yr' = exports`yr'32 + exports`yr'76  + exports`yr'152 + exports`yr'170 + exports`yr'600 + exports`yr'604  // Argentina, Brazil, Chile, Colombia, Paraguay, Peru
		}
		gen `ctry'_imports_worldmla`yr' = imports`yr'0 - imports_la`yr'
		gen `ctry'_exports_worldmla`yr' = exports`yr'0 - exports_la`yr'
	}

	keep indmatch `ctry'_imports_worldmla* `ctry'_exports_worldmla*
	sort indmatch
	aorder 
	order indmatch
	save `ctry'_worldmla_shocks, replace
	
	rm `ctry'_all_imports_indmatch.dta
	rm `ctry'_all_exports_indmatch.dta
}


****************************************
* Aggregate trade flows across Latin American countries

use argentina_worldmla_shocks, clear
sort indmatch
foreach c in chile colombia paraguay peru uruguay {
	merge 1:1 indmatch using `c'_worldmla_shocks
	drop _merge // perfect match
}
forvalues yr = 1994/2010 {
	gen la_world_imports`yr' = argentina_imports_worldmla`yr' + ///
	                           chile_imports_worldmla`yr' + ///
							   colombia_imports_worldmla`yr' + ///
							   paraguay_imports_worldmla`yr' + ///
							   peru_imports_worldmla`yr' + ///
							   uruguay_imports_worldmla`yr'
	gen la_world_exports`yr' = argentina_exports_worldmla`yr' + ///
	                           chile_exports_worldmla`yr' + ///
							   colombia_exports_worldmla`yr' + ///
							   paraguay_exports_worldmla`yr' + ///
							   peru_exports_worldmla`yr' + ///
							   uruguay_exports_worldmla`yr'
}
keep indmatch la_world*
sort indmatch

foreach c in argentina chile colombia paraguay peru uruguay {
	rm `c'_worldmla_shocks.dta
}

****************************************
* Calculate industry-level shocks

merge m:1 indmatch using ../Data/indmatch_employment_1991
tab indmatch if _merge < 3
keep if _merge == 3 // drops nontradables
drop _merge

forvalues yr = 1995/2010 {
	foreach f in imports exports {
		gen dpw_`f'_`yr' = (la_world_`f'`yr' - la_world_`f'1994)/employment
		replace dpw_`f'_`yr' = 0 if dpw_`f'_`yr' >= .
		gen dln_`f'_`yr' = ln(la_world_`f'`yr') - ln(la_world_`f'1994)
	}
}

keep indmatch dpw* dln*
sort indmatch
aorder 
order indmatch
save la_worldmb_shocks, replace

****************************************
* Calculate region-level shocks

* merge regional weights and industry shocks
use ../Data/beta_indmatch, clear
keep mmc indmatch beta_t_notheta beta_nt_notheta
drop if indmatch == 99
sort indmatch
merge m:1 indmatch using la_worldmb_shocks
drop _merge // perfect match

* confirm weight coding
bysort mmc: egen test_t = sum(beta_t_notheta)
by mmc: egen test_nt = sum(beta_nt_notheta)
sum test_t // should be all 1's
sum test_nt // should be strictly < 1
drop test*

* generate elements of weighted averages
forvalues yr = 1995/2010 {
	foreach f in imports exports {
		gen element_dpw_t_`f'_`yr' = beta_t_notheta * dpw_`f'_`yr'
		gen element_dpw_nt_`f'_`yr' = beta_nt_notheta * dpw_`f'_`yr'
		gen element_dln_t_`f'_`yr' = beta_t_notheta * dln_`f'_`yr'
		gen element_dln_nt_`f'_`yr' = beta_nt_notheta * dln_`f'_`yr'
	}
}

* calculate weighted averages
collapse (sum) element*, by(mmc)
forvalues yr = 1995/2010 {
	foreach f in imports exports {
		rename element_dpw_t_`f'_`yr' adh_t_`f'_`yr'
		rename element_dpw_nt_`f'_`yr' adh_nt_`f'_`yr'
		rename element_dln_t_`f'_`yr' adhdln_t_`f'_`yr'
		rename element_dln_nt_`f'_`yr' adhdln_nt_`f'_`yr'
	}
}
	
* summarize results
foreach yr in 2000 2010 {
	foreach f in imports exports {
		foreach t in adh adhdln {
			foreach n in t nt {
				disp "`t'_`n'_`f'_`yr'"
				sum `t'_`n'_`f'_`yr'
			}
		}
	}
}
	
*output
keep mmc adh*
codebook mmc
sort mmc
save ../Data/la_worldmb_adh_shocks, replace

rm la_worldmb_shocks.dta



log close
cd "${root}"
