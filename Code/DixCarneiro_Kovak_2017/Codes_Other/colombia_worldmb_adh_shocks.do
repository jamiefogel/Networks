******************************************************************************
* colombia_worldmb_adh_shocks.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates Colombian import and export shocks for the world except Brazil
* by MMC for use in ADH style analysis
*
* Output: /Data/colombia_worldmb_adh_shocks.dta
******************************************************************************

cd "${root}Codes_Other"
log using colombia_worldmb_adh_shocks.txt, text replace

****************************************
* Aggregate Comtrade trade flow data to the Industry level

forvalues yr = 1991/2010 {
  foreach f in imports exports {
  
    disp ("year: `yr', flow: `f'")
    use ../Data_Other/comtrade/colombia/colombia_all_`f'_`yr', clear
  
    sort hs1992
    merge m:1 hs1992 using ../Data_Other/hs1992_to_industry
    list hs1992 if _merge == 1 // should be none
	  
    keep if _merge == 3
    drop _merge
    collapse (sum) `f', by(partnercode industry)
    gen year = `yr'
    sort partnercode industry
    save colombia_all_`f'_industry_`yr', replace  
  }
}

* Deflate to 2005 dollars and combine years
foreach f in imports exports {
  use colombia_all_`f'_industry_1991, clear
  forvalues yr = 1992/2010 {
    append using colombia_all_`f'_industry_`yr'
  }
  sort partnercode industry year
  merge m:1 year using ../Data_Other/cpi
  keep if _merge == 3 // drops earlier years
  drop _merge
  replace `f' = `f' / cpi2005
  keep partnercode industry year `f'
  reshape wide `f', i(partnercode industry) j(year)
  forvalues yr = 1991/2010 {
    replace `f'`yr' = 0 if `f'`yr' >= .
  }
  drop if industry >= .
  order industry
  sort industry
  save colombia_all_`f'_industry, replace
}
foreach f in imports exports {
  forvalues yr = 1991/2010 {
    rm colombia_all_`f'_industry_`yr'.dta
  }
}

****************************************
* Further aggregate to the IndMatch level

foreach f in imports exports {
	* merge with indmatch concordance
	use colombia_all_`f'_industry, clear
	sort industry
	merge m:1 industry using ../Data_Other/industry_to_indmatch
	tab industry if _merge < 3
	keep if _merge == 3
	drop _merge
	
	* aggregate to indmatch level
	reshape long `f', i(partnercode industry) j(year)
	collapse (sum) `f', by(partnercode indmatch year)
	reshape wide `f', i(partnercode indmatch) j(year)
	forvalues yr = 1991/2010 {
		replace `f'`yr' = 0 if `f'`yr' >= .
	}
		
	* save
	sort indmatch
	save colombia_all_`f'_indmatch, replace
}

rm colombia_all_imports_industry.dta
rm colombia_all_exports_industry.dta


****************************************
* Calculate IndMatch level shocks

local base = 1991
local baseplus1 = `base' + 1

use colombia_all_imports_indmatch, clear
sort partnercode indmatch
merge 1:1 partnercode indmatch using colombia_all_exports_indmatch
drop _merge // perfect match
keep if inlist(partnercode,0,76) // world, Brazil
merge m:1 indmatch using ../Data/indmatch_employment_1991
tab indmatch if _merge < 3
keep if _merge == 3 // drops nontradables
drop _merge


reshape wide imports* exports*, i(indmatch) j(partnercode)
forvalues yr = `base'/2010 { // world trade minus Brazil
	gen imports`yr' = imports`yr'0 - imports`yr'76
	gen exports`yr' = exports`yr'0 - exports`yr'76
}

forvalues yr = `baseplus1'/2010 {
	foreach f in imports exports {
		gen dpw_`f'_`yr' = (`f'`yr' - `f'`base')/employment
		replace dpw_`f'_`yr' = 0 if dpw_`f'_`yr' >= .
		gen dln_`f'_`yr' = ln(`f'`yr') - ln(`f'`base')
	}
}

keep indmatch dpw* dln*
sort indmatch
aorder 
order indmatch
save colombia_worldmb_shocks, replace

rm colombia_all_imports_indmatch.dta
rm colombia_all_exports_indmatch.dta


****************************************
* ADH style shocks

local base = 1991
local baseplus1 = `base' + 1

foreach p in worldmb {
	* merge regional weights and industry shocks
	use ../Data/beta_indmatch, clear
	keep mmc indmatch beta_t_notheta beta_nt_notheta
	drop if indmatch == 99
	sort indmatch
	merge m:1 indmatch using colombia_`p'_shocks
	drop _merge // perfect match

	* confirm weight coding
	bysort mmc: egen test_t = sum(beta_t_notheta)
	by mmc: egen test_nt = sum(beta_nt_notheta)
	sum test_t // should be all 1's
	sum test_nt // should be strictly < 1
	drop test*

	* generate elements of weighted averages
	forvalues yr = `baseplus1'/2010 {
		foreach f in imports exports {
			gen element_dpw_t_`f'_`yr' = beta_t_notheta * dpw_`f'_`yr'
			gen element_dpw_nt_`f'_`yr' = beta_nt_notheta * dpw_`f'_`yr'
			gen element_dln_t_`f'_`yr' = beta_t_notheta * dln_`f'_`yr'
			gen element_dln_nt_`f'_`yr' = beta_nt_notheta * dln_`f'_`yr'
		}
	}
	
	* calculate weighted averages
	collapse (sum) element*, by(mmc)
	forvalues yr = `baseplus1'/2010 {
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
	save ../Data/colombia_`p'_adh_shocks, replace
}

rm colombia_worldmb_shocks.dta

log close
cd "${root}"
