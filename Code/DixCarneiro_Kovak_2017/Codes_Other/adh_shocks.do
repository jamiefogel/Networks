******************************************************************************
* adh_shocks.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates regional trade quantity shocks, following the approach of 
* Autor, Dorn, and Hanson (2013)
*
* Output: /Data/adh_shocks.dta
******************************************************************************

cd "${root}Codes_Other"
log using adh_shocks.txt, text replace

****************************************
* Aggregate Comtrade trade flow data to the Industry level

forvalues yr = 1989/2013 {
  foreach f in imports exports {
    foreach c in world {
  
      disp ("year: `yr', flow: `f'")
      use ../Data_Other/comtrade/`c'/comtrade_`c'_`f'_`yr', clear
  
      if (`yr' <= 1996) {
        sort hs1992
        merge 1:1 hs1992 using ../Data_Other/hs1992_to_industry
        list hs1992 if _merge == 1 // should be none
      }
      if (`yr' >= 1997) {
        sort hs1996
        merge 1:1 hs1996 using ../Data_Other/hs1996_to_industry
        list hs1996 if _merge == 1 // should be none
      }
  
      keep if _merge == 3
      drop _merge
      collapse (sum) `f', by(industry)
      gen year = `yr'
      sort industry
      save comtrade_`c'_`f'_industry_`yr', replace  
	}
  }
}

foreach f in imports exports {
  foreach c in world {
    use comtrade_`c'_`f'_industry_1989, clear
    forvalues yr = 1990/2013 {
      append using comtrade_`c'_`f'_industry_`yr'
    }
    sort industry year
    merge m:1 year using ../Data_Other/cpi
    keep if _merge == 3 // drops earlier years
    drop _merge
    replace `f' = `f' / cpi2005
    keep industry year `f'
    reshape wide `f', i(industry) j(year)
    forvalues yr = 1989/2013 {
      replace `f'`yr' = 0 if `f'`yr' >= .
    }
    drop if industry >= .
    order industry
    sort industry
    save comtrade_`c'_`f'_industry, replace
  }
}

forvalues yr = 1989/2013 {
  foreach f in imports exports {
    foreach c in world {
      erase comtrade_`c'_`f'_industry_`yr'.dta
    }
  }
}

****************************************
* 1991 IndMatch employment weights

* load census data for 1991
use ../Data_Census/code_sample if year == 1991, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indmatch < . // valid indmatch code

* calculate total employment by indmatch
keep if indmatch < .
collapse (sum) employment=xweighti, by(indmatch)

* save output
sort indmatch
save ../Data/indmatch_employment_1991, replace

****************************************
* 1991 Industry employment weights

* load census data for 1991
use ../Data_Census/code_sample if year == 1991, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if industry < . // valid industry code

* calculate total employment by industry
collapse (sum) employment=xweighti, by(industry)

* save output
sort industry
save ../Data/industry_employment_1991, replace


****************************************
* Calculate Industry level shocks

foreach s in world {

	use comtrade_`s'_imports_industry, clear
	sort industry
	merge 1:1 industry using comtrade_`s'_exports_industry
	drop _merge // perfect match
	merge 1:1 industry using ../Data/industry_employment_1991
	keep if _merge == 3 // drops nontradables
	drop _merge

	forvalues yr = 1991/2013 {
		foreach f in imports exports {
			gen dpw_`f'_`yr' = (`f'`yr' - `f'1990)/employment
			replace dpw_`f'_`yr' = 0 if dpw_`f'_`yr' >= .
			gen dln_`f'_`yr' = ln(`f'`yr') - ln(`f'1990)
		}
	}
	
	sort industry
	aorder 
	order industry
	save industry_shocks_`s', replace
}
erase comtrade_world_imports_industry.dta
erase comtrade_world_exports_industry.dta


****************************************
* ADH style shocks

* combine weights and industry shocks
use ../Data/beta_industry, clear
keep mmc industry beta_t_notheta beta_nt_notheta
drop if industry == 99
sort industry
merge m:1 industry using industry_shocks_world
drop _merge // perfect match

* confirm weight coding
bysort mmc: egen test_t = sum(beta_t_notheta)
by mmc: egen test_nt = sum(beta_nt_notheta)
sum test_t // should be all 1's
sum test_nt // should be strictly < 1
drop test*

* generate elements of weighted averages
forvalues yr = 1991/2013 {
	foreach f in imports exports {
		gen element_t_`f'_`yr' = beta_t_notheta * dpw_`f'_`yr'
		gen element_nt_`f'_`yr' = beta_nt_notheta * dpw_`f'_`yr'
	}
}

* calculate weighted averages
collapse (sum) element*, by(mmc)
forvalues yr = 1991/2013 {
  foreach f in imports exports {
    rename element_t_`f'_`yr' adh_t_`f'_`yr'
    rename element_nt_`f'_`yr' adh_nt_`f'_`yr'
  }
}
sum adh_t_imports_2000 adh_t_imports_2010
sum adh_nt_imports_2000 adh_nt_imports_2010
sum adh_t_exports_2000 adh_t_exports_2010
sum adh_nt_exports_2000 adh_nt_exports_2010

*output
order mmc adh_t_imports* adh_t_exports* adh_nt_imports* adh_nt_exports*
codebook mmc
sort mmc
save ../Data/adh_shocks, replace

erase industry_shocks_world.dta

log close
cd "${root}"


