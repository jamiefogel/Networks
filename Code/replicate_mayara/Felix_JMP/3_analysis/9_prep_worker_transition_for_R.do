
clear all
set more off
set matsize 11000
ssc install unicode2ascii
unicode encoding set "ISO-8859-9"
set seed 34317154

* Gtools work on server
if c(username)=="mfelix"{
	local tool "g"
}
else{
	local tool ""
}

* Mayara mounting on server
if c(username)=="mfelix"{
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
	global monopsonies		"/proj/patkin/projects/monopsonies"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/proj_atkin/raisdeidentified"
	global monopsonies		"/Volumes/proj_atkin/projects/monopsonies"
	global public			"/Volumes/proj_atkin/publicdata"
	
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
	global monopsonies		"M:/projects/monopsonies"
	global public			"M:/publicdata"
}



local indate = 20210802
local outdate =20210802

local prep 			= 0
local adjustment 	= 1

if `prep'==1{

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/transitions/`outdate'"


****************** CNAE95 labels ******************
import excel using "${dictionaries}/cnae952d_desc_translated.xlsx",  firstrow clear
keep cnae952d cnae952d_trans
ren cnae952d_trans varlabel

duplicates drop

tempfile cnaelabels
sa `cnaelabels'
****************** Occupation labels ******************
import excel using "${dictionaries}/valid_cbo94_labels.xlsx", sheet("clean_labels") firstrow clear
keep cbo942d cbo942d_des_for_graphs
drop if inlist(cbo942d,22,23,31,24,30,37)
ren cbo942d_des_for_graphs varlabel
destring cbo942d, replace

duplicates drop

tempfile cbolabels
sa `cbolabels'

****************** MMCs labels ******************
clear
cd "${dictionaries}"
unicode translate municipality_to_microregion.dta
u "${dictionaries}/municipality_to_microregion.dta", clear
replace microregion_name = state_name +"-" +microregion_name

/* MMCs with multiple names (because of aggregation for consistency),
   assign earliest name in alphabet
*/
keep mmc microregion_name 
duplicates drop
sort mmc microregion_name
by mmc: gen n = _n
keep if n==1
keep mmc microregion_name
ren microregion_name varlabel

tempfile mmclabels
sa `mmclabels'


foreach group in mmc_cbo942d cbo942d mmc cnae952d {

local files_`group': dir "${monopsonies}/transitions/`indate'" files "transition_`group'_1990*"

	foreach f in `files_`group'' {

		insheet using "${monopsonies}/transitions/`indate'/`f'", clear
		
		* Generate all origin-dest combos, fill with zero
		* if no workers went to that destination
		preserve
			keep dest
			duplicates drop
			gen num = _n
			tempfile order
			sa `order'
		restore

		merge m:1 dest using `order', keep(3) nogen
		
		if "`group'"=="mmc_cbo942d"{
			encode origin, gen(o_)
			xtset o_ num
			
			preserve
				keep o_ origin
				gduplicates drop
				tempfile labs
				sa `labs'
			restore
			
			tsfill, full
			merge m:1 o_ using `labs', keep(3) nogen
			drop o_
		}
		else{
			xtset origin num
			tsfill, full
		}
		
		replace flow = 0 if missing(flow)
		merge m:1 num using `order', update replace nogen
		
		drop num
		ren origin origin
		ren dest dest
		
		if "`group'"=="cbo942d"{
			drop if inlist(origin,22,23,24,30,31,37) | inlist(dest,22,23,24,30,31,37)
			gen double cbo942d = origin
			merge m:1 cbo942d using `cbolabels', keep(3) nogen
			ren varlabel origin_label
			
			drop cbo942d
			gen cbo942d = dest
			merge m:1 cbo942d using `cbolabels', keep(1 3) nogen
			ren varlabel dest_label
			drop cbo942d
		}
		else if "`group'"=="mmc"{
			drop if origin==13007 | dest==13007
			gen double mmc = origin
			merge m:1 mmc using `mmclabels', keep(3) nogen
			ren varlabel origin_label
		
			drop mmc
			gen mmc = dest
			merge m:1 mmc using `mmclabels', keep(1 3) nogen
			ren varlabel dest_label
			drop mmc
		}
		else if "`group'"=="mmc_cbo942d"{
			
			gen ommc = substr(origin,1,5)
			gen dmmc = substr(dest,1,5)
			destring ommc dmmc, replace
			
			gen ocbo = substr(origin,7,2)
			gen dcbo = substr(dest,7,2)
			destring ocbo dcbo, replace
			
			drop if ommc==13007 | dmmc==13007
			drop if inlist(ocbo,22,23,24,30,31,37) | inlist(dcbo,22,23,24,30,31,37)
			
			gen double mmc = ommc
			merge m:1 mmc using `mmclabels', keep(3) nogen
			ren varlabel origin_mlabel
		
			drop mmc
			gen mmc = dmmc
			merge m:1 mmc using `mmclabels', keep(1 3) nogen
			ren varlabel dest_mlabel
			drop mmc
			
			gen double cbo942d = ocbo
			merge m:1 cbo942d using `cbolabels', keep(3) nogen
			ren varlabel origin_clabel
			
			drop cbo942d
			gen cbo942d = dcbo
			merge m:1 cbo942d using `cbolabels', keep(1 3) nogen
			ren varlabel dest_clabel
			drop cbo942d
			
			des

			gen origin_label = origin_mlabel + "-" + origin_clabel
			gen dest_label = dest_mlabel + "-" + dest_clabel
			
		}
		else if "`group'"=="cnae952d"{
			gen double cnae952d = origin
			merge m:1 cnae952d using `cnaelabels', keep(3) nogen
			ren varlabel origin_label
		
			drop cnae952d
			gen cnae952d = dest
			merge m:1 cnae952d using `cnaelabels', keep(1 3) nogen
			ren varlabel dest_label
			drop cnae952d
		}
		
		* Totals at origin and totals at destination
		bys origin: egen workers = sum(flow)
		bys origin: egen left = max(cond(missing(dest),flow,.))
		gen double stayers = workers - left
		
		gen double PCT = 100*flow/workers
		gen double PCTstay = 100*flow/stayers
		
		replace PCTstay = . if missing(dest)
		
		* Scale of boxes for heatmap: proportional to # workers at origin vs dest
		bys dest: egen double dest_size = sum(flow)
		
		drop if stayers==0
		preserve
			keep origin workers
			duplicates drop
			gsort -workers
			gen origin_order = _n
			gen dest_order = origin_order
			gen dest = origin
			tempfile oorder
			sa `oorder'
		restore
		
		merge m:1 origin using `oorder', keep(3) keepusing(origin_order) nogen
		merge m:1 dest using `oorder', keep(1 3) keepusing(dest_order) nogen
		
		unique dest_order
		bys origin: replace dest_order = `r(unique)' if missing(dest_order)
		
		* Origin % of all
		preserve
			keep origin stayers workers
			duplicates drop
			egen all = sum(workers)
			egen allstayers = sum(stayers)
			gen double opct_workers = round(100*workers/all,0.01)
			gen double opct_stayers = round(100*stayers/allstayers,0.01)
			keep origin opct*
			tempfile opct
			sa `opct'
		restore
		
		preserve
			keep dest dest_size
			duplicates drop
			egen all = sum(dest_size)
			egen leavers = sum(cond(missing(dest),dest_size,.))
			gen double dpct_workers = round(100*dest_size/all,0.01)
			gen double dpct_stayers = round(100*dest_size/(all-leavers),0.01)
			replace dpct_stayers=. if missing(dest)
			
			keep dest dpct*
			tempfile dpct
			sa `dpct'
		restore
		
		merge m:1 origin using `opct', keep(3) nogen
		merge m:1 dest using `dpct', keep(1 3) nogen
		
		sum dpct* opct*, detail
		
		tostring origin_order, gen(n)
		if "`group'"=="mmc"{
			replace n=subinstr("00"+n," ","",.) if length(n)==1
			replace n=subinstr("0"+n," ","",.) if length(n)==1
		}
		else{
		replace n=subinstr("0"+n," ","",.) if length(n)==1
		}
		replace origin_label = n+". "+origin_label
		drop n
		
		tostring dpct* opct*, replace force
		gen origin_label_PCTstay = origin_label + " (" + opct_stayers + "%)"
		gen origin_label_PCT = origin_label + " (" + opct_workers + "%)"
		
		gen dest_label_PCTstay = dest_label + " (" + dpct_stayers + "%)"
		gen dest_label_PCT = dest_label + " (" + dpct_workers + "%)"
		
		*Diagonal vs off-diagonal
		bys origin: egen diag = max(cond(origin==dest,flow,.))
		gen odiag_PCTstay = diag/stayers
		replace odiag_PCTstay = . if missing(dest)
		
		*Total on diagonal
		
		egen totdiag = sum(cond(origin==dest,flow,.))
		egen totstay = sum(cond(!missing(dest),flow,.))
		gen double diag_PCTstay = totdiag/totstay
		
		sum diag_PCT odiag_PCT, detail
		
		foreach i in 10 50{
			egen totdiag`i' = sum(cond(origin==dest & (origin_order<=`i' & dest_order<=`i'),flow,.))
			egen totstay`i' = sum(cond(origin_order<=`i' & dest_order<=`i',flow,.))
			gen double diag_PCTstaytop`i' = totdiag`i'/totstay`i'
		}
		
		drop  diag totstay* totdiag*

		replace dest_label = "Exit" if dest_label==""
		outsheet using "${monopsonies}/transitions/`outdate'/for_R_`f'", comma replace
		
		* Additional graph with # people by origin and destination
	
		if "`f'"=="transition_cbo942d_1985_1990.csv" | ///
			"`f'"=="transition_cbo942d_1990_1995.csv" | ///
			"`f'"=="transition_cbo942d_1995_2000.csv" | ///
			"`f'"=="transition_cbo942d_2000_1990.csv" | ///
			"`f'"=="transition_mmc_1985_1990.csv" | ///
			"`f'"=="transition_mmc_1990_1995.csv" | ///
			"`f'"=="transition_mmc_1995_2000.csv" | ///
			"`f'"=="transition_mmc_2000_1990.csv"{
			
		local ft = subinstr("`f'",".csv","",.)
		
		preserve
			keep origin workers origin_label
			duplicates drop
			gsort -workers
			replace workers = workers/1000
			gen double rank = _n
			gen double top10 = (_n<=10)
			count
			gen double bot10 = (_n>= `r(N)'-11)

			graph hbar workers if top10==1, ///
			over(origin_label, sort(1) desc) ///
			scheme(s1mono) ytitle("Workers (thousands)")
			graph export "${monopsonies}/transitions/`outdate'/`ft'_origin_top10.pdf", replace
			
			graph hbar workers if bot10==1, ///
			over(origin_label,   sort(1) desc) ///
			scheme(s1mono) ytitle("Workers (thousands)")
			graph export "${monopsonies}/transitions/`outdate'/`ft'_origin_bot10.pdf", replace
		restore
		
		preserve
			keep dest dest_size dest_label
			drop if missing(dest)
			duplicates drop
			gsort -dest_size
			replace dest_size = dest_size/1000
			gen double rank = _n
			gen double top10 = (_n<=10)
			count
			gen double bot10 = (_n>= `r(N)'-11)

			graph hbar dest_size if top10==1, ///
			over(dest_label, sort(1) desc) ///
			scheme(s1mono) ytitle("Workers (thousands)")
			graph export "${monopsonies}/transitions/`outdate'/`ft'_dest_top10.pdf", replace
			
			graph hbar dest_size if bot10==1, ///
			over(dest_label,   sort(1) desc) ///
			scheme(s1mono) ytitle("Workers (thousands)")
			graph export "${monopsonies}/transitions/`outdate'/`ft'_dest_bot10.pdf", replace
		restore
		}
		
	}
}
} /* Close prep boolean */

if `adjustment'==1{

	insheet using "${monopsonies}/transitions/`outdate'/for_R_transition_mmc_cbo942d_1990_1991.csv", case clear 	
	
	keep if origin_order<=50 & dest_order<=50
	encode origin, gen(origin_enc)
	encode dest, gen(dest_enc)
	
	preserve
		keep  origin_enc origin_order origin origin_label_PCT origin_label_PCTstay ///
			  diag_PCTstay diag_PCTstaytop50 diag_PCTstaytop10
		gduplicates drop
		tempfile or
		sa `or'
	restore
	
	preserve
		keep dest_enc dest_order
		gduplicates drop
		tempfile dest
		sa `dest'
	restore

	xtset dest_enc origin_enc
	tsfill, full
	
	merge m:1 origin_enc using `or', keep(3 4 5) update nogen
	merge m:1 dest_enc using `dest', keep(3 4 5) update nogen
	
	foreach var of varlist flow PCT PCTstay{
		replace `var' = 0 if missing(`var')
	}
	
	drop origin dest

	decode dest_enc, gen(dest)
	decode origin_enc, gen(origin)
	
	order origin dest
	outsheet using "${monopsonies}/transitions/`outdate'/for_R_transition_llmtop50_1990_1991.csv", comma replace
	

}
