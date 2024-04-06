* extracts mmc1970 coefficients and standard errors from regression output
* in earnings premium regressions

* make a list of MMC1970 codes
keep mmc1970
drop if mmc1970 >= .
duplicates drop
sort mmc1970
mkmat mmc1970, matrix(MMC1970)

* get region premia
matrix B = e(b)
matrix Beta = B[1,1..412]'
*matrix list Beta

* get standard errors
matrix V = e(V)
matrix Var = vecdiag(V[1..412,1..412])'
matrix SE = Var 
forval i = 1/`= rowsof(Var)' {
  forval j = 1/`= colsof(Var)' { 
    mat SE[`i', `j'] = sqrt(Var[`i', `j']) 
  }
}

* save fixed effect estimates and standard errors
clear
svmat MMC1970, names(mmc1970)
rename mmc19701 mmc1970
svmat Beta, names(var)
rename var1 var
svmat SE, names(se)
rename se1 se
replace var = . if var==0 & se==0
replace se = . if var>=.
sort mmc1970
