*******************************************************
* census_currency.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates currency adjustment values for use with 
* Census data.
*
* Output: currency - variable with currency adjustments
*******************************************************

capture drop currency
gen double currency = .

replace currency = 2750000000000 if year == 1960
replace currency = 2750000000000 if year == 1970
replace currency = 2750000000000 if year == 1980
replace currency = 2750000       if year == 1991
replace currency = 1             if year == 2000
replace currency = 1             if year == 2010

