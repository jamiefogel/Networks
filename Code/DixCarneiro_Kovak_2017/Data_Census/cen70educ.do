* Codes years of education from the two education-related
* questions in the 1970 Census, following the coding used by
* IPUMS to harmonize the 1970 values with the "years of
* schooling" variables present in subsequent censuses

gen BR70A415 = v037
gen BR70A416 = v038

gen educ = .

* standard education levels

replace educ = 0 if BR70A415==1 & BR70A416==1

replace educ = 1 if BR70A415==2 & BR70A416==1

replace educ = 2 if BR70A415==3 & BR70A416==1

replace educ = 3 if BR70A415==4 & BR70A416==1

replace educ = 4 if BR70A415==5 & BR70A416==1
replace educ = 4 if BR70A415==6 & BR70A416==1
replace educ = 4 if BR70A415==7 & BR70A416==1

replace educ = 5 if BR70A415==2 & BR70A416==2

replace educ = 6 if BR70A415==3 & BR70A416==2

replace educ = 7 if BR70A415==4 & BR70A416==2

replace educ = 8 if BR70A415==5 & BR70A416==2
replace educ = 8 if BR70A415==6 & BR70A416==2

replace educ = 9 if BR70A415==2 & BR70A416==3

replace educ = 10 if BR70A415==3 & BR70A416==3

replace educ = 11 if BR70A415==4 & BR70A416==3
replace educ = 11 if BR70A415==7 & BR70A416==3

replace educ = 12 if BR70A415==2 & BR70A416==4

replace educ = 13 if BR70A415==3 & BR70A416==4

replace educ = 14 if BR70A415==4 & BR70A416==4

replace educ = 15 if BR70A415==5 & BR70A416==4

replace educ = 16 if BR70A415==6 & BR70A416==4

* none / adult literacy etc.

replace educ = 0 if BR70A415 == 0
replace educ = 0 if BR70A416 == 5
replace educ = 0 if BR70A415 == 8
replace educ = 0 if BR70A415 == 9
