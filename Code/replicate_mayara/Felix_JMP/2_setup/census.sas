 /* SAS code for de-IDing RAIS data 
	
	all_workers_SP_1985_2010.dta

 */

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

/* Macros */

/******************* Import datasets *******************/

/* Auxiliary files */

proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_Files/census_1980_munic_to_mmc_1970_2010.dta"
	out=muni_to_mmc_cen80 
	dbms=dta;
run;

proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_Files/census_1991_munic_to_mmc_1970_2010.dta"
	out=muni_to_mmc_cen91 
	dbms=dta;
run;

proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_Files/census_2000_munic_to_mmc_1970_2010.dta"
	out=muni_to_mmc_cen00 
	dbms=dta;
run;

proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_Files/census_2010_munic_to_mmc_1970_2010.dta"
	out=muni_to_mmc_cen10 
	dbms=dta;
run;


/* Macro # 1 - Import census */
%macro import;
	%let years = 80 91 00 10;

	%local i next_year;
	%let i =1;
	%do %while(%scan(&years,&i) ne);
		%let next_year = %scan(&years,&i);

 		/* Import */
		proc import datafile="/proj/patkin/publicdata/other/DK (2017)/ReplicationFiles/Data_Census/cen&next_year"
			out=monopsas.cen&next_year 
			dbms=dta replace;
		run;

		/* Variable names in working directory */
		proc contents
     		data = monopsas.cen&next_year
          	noprint
          	out = vars_&next_year
            (keep = name varnum format);
		run;

	%let i = %eval(&i+1);
	%end;
%mend import;


/* Macro # 2 - Create dataset by microregion with unemployment and informality figures */

%macro descriptives;
	%let years = 80 91 00 10;

	%let activity80 = atividade;
	%let activity91 = atividade;
	%let activity00 = cnae;
	%let activity10 = cnae;

	%let form80 = prev_formemp;
	%let form91 = prev_formemp;
	%let form00 = formemp;
	%let form10 = formemp;

	%let inform80 = prev_nonformemp;
	%let inform91 = prev_nonformemp;
	%let inform00 = nonformemp;
	%let inform10 = nonformemp;

	%local j next_year;
	%let j =1;
	%do %while(%scan(&years,&j) ne);
		%let next_year = %scan(&years,&j);

 		/* Merge in mmc */
		proc sql;
			create table cen&next_year as 
			select a.*, b.mmc
			from monopsas.cen&next_year as a
			left join muni_to_mmc_cen&next_year as b
			on a.munic = b.munic;
		quit;

		/* Descritives by mmc - weight by census sampling weight */

		/* All */
		proc sql;
			create table monopsas.mmc_level_cen&next_year as
			select  mmc,
					sum(nilf*xweighti) as tot_nilf, 
					sum(unemp*xweighti) as tot_unempl,
					sum(&&form&next_year*xweighti) as tot_formal,
					sum(&&inform&next_year*xweighti) as tot_informal,
					sum((employed=1 & &&inform&next_year=1)*ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_earn_informal,
					sum((employed=1 & &&form&next_year=1)*ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as ewavg_arn_formal,
					sum((employed=1 & &&inform&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_mw_informal,
					sum((employed=1 & &&form&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as wavg_mw_formal
			from cen&next_year where age>=18 & age<=65 & age~=. & mmc~=.
			group by mmc;
		quit;

		/* Recode education groups */
		data ceneduc&next_year;
			length geduc $20.;
			set cen&next_year;
			if educ =. then delete;
			if educ =0 then geduc = "No education";
			if educ>0 & educ<5  then geduc = "Less than 5th";
			if educ>=5 & educ<11  then geduc = "Less than 11th";
			if educ=11  then geduc 					= "High school graduate";
			if educ>=12 & educ<15  then geduc 		= "Some college";
			if educ>14  then geduc 					= "College graduate";
		run;

		/* By education group */
		proc sql;
			create table monopsas.mmc_educ_level_cen&next_year as
			select  mmc, geduc,
					sum(nilf*xweighti) as tot_nilf, 
					sum(unemp*xweighti) as tot_unempl,
					sum(&&form&next_year*xweighti) as tot_formal,
					sum(&&inform&next_year*xweighti) as tot_informal,
					sum((employed=1 & &&inform&next_year=1)*ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_earn_informal,
					sum((employed=1 & &&form&next_year=1)*ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as ewavg_arn_formal,
					sum((employed=1 & &&inform&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_mw_informal,
					sum((employed=1 & &&form&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as wavg_mw_formal
			from ceneduc&next_year where age>=18 & age<=65 & age~=. & mmc~=.
			group by mmc, geduc;
		quit;

		/* By activity code */
		proc sql;
			create table monopsas.mmc_activity_level_cen&next_year as
			select  mmc, &&activity&next_year,
					sum(nilf*xweighti) as tot_nilf, 
					sum(unemp*xweighti) as tot_unempl,
					sum(&&form&next_year*xweighti) as tot_formal,
					sum(&&inform&next_year*xweighti) as tot_informal,
					sum((employed=1 & &&inform&next_year=1)*ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_earn_informal,
					sum((employed=1 & &&form&next_year=1)*ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as ewavg_arn_formal,
					sum((employed=1 & &&inform&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&inform&next_year=1)*xweighti) 
		 			as wavg_mw_informal,
					sum((employed=1 & &&form&next_year=1)*mw_ymain*xweighti)/sum((employed=1 & &&form&next_year=1)*xweighti) 
		 			as wavg_mw_formal
			from cen&next_year where age>=18 & age<=65 & age~=. & mmc~=.
			group by mmc, &&activity&next_year;
		quit;
	%let j = %eval(&j+1);
	%end;
%mend descriptives;


/* Execute macros */

*%import;
%descriptives;


