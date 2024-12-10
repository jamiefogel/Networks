/*
	Get annual list of mmcs
*/

proc datasets library=work kill nolist; 
quit;

libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

/************ Define macros ************/
%macro valid(i=);

	/* Contract type (keep all except civil servants and military)*/
	%if &i<1994 %then %do;
		%let dropcont1= 2;
		%let dropcont2= 8;
		%let dropcont3= 8;
	%end;
	%else %do;
		%let dropcont1= 30;
		%let dropcont2= 31;
		%let dropcont3= 35;
	%end;

		proc sql;
			create table totals&i as
			select  distinct	b.microregion as mmc,
								&i as year,
								count(a.fakeid_worker) as emp,
								mean(log(a.earningsavgmw)) as lnmearnmw,
								mean(log(a.earningsdecmw)) as lndecearnmw
			from monopsas.rais&i as a
			/*
			inner join monopsas.crosswalk_muni_to_mmc_DK17 as c
			on a.municipality=c.codemun 
			
			inner join monopsas.rais_estab_location_master as b
			on a.fakeid_estab=b.fakeid_estab
			*/
			inner join monopsas.crosswalk_municipality_to_mmc as b
			on a.municipality=b.municipality_rais 

			inner join monopsas.rais_worker_traits_master as d
			on a.fakeid_worker	= d.fakeid_worker
			where (	d.birthyear<= %eval(&i-18) & d.birthyear>= %eval(&i - 65) &
					a.municipality~=. &
					a.emp1231=1 & 
					a.earningsavgmw>0 &
					a.earningsavgmw~=. & 
					a.contracttype~=&dropcont1 & a.contracttype~=&dropcont2 & a.contracttype~=&dropcont3)
			group by b.mmc;
		quit;

%mend valid;

%macro inyears;
	%do y= 1985 %to 2000 %by 1;
		%valid(i=&y);
	%end;
%mend inyears;


/* Append all years */
%macro append;	

	data allyears (compress=yes);
		set totals:;
	run;

	proc export data=allyears
	outfile="/proj/patkin/projects/monopsonies/sas/check_rais_annual_emp_wage.dta"
	replace;
	run;

	proc datasets library=work memtype=data nolist;
    		delete allyears;
	run; quit;
%mend append;


/*************** Execute macros ***************/
%inyears;
%append;

proc datasets library=work kill nolist; 
quit;
