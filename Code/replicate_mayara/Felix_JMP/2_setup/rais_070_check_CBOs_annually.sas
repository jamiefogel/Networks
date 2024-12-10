/*
	Get annual list of CBO codes
*/

proc datasets library=work kill nolist; 
quit;

libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

/************ Define macros ************/
%macro valid(i=);
	/* Occupation related variables */
	%if (&i<1994 | &i=2002) %then %do;
		%let cboraw=cbo;
		%let cboclean=cbo94;
		%let validdata = valid_cbo94;
	%end;
	%else %if &i <2010  %then %do;
		%let cboraw=cbo94;
		%let cboclean=cbo94;
		%let validdata = valid_cbo94;
	%end;
	%else %do;
		%let cboraw=cbo02;
		%let cboclean=cbo02;
		%let validdata = crosswalk_cbo02_cbo94;
	%end;

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
			select  distinct	a.&cboraw as cboraw,
								&i as year,
								count(a.fakeid_worker) as workers,
								coalesce(c.cbo942d,99) as cbo942d
			from monopsas.rais&i as a
			left join monopsas.&validdata as c
			on a.&cboraw = c.&cboclean
			where (	a.emp1231=1 & 
					a.earningsavgmw>0 &
					a.earningsavgmw~=. & 
					a.contracttype~=&dropcont1 & a.contracttype~=&dropcont2 & a.contracttype~=&dropcont3)
			group by a.&cboraw;
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
	outfile="/proj/patkin/projects/monopsonies/sas/rais_cbo_annual_list.dta"
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
