/*
	Revisions. Want to confirm if CNAE95 is closer to original CNAE
	or to CNAE1.0 revision, as data dictionary label suggests.
	Conern is CNAE1.0 was only created in 2003, so how come it
	appears since 1995.

	For each year:
		- Compute number of unique non-missing CNAE95 code by fakeid_firm
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


/************ Define macros ************/

%macro collapse(i=);

		proc sql;
			create table distinctCNAE&i as
			select	distinct 
					cnaeclass95 as cnae95,
					fakeid_firm,
					&i 					 				as year
				
			from monopsas.rais&i;

			create table countCNAE&i as
			select  count(cnae95) as unique_codes,
					fakeid_firm,
					year
			from distinctCNAE&i
			group by fakeid_firm;
		quit;

%mend collapse;

/*************** Execute macros ***************/
%collapse(i=1995);
%collapse(i=1996);
%collapse(i=1997);
%collapse(i=1998);
%collapse(i=1999);
%collapse(i=2000);

data totals;
	set countCNAE:;
run;

proc summary data=totals nway; 
				by year; 
				var unique_codes;
				output out=unique_codes  (drop=_type_ rename=(_freq_=cnae95s_per_firm))
				p10=p10 p25=p25 p50=p50 p75=p75 p95=p95 p99=p99 max=max;
run;

proc export data=unique_codes
	outfile="/proj/patkin/projects/monopsonies/sas/CNAE95s_per_firm_stats.dta"
	dbms=dta
	replace;
run;

proc datasets library=work kill nolist; 
quit;