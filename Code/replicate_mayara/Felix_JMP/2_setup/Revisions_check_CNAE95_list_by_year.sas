/*
	Revisions. Want to confirm if CNAE95 is closer to original CNAE
	or to CNAE1.0 revision, as data dictionary label suggests.
	Conern is CNAE1.0 was only created in 2003, so how come it
	appears since 1995.

	For each year:
		- Compute total workers and total wage bill by CNAE95 code
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
				create table totalsCNAE&i as
				select	distinct 
						cnaeclass95 as cnae95,
						&i 					 				as year,
						count(fakeid_worker) 				as total_workers,
						sum(earningsdecmw) 					as total_payroll
					
				from monopsas.rais&i 
				group by cnae95;
			quit;

	%mend collapse;

	/*************** Execute macros ***************/
	%collapse(i=1995);
	%collapse(i=1996);
	%collapse(i=1997);
	%collapse(i=1998);
	%collapse(i=1999);
	%collapse(i=2000);

	/* Append all data */

	data totals (compress=yes);
		set totalsCNAE:;
	run;

	proc export data=totals
			outfile="/proj/patkin/projects/monopsonies/sas/cnae95_year_totals.dta"
			replace;
	run;


	proc datasets library=work kill nolist; 
	quit;