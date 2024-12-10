/*
	Revisions. Compute totals by sector (ibgesubactivity pre-1995, cnae starting 1995)
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


/************ Define macros ************/

	/* Export crosswalk */
	proc export data=monopsas.crosswalk_ibgesubactivity_cnae95
			outfile="/proj/patkin/projects/monopsonies/sas/crosswalk_ibgesubactivity_cnae95.dta"
			replace;
	run;

	/* Get CNAE95 for 1994 from establishment data */
		proc import datafile="/proj/patkin/raisdeidentified/dta/20191213/establishment_info_files/deID_Estb1994ID.dta"
			     out=Estabs1994
			     dbms=dta
			     replace;
		run;
		
		/* 	First get fakeid_firm unique CNAE95 from fakeid_estab level data */
		proc sql;			
			create table cnae1994 as
			select distinct fakeid_estab, cnaeclass95 as cnae95
			from Estabs1994
			where cnaeclass95 is not missing & cnaeclass95~=1 & (cnaeclass95<75116 | cnaeclass95>75302);
		quit;

		/* Tiny number of estabIDs has multiple CNAE, randomly pick 1 
			Supress notes from log file as it's one per establishment,
			log file becomes > 300MB
		*/
		proc sort data=cnae1994; by fakeid_estab; run;

		options nonotes;
		proc surveyselect data=cnae1994 
			method=srs seed=123 sampsize=1 
			out=Ucnae1994 noprint;
			strata fakeid_estab;
		run;
		options notes;

		/* Merge with worker-level data on fakeid_estab */
		proc sql;
			create table totalsCNAE1994 as
			select	distinct
					1994 as year,
					count(a.fakeid_worker) 				as total_workers,
					sum(a.earningsdecmw) 				as total_payroll,
					b.cnae95
			from monopsas.rais1994 as a
			inner join Ucnae1994 as b
			on a.fakeid_estab=b.fakeid_estab
			where 	a.ibgesubsector~=24 & a.ibgesubactivity is not missing & 
			  		a.ibgesubactivity~=9000 & 
					(a.ibgesubactivity<7011 | a.ibgesubactivity>7029)
			group by b.cnae95;

		quit;

	%macro collapse(i=);

		%if &i<1995 %then %do;
			proc sql;
				create table totalsIBGE&i as
				select	distinct 
						ibgesubactivity 					as ibgesubactivity,
						&i 					 				as year,
						count(fakeid_worker) 				as total_workers,
						sum(earningsdecmw) 					as total_payroll
					
				from monopsas.rais&i 
				where 	ibgesubsector~=24 & ibgesubactivity is not missing & 
						  ibgesubactivity~=9000 & 
						  (ibgesubactivity<7011 | ibgesubactivity>7029)
				group by ibgesubactivity;
			quit;
		%end;

		%else %do;
				proc sql;
				create table totalsCNAE&i as
				select	distinct 
						cnaeclass95 						as cnae95,
						&i 					 				as year,
						count(fakeid_worker) 				as total_workers,
						sum(earningsdecmw) 					as total_payroll
					
				from monopsas.rais&i 
				where 	cnaeclass95 is not missing & cnaeclass95~=1 &
						(cnaeclass95<75116 | cnaeclass95>75302)
				group by cnae95;
				quit;
		%end;

	%mend collapse;

	%macro inyears;
	%do y= 1985 %to 2000 %by 1;
		%collapse(i=&y);
	%end;
	%mend inyears;


	/*************** Execute macros ***************/
	%inyears;

	/* Append all data */

	data totCNAE (compress=yes);
		set totalsCNAE:;
	run;

	proc export data=totCNAE
			outfile="/proj/patkin/projects/monopsonies/sas/cnae95_1994_2000_totals.dta"
			replace;
	run;

	data totIBGE (compress=yes);
		set totalsIBGE:;
	run;

	
	proc export data=totIBGE
			outfile="/proj/patkin/projects/monopsonies/sas/ibgesubactivity_1985_1994_totals.dta"
			replace;
	run;


	proc datasets library=work kill nolist; 
	quit;