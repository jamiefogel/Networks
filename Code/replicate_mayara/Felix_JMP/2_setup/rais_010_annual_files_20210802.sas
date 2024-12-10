/*
	20210802
		a) Edited to not drop observations that were
		either hired or fired in December because DK
		does not drop those for purposes of estimating
		employment effects

		b) Do not bring in IGNORADO files
	This script appends rais state-year files into year files
	for each year. Each annual file is unique at worker level,
	keeping workers employed as of Dec 31 of the year and
	the job where the worker had highest average annual earnings.
*/

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


/******************* Import datasets *******************/


/* Macro # 1 - Import rais datasets; 
	1) Create mega annual files appending all states + IGNORADO files (no municipality information)
	2) Observations to keep:
		- Employed as of December 31
		- Unique record at fakeid_worker fakeid_firm level. Keep highest wage.
*/


%let firstyear  = 1985;
%let lastyear 	= 2000;


/* Check that there is a unique observation per worker per year */

/******************* Import datasets *******************/
%macro import_years;
	%do i= &firstyear %to &lastyear;
		filename filelist pipe "dir /proj/patkin/raisdeidentified/dta/20191213/*&i*.dta"; 

		data files&i;                                        
		 Infile filelist truncover;
		 Input filename $2000.;
		 Put filename=;
		run;

		/* Assigns filename string to macros file1 file 2, etc. */
		data _null_;
		     set files&i end=final;
		     call symput(compress('file'||_n_),trim(filename));
		     if final then call symput(trim('Total'),_n_);
		run;

		/* All files from a year */
		%do j=1 %to &Total;
			proc import datafile="&&file&j"
			     out=year&i&j
			     dbms=dta
			     replace;
			run;

			/* Keep if employed as of December 31 */
			data year&i&j;
				set year&i&j;
				if emp1231~=1 then delete;
				if educ=. then delete;
				if educ<1 | educ > 11 then delete;
				if agegroup<3 | agegroup>7 then delete;
				if agegroup=. then delete;
				if earningsdecmw=0 then delete;
				if earningsdecmw=. then delete;
			run;

			/* Drop all duplicates */
			proc sort data=year&i&j nodupkey; by _all_; run;

		%end;

		/* Append all files from a year in a single year file */
		data rais&i;
			set year&i: ;
		run;

		proc datasets library=work nolist nowarn; delete year&i:; run;

		/* Keep unique record per worker-establishment: highest paid in December */
		proc sql;
			create table monopsas.rais&i as
			select *, max(earningsdecmw) as maxearn
			from rais&i
			group by fakeid_worker;
		quit;

		data monopsas.rais&i (compress=yes);
			set monopsas.rais&i;
			if earningsdecmw~=maxearn then delete;
			drop maxearn;
		run;

		proc sort data=monopsas.rais&i nodupkey; by fakeid_worker; run;

	%end;

%mend import_years;

%import_years;