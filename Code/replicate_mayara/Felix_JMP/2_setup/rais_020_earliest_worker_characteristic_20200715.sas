/*
	Get workers' earliest measures of
		education
		birthdate
		gender
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


%let firstyear  = 1985;
%let lastyear 	= 2015;

/************ Assign unique municipality to each establishment*************/

%macro workerloop;
	%do i= &firstyear %to &lastyear; 

		%if &i<1994 %then %do;
			proc sql;
				create table worker&i as
				select	distinct
						a.fakeid_worker,
						(a.gender =2) as female,
						a.educ, 
						&i - b.age	as birthyear,
						&i as year
				from monopsas.rais&i as a
				left join monopsas.crosswalk_agegroup_to_age as b
				on a.agegroup=b.agegroup;
			quit;
		%end;

		%else %if &i <2002 | &i >2010 %then %do;
			proc sql;
				create table worker&i as
				select	distinct
						fakeid_worker,
						(gender =2) as female,
						educ, 
						&i - age	as birthyear,
						&i as year
				from monopsas.rais&i;
			quit;
		%end;

		%else %do;
			data  workers&i ;
				set monopsas.rais&i;
				bn = put(birthdate, 8.);
				birthyear = input(substrn(bn,length(bn)-3,4),8.);
				year = &i;
				keep fakeid_worker educ female birthyear year;
			run;
		%end;
	%end; 
%mend workerloop;

/* Execute */

%workerloop;

/* Append all years */
	data genderdata;
		set worker:;
		if female=. then delete;
		keep fakeid_worker female year;
	run;

	data educdata;
		set worker:;
		if educ=. then delete;
		keep fakeid_worker educ year;
	run;

	data birthdata;
		set worker:;
		if birthyear=. then delete;
		keep fakeid_worker birthyear year;
	run;

	proc datasets library=work memtype=data nolist;
    	delete worker:;
	run; quit;

	proc sort data=genderdata; by fakeid_worker year; run;
	proc sort data=educdata; by fakeid_worker year; run;
	proc sort data=birthdata; by fakeid_worker year; run;

	data rais_worker_gender_master;
		set genderdata;
		by fakeid_worker;
		if first.fakeid_worker then keepobs=1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	data rais_worker_educ_master;
		set educdata;
		by fakeid_worker;
		if first.fakeid_worker then keepobs=1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	data rais_worker_birthyear_master;
		set birthdata;
		by fakeid_worker;
		if first.fakeid_worker then keepobs=1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc sql;
		create table monopsas.rais_worker_traits_master as
		select 	a.fakeid_worker,
				a.female,
				b.birthyear,
				c.educ
		from rais_worker_gender_master  as a
		left join rais_worker_birthyear_master  as b
		on a.fakeid_worker=b.fakeid_worker
		left join rais_worker_educ_master as c
		on a.fakeid_worker=c.fakeid_worker;
	quit;

proc datasets library=work kill nolist; 
quit;
