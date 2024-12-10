/*
	Identify municipality code changes (and hence microregion)
	by seeing same establishment in different munis in different years

*/

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


/************ Assign unique municipality to each establishment*************/


%let firstyear  = 1985;
%let lastyear 	= 2015;

/************ See which firms changed municipality code, if any *************/

%macro location;
	%do i= &firstyear %to &lastyear;
		%let j = %eval(&i +1);
		proc sql;
			create table location&i._&j as
			select	distinct
					a.municipality 		as 	base_municipality,
					b.municipality  	as  out_municipality,
					&i 	as base_year,
					&j  as out_year,
					count(a.fakeid_estab) as tot_firms
			from monopsas.rais&i as a
			inner join monopsas.rais&j as b
			on a.fakeid_estab=b.fakeid_estab
			group by a.municipality, b.municipality;
			where a.municipality~=b.municipality & a.municipality~=. & b.municipality~=.;
		quit;
	%end;

	
	data all (compress=yes);
		set location: ;
	run;

	proc export data=all 
	outfile="/proj/patkin/projects/monopsonies/sas/rais_municipality_changes.dta"
	dbms=dta
	replace;
run;
%mend location;

/************ Number of observations by municipality *************/

%macro collapse;
	%do i= &firstyear %to &lastyear;
		proc sql;
			create table munis&i. as
			select	distinct
					a.municipality 		as 	municipality,
					&i 	as year,
					count(a.fakeid_worker) as tot_obs
			from monopsas.rais&i as a
			group by a.municipality;
		quit;
	%end;

	
	data all (compress=yes);
		set munis: ;
	run;

	proc export data=all 
	outfile="/proj/patkin/projects/monopsonies/sas/rais_obs_by_municipality_year.dta"
	dbms=dta
	replace;
run;
%mend collapse;

/* Execute */

*%location;
%collapse;