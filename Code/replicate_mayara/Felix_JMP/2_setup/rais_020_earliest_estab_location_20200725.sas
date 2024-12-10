/*
	Assign unique municipality to each establishment
	Assign unique industry code to each firm
*/

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


%let firstyear  = 1985;
%let lastyear 	= 2015;

/************ Assign unique municipality to each establishment*************/

%macro location;
	%do i= &firstyear %to &lastyear;
		proc sql;
			create table location&i as
			select	distinct
					a.fakeid_estab, 
					b.municipality_rais	as municipality,	/* Valid muni codes */
					b.mmc,
					&i 						 as year
			from monopsas.rais&i as a
			left join monopsas.crosswalk_municipality_to_mmc as b
			on a.municipality=b.municipality_rais
			order by fakeid_estab;
		quit;
	%end;

	/* Append all years */
	data estabmiss estabhas;
		set location:;
		char_muni = put(municipality, 6.);
		last4 = substr(char_muni,3,4);
		if municipality=. then output estabmiss;
		else if last4='9999' then output estabmiss;
		else output estabhas;
		drop char_muni last4;
	run;

	proc sort data=estabhas; by fakeid_estab year; run;

	data monopsas.rais_estab_location_master;
		set estabhas;
		by fakeid_estab; 
		if first.fakeid_estab then keepobs = 1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc sql;
		create table monopsas.miss_location_estabs as
		select distinct fakeid_estab from estabmiss
		where 	fakeid_estab not in 
				(select fakeid_estab 
				 from monopsas.rais_estab_location_master);
	quit;

%mend location;

/* Execute */

%location;
/*
proc export data=monopsas.rais_estab_location_master 
	outfile="/proj/patkin/projects/monopsonies/sas/rais_estab_location_master.dta"
	dbms=dta
	replace;
run;
*/