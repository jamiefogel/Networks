/*
	Assign unique cnae95 and ibgesubsector codes to all firms
*/

proc datasets nolist library=work kill; run; quit; 

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

%let firstyear  = 1985;
%let lastyear 	= 2015;

/************ Find unique cnae for each firm*************/

%macro cnae;
	%do i= &firstyear %to &lastyear;
		%if &i<1995 %then %do;
			proc sql;
				create table firms&i as
				select	distinct
						fakeid_firm, 
						&i 						 as year
				from monopsas.rais&i;
			quit;
		%end;
		%else %do;
			proc sql;
				create table firms&i as
				select	distinct
						fakeid_firm, 
						cnaeclass95 as cnae95,
						&i 						 as year
				from monopsas.rais&i;
			quit;
		%end;
	%end;

	data allfirms;
		set firms:;
	run;

	proc datasets library=work memtype=data nolist;
    	delete firms:;
	run; quit;

	/* Valid CNAE95 codes */
	proc sql;

		/* Earliest valid code for a firm */
		create table rais_firm_cnae95_master as
		select distinct
			   a.fakeid_firm,
			   a.year,
			   a.cnae95
		from allfirms as a
		inner join monopsas.valid_cnae95 as b
		on a.cnae95=b.cnae95
		order by a.fakeid_firm, a.year;

		/* Firms with no valid codes */
		create table monopsas.miss_cnae95_firms as
		select 	distinct 
				fakeid_firm,
				cnae95
		from allfirms
		where fakeid_firm not in 
			  (select fakeid_firm from rais_firm_cnae95_master);
	quit;

	proc datasets library=work memtype=data nolist;
    	delete allfirms;
	run; quit;

	data monopsas.rais_firm_cnae95_master;
		set rais_firm_cnae95_master;
		by fakeid_firm;
		if first.fakeid_firm then keepobs=1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete rais_firm_cnae95_master;
	run; quit;
%mend cnae;

/****** Construct ibgesubactivity-cnae and cnae20-cnae95 crosswalks *************/

/* 	Crosswalk Ibgesubactivity-cnae95 
	Based on firms that have CNAE95, looking back
	to their pre-1995 data
*/

%macro ibgecross;
	%do i= 1985 %to 1994;
		proc sql;
			create table subac&i as
			select	distinct
					fakeid_firm,
					ibgesubactivity,
					&i 						 as year
			from monopsas.rais&i;
		quit;
	%end;

	/* Append all years */
	data tempibge;
		set subac:;
		if ibgesubactivity=. then delete;
	run;

	proc datasets library=work memtype=data nolist;
    	delete subac:;
	run; quit;

	proc sort data=tempibge; by fakeid_firm year; run;

	data oneibge;
		set tempibge;
		by fakeid_firm;
		if first.fakeid_firm then keepobs =1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete tempibge;
	run; quit;

	/*  Find CNAE95s in post-1995 data for each ibgesubactivity
		in pre-1995 using firms that survive both periods
		(inner join)
	*/
	proc sql;
		create table ibgesubactivity_cnae95_crosswalk as
		select distinct
			   a.ibgesubactivity, 
			   b.cnae95, 
			   count(b.cnae95) as obs
		from oneibge as a
		inner join monopsas.rais_firm_cnae95_master as b
		on a.fakeid_firm = b.fakeid_firm
		group by ibgesubactivity, cnae95
		order by ibgesubactivity, obs desc;
	quit;

	proc datasets library=work memtype=data nolist;
    	delete oneibge;
	run; quit;

	data monopsas.crosswalk_ibgesubactivity_cnae95;
		set ibgesubactivity_cnae95_crosswalk;
		by ibgesubactivity;
		if first.ibgesubactivity then keepobs = 1;
		if keepobs~=1 then delete;
		drop obs keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete ibgesubactivity_cnae95_crosswalk;
	run; quit;
%mend ibgecross;

/*  Crosswalk cnae20-cnae95 - datasets: 2008-2015 
	except 2010, where cnae only stored in Estb file
*/

%macro cnae20cross;
	%do i= 2006 %to 2015;
	%if &i~=2010 %then %do;
		proc sql;
			create table cnae20&i as
			select	distinct
					fakeid_firm,
					cnaeclass20 as cnae20,
					cnaeclass95 as cnae95,
					&i 						 as year
			from monopsas.rais&i;
		quit;
	%end;
	%end;

	data tempcnae;
		set cnae20:;
		if cnae20=. | cnae95=. then delete;
	run;

	proc datasets library=work memtype=data nolist;
    	delete cnae20:;
	run; quit;

	proc sort data=tempcnae; by fakeid_firm year; run;

	proc sql;	
		create table cnae20_cnae95_crosswalk as
		select distinct 
				cnae20, 
				cnae95, 
				count(cnae95) as obs
		from tempcnae
		group by cnae20, cnae95
		order by cnae20, obs desc;
	quit;

	data monopsas.crosswalk_cnae20_cnae95;
		set cnae20_cnae95_crosswalk;
		by cnae20;
		if first.cnae20 then keepobs = 1;
		if keepobs~=1 then delete;
		drop obs keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete tempcnae cnae20_cnae95_crosswalk;
	run; quit;
%mend cnae20cross;


/****** Create master assignment of firm industry codes *************/
%macro assign;

	%do i= &firstyear %to 1994;
		proc sql;
			create table miss95ibge&i as
			select	distinct
					a.fakeid_firm,
					b.ibgesubactivity,
					&i 						 as year
			from monopsas.miss_cnae95_firms as a
			left join monopsas.rais&i as b
			on a.fakeid_firm = b.fakeid_firm;
		quit;
	%end;

	/* Keep earliest ibgesubactivity */
	data allmiss95ibge;
		set miss95ibge:;
		if ibgesubactivity=. then delete;
	run;

	proc datasets library=work memtype=data nolist;
    	delete miss95ibge:;
	run; quit;

	proc sort data=allmiss95ibge; by fakeid_firm year; run;
	data uniqueibge;
		set allmiss95ibge;
		by fakeid_firm;
		if first.fakeid_firm then keepobs = 1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete allmiss95ibge;
	run; quit;

	%do i= 2006 %to 2015; 
	%if &i~=2010 %then %do;
		proc sql;
			create table miss95cnae20&i as
			select	distinct
					a.fakeid_firm,
					b.cnaeclass20 as cnae20,
					&i 						 as year
			from monopsas.miss_cnae95_firms as a
			left join monopsas.rais&i as b
			on a.fakeid_firm = b.fakeid_firm;
		quit;
	%end;
	%end;

	/* Keep earliest cnae20 */
	data allmiss95cnae20;
		set miss95cnae20:;
		if cnae20=. then delete;
	run;

	proc datasets library=work memtype=data nolist;
    	delete miss95cnae20:;
	run; quit;

	proc sort data=allmiss95cnae20; by fakeid_firm year; run;
	data uniquecnae20;
		set allmiss95cnae20;
		by fakeid_firm;
		if first.fakeid_firm then keepobs = 1;
		if keepobs~=1 then delete;
		drop year keepobs;
	run;

	proc datasets library=work memtype=data nolist;
    	delete allmiss95cnae20;
	run; quit;

	/* Combine ibgesub and cnae20 in one step */
	proc sql;
		create table misslinks as
		select  a.fakeid_firm,
				b.ibgesubactivity,
				c.cnae20
		from monopsas.miss_cnae95_firms as a
		left join uniqueibge as b
		on a.fakeid_firm=b.fakeid_firm
		left join uniquecnae20 as c
		on a.fakeid_firm=c.fakeid_firm;
	quit;	

	proc datasets library=work memtype=data nolist;
    	delete unique:;
	run; quit;

	/* Assign cnae95 based on the crosswalks, giving
	   preference to the cnae95-cnae20 mapping */

	proc sql;
		create table assign_missing as
		select  distinct
				a.fakeid_firm, 
				coalesce(c.cnae95,b.cnae95) as cnae95
		from misslinks a
		left join monopsas.crosswalk_cnae20_cnae95 as b
		on a.cnae20=b.cnae20
		left join monopsas.crosswalk_ibgesubactivity_cnae95 as c
		on a.ibgesubactivity=c.ibgesubactivity;
	quit;

	proc datasets library=work memtype=data nolist;
    	delete misslinks;
	run; quit;

	data assign_missing;
		set assign_missing;
		cnae95_assigned = 1;
	run;

	data rais_firm_cnae95_master;
		set monopsas.rais_firm_cnae95_master;
		cnae95_assigned = 0;
	run;

	/* Create master dataset */
	proc sql;
	 	create table monopsas.rais_firm_cnae95_master_plus as
		select * from rais_firm_cnae95_master 
		union
		select * from assign_missing;
	quit;

	proc datasets library=work memtype=data nolist;
    	delete assign_missing rais_firm_cnae95_master;
	run; quit;

	proc sql;
		drop table monopsas.rais_firm_cnae95_master;
	quit;

%mend;

/* Execute */

*%cnae;
*%ibgecross;
*%cnae20cross;
*%assign;
/*
proc export data=monopsas.rais_firm_cnae95_master_plus
	outfile="/proj/patkin/projects/monopsonies/sas/rais_firm_cnae95_master_plus.dta"
	dbms=dta
	replace;
run;
*/