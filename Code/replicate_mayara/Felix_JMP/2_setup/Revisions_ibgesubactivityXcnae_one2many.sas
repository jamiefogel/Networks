/*
	Create one-to-many crosswalk between ibgesubactivity and cnae
	keeping count of number of firms per pair

	Based on ibgesubactivity-cnae pairs of surviving firms
	between pre- and post- periods

	pre years: 	1985 - 1994		: append unique fakeid_firm, ibgesubactivity
	post years	1995 - 2000 	: append unique fakeid_firm, cnae95

	1994 has both codes
	
	merge pre and post on firm ID
	
	group by unique ibgesubactivity-cnae, count total number of firms in pre and post

	ibgesubactivity codes for direct public admin or military: 7011-7029
	cnae codes for direct public admin or military: 75116 - 75302

*/

proc datasets nolist library=work kill; run; quit; 

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

%let firstyear  = 1985;
%let lastyear 	= 2000;

/* 1994 has ibgesubactivity in the worker-level data and cnaeclass95 in the establishment-level data */
	proc import datafile="/proj/patkin/raisdeidentified/dta/20191213/establishment_info_files/deID_Estb1994ID.dta"
		     out=Estabs1994
		     dbms=dta
		     replace;
	run;

	proc sql;
		create table map1994 as
		select distinct
				a.ibgesubactivity,
				b.cnaeclass95 as cnae95,
				count(fakeid_worker) as workers
		from monopsas.rais1994 as a
		inner join Estabs1994 as b
		on a.fakeid_estab=b.fakeid_estab
		where a.ibgesubsector~=24 & a.ibgesubactivity is not missing & 
			  a.ibgesubactivity~=9000 & b.cnaeclass95 is not missing & b.cnaeclass95~=1 &
			  (a.ibgesubactivity<7011 | a.ibgesubactivity>7029) &
			  (b.cnaeclass95<75116 | b.cnaeclass95>75302)
		group by a.ibgesubactivity, b.cnaeclass95;

		/* To add to map using surviving firms */
		create table firms1994 as
		select distinct
				a.fakeid_firm,
				a.ibgesubactivity,
				b.cnaeclass95 as cnae95
		from monopsas.rais1994 as a
		inner join Estabs1994 as b
		on a.fakeid_firm=b.fakeid_firm
		where a.ibgesubsector~=24 & a.ibgesubactivity is not missing & 
			  a.ibgesubactivity~=9000 & b.cnaeclass95 is not missing & b.cnaeclass95~=1 &
			  (a.ibgesubactivity<7011 | a.ibgesubactivity>7029) &
			  (b.cnaeclass95<75116 | b.cnaeclass95>75302);

	quit;

	proc export data=map1994
	outfile="/proj/patkin/projects/monopsonies/sas/ibgesubactivityXcnae95_1994map.dta"
	replace;
	run;

/* Find matching using other years */
	%macro sectors;
		%do i= &firstyear %to &lastyear;
			%if &i<1995 %then %do;
				proc sql;
					create table pre&i as
					select	distinct
							fakeid_firm, 
							ibgesubactivity
					from monopsas.rais&i
					where ibgesubsector~=24 & ibgesubactivity is not missing & 
						  ibgesubactivity~=9000 & 
						  (ibgesubactivity<7011 | ibgesubactivity>7029) ;
				quit;
			%end;
			%else %do;
				proc sql;
					create table post&i as
					select	distinct
							fakeid_firm, 
							cnaeclass95 as cnae95
					from monopsas.rais&i
					where cnaeclass95 is not missing & cnaeclass95~=1 &
						  (cnaeclass95<75116 | cnaeclass95>75302);
				quit;
			%end;
		%end;
	%mend;

	/* execute */
	%sectors;

	data sectorpre;
		set firms1994 pre:;
	run;

	data sectorpost;
		set firms1994 post:;
	run;

	proc datasets library=work memtype=data nolist;
		delete pre: post:;
	run; quit;

/* Merge pre and post */

	proc sql;
		create table Usectorpre as
		select distinct fakeid_firm, ibgesubactivity
		from sectorpre;

		create table Usectorpost as
		select distinct fakeid_firm, cnae95
		from sectorpost;

		create table ibgesubactivityXcnae95 as
		select distinct
			   a.ibgesubactivity, 
			   b.cnae95, 
			   count(b.fakeid_firm) as firms
		from Usectorpre as a
		inner join Usectorpost as b
		on a.fakeid_firm = b.fakeid_firm
		group by ibgesubactivity, cnae95
		order by ibgesubactivity, firms desc;
	quit;

proc export data=ibgesubactivityXcnae95
	outfile="/proj/patkin/projects/monopsonies/sas/ibgesubactivityXcnae95.dta"
	replace;
run;

proc datasets library=work kill nolist; 
quit;