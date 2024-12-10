/*
	20210802
		a) Do not restrict data based on birthyear
		Looks like I drop more than DK after 1994,
		when age and/or birthyear become available
		Instead, keep agegroups 3-7. Same variable
		is available for 1986-2000

		b) For the purposes of employment do not drop
		if hired or fired in December, just like DK

		c) Restriction of ibgesubsector should be based
		on data in rais (not on ibgesubsector of crosswalk)
		and should also drop missing ibgesubsector

		d) No longer drop cbo942d==31,22,37 to see if this
		gets results closer to DK (2017). 
		
	20210729
		a) Should NOT have restricted contract types
		In 1994 there is flexibilization and many
		contracts go to temporary. Restricting to
		that shrinks my # of observations by a lot!

		b) Do not use the estab_location_master file
		Found some establishments in the AC1994ID file
		that get an SP address. Just use the raw data

		c) Do not restrict on missing education
		take care of that in the stata premia code
	20210726
		a) Use DK (2017) municipality/mmc codes
		because there is issue with TO mmc codes

	20201003
		a) Bring back 23, 24, 30
	20200803
		a) Drop 24 and 30 (managers and intermediate managers)
	20200729
		a) Use 558 2016-border microregions, still call it mmcs
		b) Aggregated some cbo942d that are the same to be the same
		c) exclude occupations in government and with differing reporting standards over time
	20200725
		a) Munis ending in 9999 are missing, treat just as if IGNORADO
		b) use DK (2017) municipality code
		c) Use workers in contracts 1 and 10 only
	20200720
		a) Now that discovered issue is real pre-trends that need
		to be netted out, go back to including more data for better
		SEs - include IGNORADO files, use my definition of microregions.


	20200529
		a) Should not be keeping just contract types 1 or 10
		keep all contracts except civil servants:
			before 1994:	2, 8
			after 1994:		30, 31, 35

		b) Fixes assignment of CBO94 "9" codes
		Inputs needed:
			- average residualized log earnings
			- claned validCBO94 codes

		c) Use DK (2017) municipality-mmc mapping

		d) Collapse all data except residualized log earnings
		   Bring in average residualized log earnings (or premia)
		   after collapsed firm data ready. Then can also
		   compute firm residualized wage bills.

		 20200618

		 2) Do not use IGNORADO files (aka request that
		 observation in raisYEAR file has non-missing municipality)

		 3) Use updated raisYEAR file which excludes workers hired
		 or fired in December

	This script creates a panel dataset
	aggregated at the firm-market level
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
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

	/* 	Small number of firms have estabs with diff ibgesubsector
		but shouldn't be. Assign the ibgesubsector for the largest
		establishment.
	*/	
		proc sql;
			create table temp&i as
			select  distinct
					fakeid_firm, 
					ibgesubsector,
					count(fakeid_worker) as emp
			from monopsas.rais&i
			where (	educ IS NOT NULL & educ>=1 & educ<=11 &
					municipality IS NOT NULL & 
					earningsdecmw>0 &
					earningsdecmw IS NOT NULL & 
					ibgesubsector ~=24 & ibgesubsector IS NOT NULL &
					agegroup>=3 & agegroup<=7);

			create table ibge&i as
			select *, max(emp) as maxemp
			from temp&i
			group by fakeid_firm;
		quit;

		data ibge&i;
			set ibge&i;
			if maxemp~=emp then delete;
		run;

		proc sort data=ibge&i nodupkey; by fakeid_firm; run;

		proc datasets library=work nolist nowarn; delete temp&i:; run;

		proc sql;
			create table valid&i as
			select 	distinct
					a.fakeid_worker,
					a.fakeid_firm,
					e.cnae95,
					f.ibgesubsector,	
					c.mmc,
					1 as none,
					case when c.cbo942d IS NOT NULL then c.cbo942d else 99 end	as cbo942d,
					a.earningsavgmw,
					a.earningsdecmw
			from monopsas.rais&i as a
			inner join monopsas.crosswalk_muni_to_mmc_DK17 as c
			on a.municipality=c.codemun  
			inner join monopsas.rais_firm_cnae95_master_plus as e
			on a.fakeid_firm = e.fakeid_firm
			inner join ibge&i as f
			on a.fakeid_firm = f.fakeid_firm
		/*	inner join monopsas.IPEA_minwage as g
			on &i = g.year */
			left join monopsas.&validdata as c
			on a.&cboraw = c.&cboclean
			where (	a.educ IS NOT NULL & a.educ>=1 & a.educ<=11 &
					a.municipality IS NOT NULL & 
					a.earningsdecmw>0 &
					a.earningsdecmw IS NOT NULL & 
					a.ibgesubsector ~=24 & a.ibgesubsector IS NOT NULL &
					a.agegroup>=3 & a.agegroup<=7);
		quit;

%mend valid;

%macro inyears;
	%do y= 1985 %to 2000 %by 1;
		%valid(i=&y);
	%end;
%mend inyears;

/* Firm totals by market */
%macro collapse(i=, level2=);

		proc sql;
			create table firm_&level2&i as
			select	distinct 
					a.fakeid_firm,
					a.cnae95,
					a.ibgesubsector,	
					a.mmc,
					a.&level2,
					&i 					 					as year,
					count(a.fakeid_worker) 					as emp,
					sum(a.earningsavgmw) 					as totmearnmw,	/* Total monthly earnings */
					sum(a.earningsdecmw) 					as totdecearnmw,	/* Total Dec earnings */
					mean(a.earningsavgmw)	 				as avgmearn,	/* Average monthly earnings */
					mean(a.earningsdecmw)	 				as avgdecearn	/* Average Dec earnings */
			from valid&i as a
			group by a.fakeid_firm, a.mmc, a.&level2
			order by a.fakeid_firm, a.mmc, a.&level2;
		quit;
%mend collapse;

/* Append all years */
%macro append(level2=);	

	data allyears (compress=yes);
		set firm_&level2:;
	run;

	proc datasets library=work memtype=data nolist;
    		delete firm_&level2:;
	run; quit;

	proc export data=allyears
	outfile="/proj/patkin/projects/monopsonies/sas/rais_collapsed_firm_mmc_&level2..dta"
	replace;
	run;

	proc datasets library=work memtype=data nolist;
    		delete allyears;
	run; quit;
%mend append;

%macro master(occup=);
	%do y= 1985 %to 2000 %by 1;
		%collapse(i=&y,level2=&occup);
	%end;
	
	%append(level2=&occup);
%mend master;

/*************** Execute macros ***************/
%inyears;
%master(occup=none);
%master(occup=cbo942d);

proc datasets library=work kill nolist; 
quit;
