/*
	Revisions. Compute firm-market-level baseline characteristics
	to explore heterogeneity in eta estimation by worker 
	demographics and market types
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
					(a.gender = 2) as female,
					a.educ,
					a.agegroup
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
					count(a.fakeid_worker) 					as total_workers,
					sum(a.female) 							as total_female,
					sum(case when a.educ>=7 then 1 else 0 end)	as total_hs,
					sum(case when a.educ>=9 then 1 else 0 end)	as total_college,
					sum(case when a.agegroup=3 then 1 else 0 end)	as total_agegroup3,
					sum(case when a.agegroup=4 then 1 else 0 end)	as total_agegroup4,
					sum(case when a.agegroup=5 then 1 else 0 end)	as total_agegroup5,
					sum(case when a.agegroup=6 then 1 else 0 end)	as total_agegroup6,
					sum(case when a.agegroup=7 then 1 else 0 end)	as total_agegroup7
			from valid&i as a
			group by a.fakeid_firm, a.mmc, a.&level2
			order by a.fakeid_firm, a.mmc, a.&level2;
		quit;

		proc export data=firm_&level2&i
		outfile="/proj/patkin/projects/monopsonies/sas/rais_firm_mmc_&level2._baseline_demos.dta"
		replace;
		run;
%mend collapse;

/*************** Execute macros ***************/
%valid(i=1991);
%collapse(i=1991,level2=cbo942d);

proc datasets library=work kill nolist; 
quit;
