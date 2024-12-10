/*
	20210802
		a) Do not restrict data based on birthyear
		Looks like I drop more than DK after 1994,
		when age and/or birthyear become available
		Instead, keep agegroups 3-7. Same variable
		is available for 1986-2000

		b) Do not delete the workers that were
		hired or fired in December

		c) Restriction of ibgesubsector should be based
		on data in rais (not on ibgesubsector of crosswalk)
		and should also drop missing ibgesubsector

		d) No longer drop cbo942d==31,22,37 to see if this
		gets results closer to DK (2017). 

		e) Now drop if admitted in December just like DK (2017)

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
		a) Bring back managers and firm directors
	20200725
		a) Munis ending in 9999 are missing, treat just as if IGNORADO
		b) use DK (2017) municipality code
		c) Use workers in contracts 1 and 10 only
	20200720
		a) Now that discovered issue is real pre-trends that need
		to be netted out, go back to including more data for better
		SEs - include IGNORADO files, use my definition of microregions.

	20200715
		a) Make earnings restriciton on december earnings
		not average earnings

	20200618
		a) Keep all contract types except civil servants
		b) Using new rais&year file, which excludes workers
		hired or fired in December
		c) Do not use workers from IGNORADO files, which
		have missing municipality

	Make stata datasets to be used for
	producing residualized earnings
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';


/************ Define macros ************/
%macro getdemos(i=);
	%let validdata = valid_cbo94;
	%if &i<1994 %then %do;
		%let cboraw=cbo;
		%let cboclean=cbo94;
	%end;
	%else %do;
		%let cboraw=cbo94;
		%let cboclean=cbo94;
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
			create table workers&i as	
			select	distinct
					a.fakeid_worker,
					a.fakeid_firm,
					e.cnae95,
					f.ibgesubsector,
					a.empmonths,
					a.earningsavgmw,
					a.earningsdecmw,
					a.agegroup,
					(a.gender = 2) as female,
					a.educ,
					c.mmc,
					case when k.cbo942d IS NOT NULL then k.cbo942d else 99 end	as cbo942d
			from monopsas.rais&i as a
			inner join monopsas.crosswalk_muni_to_mmc_DK17 as c
			on a.municipality=c.codemun
			inner join monopsas.rais_firm_cnae95_master_plus as e
			on a.fakeid_firm = e.fakeid_firm
			inner join ibge&i as f
			on a.fakeid_firm = f.fakeid_firm
			left join monopsas.&validdata as k
			on a.&cboraw = k.&cboclean
/*			inner join monopsas.IPEA_minwage as g
			on g.year = &i */
			where (	a.municipality IS NOT NULL &
					a.earningsdecmw>0 &
					a.earningsdecmw IS NOT NULL & 
					a.admmonth~=12 & 
					a.ibgesubsector ~=24 & a.ibgesubsector IS NOT NULL &
					a.educ IS NOT NULL & a.educ>=1 & a.educ<=11 &
					a.agegroup>=3 & agegroup<=7);
		quit;

	proc export data=workers&i
	outfile="/proj/patkin/projects/monopsonies/sas/rais_for_earnings_premia&i..dta"
	replace;
	run;

	proc datasets library=work kill nolist; 
	quit;

%mend getdemos;
	
/*************** Execute macros ***************/

%getdemos(i=1985);
%getdemos(i=1986);
%getdemos(i=1987);
%getdemos(i=1988);
%getdemos(i=1989);
%getdemos(i=1990);
%getdemos(i=1991);
%getdemos(i=1992);
%getdemos(i=1993);
%getdemos(i=1994);
%getdemos(i=1995);
%getdemos(i=1996);
%getdemos(i=1997);
%getdemos(i=1998);
%getdemos(i=1999);
%getdemos(i=2000);