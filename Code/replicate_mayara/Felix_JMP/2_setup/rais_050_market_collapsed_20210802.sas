/*
	20210802
		a) Do not restrict data based on birthyear
		Looks like I drop more than DK after 1994,
		when age and/or birthyear become available
		Instead, keep agegroups 3-7. Same variable
		is available for 1986-2000
	20210729
		a) Fix ICE shock again
		If the intention is to only use the shares of tradable sector firms,
		then need to first keep tradables only. Then do the collapsing.
		Otherwise sum() is going to include even non-tradables stuff
	20210726
		a) Fix ICE shock - forgot to include fakid_firm
		in shocks&level2 file
		b) Add additional shock with shares squared
		c) Use mmc from DK 2017 because there is issue with TO state
	20210406
		a) Add additional variables on firm size
	20201003
		a) Add back the other occupations
	20200803
		a) Drop other management occupations: 24 and 30
	20200729
		a) Use 558 microregions rather than DK's 475 mmcs.
		Call them mmcs still.
		b) Aggregated some occupational groups
		c) Fix 1995 outcomes of firm directors (cbo942d==23)
	20200720
		a) Now that discovered issue is real pre-trends that need
		to be netted out, go back to including more data for better
		SEs - include IGNORADO files, use my definition of microregions.

	20200618
		- Input files will be those without data on IGNORADO files

	20200608
		- Added employment by 1990 percentiles as outcomes
		- Compute ICE shock with employment shares to mimick DK
		- Compute earnings shares using december earnings

	20200529
		- Removed line that computed mean(avglnmearn) at the market level
		because source file is at the firm-market level, so need to weigh
		by firm employment


	This script creates
	aggregated market-level panel datasets

	Define base year as 1990 because in March 1990 there was large
	non-tariff barrier removal

	That might be driving some of the effects, and without NTB instrument
	it is hard to net those out

	Using firm-level collapsed that does not assign workers
	across occup if occup missing, and incldues residual earnings
*/

proc datasets library=work kill nolist; 
quit;

libname dict				'/proj/patkin/raisdictionaries/harmonized';
libname deIDrais			'/proj/patkin/raisdeidentified/dta/20191213';
libname monopsas			'/proj/patkin/projects/monopsonies/sas';
libname public				'/proj/patkin/publicdata';

/************ Define macros ************/

%let baseyear = 1991;
%let baseyear_o1 = &baseyear+3;

/* Flag Exporters and importers */
%macro flag(type=);
proc sql;
	create table lib&type as
	select distinct fakeid_firm, &type
	from monopsas.importers_exporters
	where &type = 1 & (year>=&baseyear & year<=&baseyear_o1);

	create table any&type as
	select distinct fakeid_firm, &type
	from monopsas.importers_exporters
	where &type = 1;
quit;
%mend flag;


/* Append all years  */
%macro append(level2=);

	proc import datafile="/proj/patkin/projects/monopsonies/sas/rais_collapsed_firm_mmc_&level2..dta"
	     out=rais_collapsed_firm_&level2
	     dbms=dta
	     replace;
	run;

	proc sql;

		create table theta&level2 as
		select distinct a.ibgesubsector,
						sum(b.theta*c.emp)/sum(c.emp) as theta
		from monopsas.crosswalk_ibgesubsector_indmatch as a
		inner join monopsas.theta_indmatch as b
		on a.indmatch=b.indmatch
		inner join rais_collapsed_firm_&level2 as c
		on a.ibgesubsector=c.ibgesubsector
		where   (c.year=&baseyear & 
				 c.emp>0 & 
				 c.emp IS NOT NULL)
		group by a.ibgesubsector;

		create table all&level2 as
		select 	distinct
				a.fakeid_firm,
				a.ibgesubsector,
				a.mmc,
				a.&level2,
				a.totdecearnmw/sum(a.totdecearnmw) 			as earndshare,
				(a.totdecearnmw/sum(a.totdecearnmw))**2 	as earndshare2,
				a.emp/sum(a.emp)							as empshare
		from rais_collapsed_firm_&level2 as a
		inner join theta&level2 as c
		on a.ibgesubsector=c.ibgesubsector
		where   (a.year=&baseyear & 
				a.emp>0 & 
				a.emp IS NOT NULL)
		group by a.mmc, a.&level2;

		create table tradables&level2 as
		select 	a.fakeid_firm,
				a.mmc,
				a.&level2,
				a.totdecearnmw/sum(a.totdecearnmw) 			as Tearndshare,
				b.chng19941990TRAINS,
				b.chng19941990ErpTRAINS,
				b.chng19941990Kume
		from rais_collapsed_firm_&level2 as a
		inner join monopsas.cnae95_tariff_changes_1990_1994 as b
		on a.cnae95 = b.cnae95
		where   (a.year=&baseyear & 
				a.emp>0 & 
				a.emp IS NOT NULL & b.chng19941990TRAINS IS NOT NULL)
		group by a.mmc, a.&level2;

		create table beta_rf&level2 as
		select distinct a.fakeid_firm,
						a.mmc, 
						a.&level2,
						(a.earndshare/b.theta)/sum(a.earndshare/b.theta) 	as betadw_rf,
						(a.empshare/b.theta)/sum(a.empshare/b.theta) 		as beta_rf
		from all&level2 as a
		inner join theta&level2 as b
		on a.ibgesubsector=b.ibgesubsector
		group by a.mmc, a.&level2;

		create table sums&level2 as
		select distinct a.mmc,
						a.&level2,
						sum(a.empshare) 	as sum_empshare,
						sum(a.earndshare) 	as sum_earndshare,
						sum(a.earndshare2) 	as sum_earndshare2,
						sum(b.Tearndshare) 	as sum_Tearndshare
		from all&level2 as a
		inner join tradables&level2 as b
		on a.fakeid_firm = b.fakeid_firm & a.mmc=b.mmc & a.&level2 = b.&level2
		group by a.mmc, a.&level2;

		create table ice&level2 as
		select distinct 
				b.mmc, 
				b.&level2,
				sum(c.betadw_rf*b.chng19941990TRAINS)  				as ice_bdwTRAINS,
				sum(c.beta_rf*b.chng19941990TRAINS)  				as ice_bTRAINS,
				sum((a.earndshare/d.sum_earndshare)*b.chng19941990TRAINS)  			as ice_dwTRAINS,
				sum((a.empshare/d.sum_empshare)*b.chng19941990TRAINS)  				as ice_TRAINS,
				sum((a.earndshare/d.sum_earndshare)*b.chng19941990ErpTRAINS)  		as ice_dwErpTRAINS,
				sum((a.earndshare/d.sum_earndshare)*b.chng19941990Kume)  			as ice_dwKume,
				sum((a.earndshare2/d.sum_earndshare2)*b.chng19941990TRAINS)  		as ice_dwTRAINS_Hf,
				sum((b.Tearndshare/d.sum_Tearndshare)*b.chng19941990TRAINS)  		as iceT_dwTRAINS
		from all&level2 as a
		inner join tradables&level2 as b
		on a.fakeid_firm = b.fakeid_firm & a.mmc=b.mmc & a.&level2 = b.&level2
		inner join beta_rf&level2 as c
		on a.fakeid_firm = c.fakeid_firm & a.mmc=c.mmc & a.&level2 = c.&level2
		inner join sums&level2 as d
		on a.mmc=d.mmc & a.&level2 = d.&level2
		group by b.mmc, b.&level2;

		drop table theta&level2, tradables&level2, all&level2, shocks&level2;

		/* Merge exporters and importers info here */
		create table shares&level2 as
		select  distinct
				a.fakeid_firm,
				a.year,
				a.mmc,
				a.&level2,
				a.avgdecearn,
				a.ibgesubsector,
				a.emp*(a.ibgesubsector <=14 | a.ibgesubsector=25) as temp,
				a.emp,
				a.totdecearnmw,
				a.emp/sum(a.emp)																as empshare,
				a.emp*(a.ibgesubsector <=14 | a.ibgesubsector=25)/sum(a.emp)					as Tempshare,
				a.emp*(13 <=a.ibgesubsector & a.ibgesubsector <=23 )/sum(a.emp)					as NTempshare,
				a.emp*(coalesce(b.exporter,0)=0)*(a.ibgesubsector <=14 | a.ibgesubsector=25)/sum(a.emp)	as TNEXPempshare,
				a.emp*(coalesce(b.exporter,0)=1)/sum(a.emp)										as EXPempshare,
				a.totdecearnmw/sum(a.totdecearnmw)													as earndshare,
				a.totdecearnmw*(a.ibgesubsector <=14 | a.ibgesubsector=25)/sum(a.totdecearnmw)		as Tearndshare,
				a.totdecearnmw*(13 <=a.ibgesubsector & a.ibgesubsector <=23 )/sum(a.totdecearnmw)	as NTearndshare,
				a.totdecearnmw*(coalesce(b.exporter,0)=1)/sum(a.totdecearnmw)						as EXPearndshare,
				a.totdecearnmw*(coalesce(b.exporter,0)=0)*(a.ibgesubsector <=14 | a.ibgesubsector=25)/sum(a.totdecearnmw) as TNEXPearndshare,
				coalesce(b.exporter,0)															as explib,
				coalesce(d.exporter,0) 															as expany
		from rais_collapsed_firm_&level2 as a
		inner join monopsas.crosswalk_cnae95_ibgesubsector as k
		on a.cnae95 = k.cnae95
		left join libexporter as b
		on a.fakeid_firm=b.fakeid_firm
		left join anyexporter as d
		on a.fakeid_firm=d.fakeid_firm
		left join libimporter as e
		on a.fakeid_firm=e.fakeid_firm	
		left join anyimporter as g
		on a.fakeid_firm=g.fakeid_firm
		where a.emp>0 & a.emp IS NOT NULL
		group by a.mmc, a.&level2, a.year;
	quit;

		proc datasets library=work memtype=data nolist;
    		delete rais_collapsed_firm_&level2;
		run; quit;

		/*************/

		proc sql;
			create table base&level2 as
			select  distinct
					fakeid_firm, 
					mmc, 
					&level2, 
					emp
			from shares&level2
			where year=&baseyear
			order by mmc, &level2, emp;
		quit;

		proc summary data=base&level2 nway; 
					by mmc &level2; 
					var emp;
					output out=emp_pctiles_&baseyear._&level2  (drop=_type_ rename=(_freq_=firms))
					p5=p5 p10=p10 p25=p25 p50=p50 p75=p75 p90=p90 p95=p95;
		run;

		proc export data=emp_pctiles_&baseyear._&level2
			outfile="/proj/patkin/projects/monopsonies/sas/regsfile_mmc_&level2._&baseyear._emp_pctiles.dta"
			dbms=dta
			replace;
		run;

		/* Percentiles for tradables only */

		proc sql;
			create table baseT&level2 as
			select  distinct
					fakeid_firm, 
					mmc, 
					&level2, 
					emp
			from shares&level2
			where year=&baseyear & (ibgesubsector <=14 | ibgesubsector=25)
			order by mmc, &level2, emp;
		quit;

		proc summary data=baseT&level2 nway; 
					by mmc &level2; 
					var emp;
					output out=empT_pctiles_&baseyear._&level2  (drop=_type_ rename=(_freq_=firms))
					p5=p5 p10=p10 p25=p25 p50=p50 p75=p75 p90=p90 p95=p95;
		run;

		proc export data=empT_pctiles_&baseyear._&level2
			outfile="/proj/patkin/projects/monopsonies/sas/regsfile_mmc_&level2._&baseyear._empT_pctiles.dta"
			dbms=dta
			replace;
		run;

		/* Merge top bot indicators back to main data */
		proc sql;

			create table Tags&level2 as
			select distinct	
					a.fakeid_firm,
					a.mmc,
					a.&level2,
					(a.emp>=b.p90) 					as top10_&baseyear,
					(a.emp<b.p90) 					as bot90_&baseyear,
					(a.emp>=b.p95) 					as top5_&baseyear,
					(a.emp<b.p95) 					as bot95_&baseyear,
					(a.emp<=b.p25) 					as bot25_&baseyear,
					(a.emp>=b.p25 & a.emp<=b.p50) 	as mid2550_&baseyear,
					(a.emp>=b.p50 & a.emp<=b.p75) 	as mid5075_&baseyear,
					(a.emp>=b.p75)  				as top25_&baseyear,
					(a.emp>50)  					as gt50_&baseyear,
					(a.emp>100)  					as gt100_&baseyear,
					(a.emp>1000)  					as gt1000_&baseyear,
					(a.emp<=50)  					as lt50_&baseyear,
					(a.emp<=100)  					as lt100_&baseyear,
					(a.emp<=1000)  					as lt1000_&baseyear,
					(a.emp>=c.p90)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 					as top10_T_&baseyear,
					(a.emp<c.p90)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 					as bot90_T_&baseyear,
					(a.emp>=c.p95)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 					as top5_T_&baseyear,
					(a.emp<c.p95)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 					as bot95_T_&baseyear,
					(a.emp<=c.p25)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 					as bot25_T_&baseyear,
					(a.emp>=c.p25 & a.emp<=c.p50)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 	as mid2550_T_&baseyear,
					(a.emp>=c.p50 & a.emp<=c.p75)*(a.ibgesubsector <=14 | a.ibgesubsector=25) 	as mid5075_T_&baseyear,
					(a.emp>=c.p75)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  				as top25_T_&baseyear,
					(a.emp>50)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as gt50_T_&baseyear,
					(a.emp>100)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as gt100_T_&baseyear,
					(a.emp>1000)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as gt1000_T_&baseyear,
					(a.emp<=50)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as lt50_T_&baseyear,
					(a.emp<=100)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as lt100_T_&baseyear,
					(a.emp<=1000)*(a.ibgesubsector <=14 | a.ibgesubsector=25)  					as lt1000_T_&baseyear
			from shares&level2 as a
			left join emp_pctiles_&baseyear._&level2 as b
			on a.mmc=b.mmc & a.&level2=b.&level2
			left join empT_pctiles_&baseyear._&level2 as c
			on a.mmc=c.mmc & a.&level2=c.&level2
			where a.year=&baseyear;

			drop table baseT&level2, base&level2, emp_pctiles_&baseyear._&level2, empT_pctiles_&baseyear._&level2;

			/* Outcomes dataset */

			create table mktout&level2 as
			select distinct
				a.mmc,
				a.&level2,
				a.year,	
				sum(a.temp)				  			as mkt_temp,
				sum(a.emp)				  			as mkt_emp,
				sum(a.emp*a.avgdecearn)/sum(a.emp)	as mkt_avgdearn,
				sum(a.totdecearnmw)					as mkt_wdbill,
				mean(a.emp)						as avg_firmemp,
				sum( a.empshare**2 ) 			as hf_emp,
				sum( a.earndshare**2 ) 			as hf_wdbill,
				sum( a.Tempshare**2 ) 			as hf_Temp,
				sum( a.Tearndshare**2 ) 		as hf_Twdbill,
				sum( a.NTempshare**2 ) 			as hf_NTemp,
				sum( a.NTearndshare**2 ) 		as hf_NTwdbill,
				sum( a.EXPempshare**2 ) 		as hf_EXPemp,
				sum( a.EXPearndshare**2 ) 		as hf_EXPwdbill,
				sum( a.TNEXPempshare**2 ) 		as hf_TNEXPemp,
				sum( a.TNEXPearndshare**2 ) 	as hf_TNEXPwdbill,
				count(distinct a.fakeid_firm)	as mkt_firms,
				sum(a.expany)					as expany_firms,
				sum(a.explib)					as explib_firms,
				sum(case when a.expany=1 then a.emp else 0 end)						as expany_emp,
				sum(case when a.explib=1 then a.emp else 0 end)						as explib_emp,
				sum(case when b.top5_&baseyear=1 then a.emp else 0 end)				as top5_&baseyear._emp,
				sum(case when b.bot95_&baseyear=1 then a.emp else 0 end)			as bot95_&baseyear._emp,
				sum(case when b.top10_&baseyear=1 then a.emp else 0 end)			as top10_&baseyear._emp,
				sum(case when b.bot90_&baseyear=1 then a.emp else 0 end)			as bot90_&baseyear._emp,
				sum(case when b.top25_&baseyear=1 then a.emp else 0 end)			as top25_&baseyear._emp,
				sum(case when b.mid5075_&baseyear=1 then a.emp else 0 end)			as mid5075_&baseyear._emp,
				sum(case when b.mid2550_&baseyear=1 then a.emp else 0 end)			as mid2550_&baseyear._emp,
				sum(case when b.bot25_&baseyear=1 then a.emp else 0 end)			as bot25_&baseyear._emp,
				sum(case when b.gt50_&baseyear=1 then a.emp else 0 end)				as gt50_&baseyear._emp,
				sum(case when b.gt100_&baseyear=1 then a.emp else 0 end)			as gt100_&baseyear._emp,
				sum(case when b.gt1000_&baseyear=1 then a.emp else 0 end)			as gt1000_&baseyear._emp,
				sum(case when b.lt50_&baseyear=1 then a.emp else 0 end)				as lt50_&baseyear._emp,
				sum(case when b.lt100_&baseyear=1 then a.emp else 0 end)			as lt100_&baseyear._emp,
				sum(case when b.lt1000_&baseyear=1 then a.emp else 0 end)			as lt1000_&baseyear._emp,
				sum(case when b.top5_T_&baseyear=1 then a.emp else 0 end)			as top5_T_&baseyear._emp,
				sum(case when b.bot95_T_&baseyear=1 then a.emp else 0 end)			as bot95_T_&baseyear._emp,
				sum(case when b.top10_T_&baseyear=1 then a.emp else 0 end)			as top10_T_&baseyear._emp,
				sum(case when b.bot90_T_&baseyear=1 then a.emp else 0 end)			as bot90_T_&baseyear._emp,
				sum(case when b.top25_T_&baseyear=1 then a.emp else 0 end)			as top25_T_&baseyear._emp,
				sum(case when b.mid5075_T_&baseyear=1 then a.emp else 0 end)		as mid5075_T_&baseyear._emp,
				sum(case when b.mid2550_T_&baseyear=1 then a.emp else 0 end)		as mid2550_T_&baseyear._emp,
				sum(case when b.bot25_T_&baseyear=1 then a.emp else 0 end)			as bot25_T_&baseyear._emp,
				sum(case when b.gt50_T_&baseyear=1 then a.emp else 0 end)			as gt50_T_&baseyear._emp,
				sum(case when b.gt100_T_&baseyear=1 then a.emp else 0 end)			as gt100_T_&baseyear._emp,
				sum(case when b.gt1000_T_&baseyear=1 then a.emp else 0 end)			as gt1000_T_&baseyear._emp,
				sum(case when b.lt50_T_&baseyear=1 then a.emp else 0 end)			as lt50_T_&baseyear._emp,
				sum(case when b.lt100_T_&baseyear=1 then a.emp else 0 end)			as lt100_T_&baseyear._emp,
				sum(case when b.lt1000_T_&baseyear=1 then a.emp else 0 end)			as lt1000_T_&baseyear._emp

			from shares&level2 as a
			left join Tags&level2 as b
			on a.fakeid_firm = b.fakeid_firm & a.mmc=b.mmc & a.&level2=b.&level2
			group by a.mmc, a.&level2, a.year;

			drop table Tags&level2, shares&level2;

			/* Regression dataset: outcomes + shocks */

			create table regsfile_mmc_&level2 as
			select a.*, b.* 
			from mktout&level2 as a
			left join ice&level2 as b
			on a.mmc =b.mmc & a.&level2 = b.&level2;

		quit;


	proc export data=regsfile_mmc_&level2
	outfile="/proj/patkin/projects/monopsonies/sas/regsfile_mmc_&level2..dta"
	dbms=dta
	replace;
	run;

%mend append;

%flag(type=exporter);
%flag(type=importer);

%append(level2=none);
%append(level2=cbo942d);

proc datasets library=work kill nolist; 
quit;
