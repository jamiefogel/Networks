/*
	20200729:
	a) Use 558 microregions
	b) Use aggregated CBO942ds

	20200529:
	a)	Fixed ibgesubsector restriction, should have been "!=24" not "!=14"

	b) 	Use DK (2017) municipality - mmc mapping

	c)	Using fixed CBO942d assignments to account for phasing out of "99"
		cbo codes

	Q: 	Do people move more across occupation than industry? By how much?

	Want to compare occupation and industries of similar coarseness.
	
	1) 	Get all private sector CBO 1994 and CNAE 1995 codes
		How many of each?
		%occupcode shows unique 2373 occupations, but only 2358 are valid
		%indcode shows unique 574 unique CNAE95, but a few invalid

		Per data dictionary CBO94:
		2358 5-digit codes
		1906 4-digit
		350 3-digit
		84 2-digit
		
		Per data dictionary CNAE95:
		614 5-digit codes
		323 3-digits (grupos)
		59 2-digits (divisão)
	
	2) Compare flows of valid codes for:
	   	84 2-digit CBO94 to 59 2-digits CNAE95
	   	Using CNAE95 codes from firms whose CNAE95
	   	are known with certainty (no assignment based
	   	on raissubactivity): rais_firm_cnae95_master

*/

proc datasets library=work kill nolist; 
quit;

libname monopsas			'/proj/patkin/projects/monopsonies/sas';

%let outdate = 20200804;

data _null_;
	NewDirectory=dcreate("&outdate","/proj/patkin/projects/monopsonies/transitions/");
run;

/************ Worker flows *************/

/* Moves from and towards valid CBO and CNAE95 */

%macro flows(y=);

/* Occupation related variables */
%if &y<1994 %then %do;
	%let cboraw = cbo;
	%let cboclean = cbo94;
	%let validcbo = valid_cbo94;
%end;
%else %if &y<2010 %then %do;
	%let cboraw = cbo94;
	%let cboclean = cbo94;
	%let validcbo = valid_cbo94;
%end;
%else %do;
	%let cboraw = cbo02;
	%let cboclean = cbo02;
	%let validcbo = crosswalk_cbo02_cbo94;
%end;

/* Contract type variables */
%if &y<1994 %then %do;
	%let keepcont= 1;
%end;
%else %do;
	%let keepcont= 10;
%end;

proc sql;
	create table valid&y as
	select a.fakeid_worker,
		   a.fakeid_firm,
		   a.&cboraw						as cbo94_&y,
		   b.cnae953d_grupo 				as cnae953d_&y,
		   b.cnae952d_divisao 				as cnae952d_&y,
		   coalesce(c.cbo943d,999)			as cbo943d_&y,
		   coalesce(c.cbo942d,99) 			as cbo942d_&y,
		   d.microregion 				as mmc_&y,
		   e.cnae95  			as cnae95_&y	
	from monopsas.rais&y as a
		inner join monopsas.rais_worker_traits_master as k
			on a.fakeid_worker	= k.fakeid_worker 
		inner join monopsas.rais_estab_location_master as j
			on a.fakeid_estab=j.fakeid_estab
		inner join monopsas.crosswalk_municipality_to_mmc as d
		on j.municipality=d.municipality_rais 
		inner join monopsas.rais_firm_cnae95_master_plus as e
			on a.fakeid_firm = e.fakeid_firm
		inner join monopsas.valid_cnae95 as b
			on e.cnae95  = b.cnae95
		inner join monopsas.crosswalk_cnae95_ibgesubsector as f
			on b.cnae95 = f.cnae95
		inner join monopsas.&validcbo as c
			on a.&cboraw  = c.&cboclean
	where (	/* a.municipality~=. & */
			c.cbo942d~=24 & c.cbo942d~=30 & c.cbo942d~=23 & c.cbo942d~=22	& c.cbo942d~=31 & c.cbo942d~=37 &
			a.earningsdecmw>0 &
			a.earningsdecmw ~=. & 
			f.ibgesubsector ~=24 &
			a.contracttype= &keepcont &
			k.birthyear<= %eval(&y-18) & k.birthyear>= %eval(&y - 65) &
			k.educ ~=.) ;
quit;
%mend flows;

/* Year to year cross-firm moves */
%macro moves(o=,d=);

	proc sql;
		create table flow&o&d as
		select a.*, b.*
		from valid&o as a
		left join valid&d as b
		on a.fakeid_worker = b.fakeid_worker
		where a.fakeid_firm  ^= b.fakeid_firm;
	quit;

	%macro transmat(fromto=);
		proc sql;
			create table transition_&fromto as
			select 	&fromto._&o as origin_&fromto,
					&fromto._&d as dest_&fromto,
					count(fakeid_worker) as flow
			from flow&o&d
			group by &fromto._&o, &fromto._&d;
		quit;

		proc export data=transition_&fromto
			outfile="/proj/patkin/projects/monopsonies/transitions/&outdate./transition_&fromto._&o._&d..csv"
			dbms=csv
			replace;
		run;
	%mend transmat;

	%transmat(fromto=mmc);
	%transmat(fromto=cbo94);
	%transmat(fromto=cbo942d);
	%transmat(fromto=cnae95);
	%transmat(fromto=cnae952d);

	/* Summary stats */
	proc sql;
		create table stats&o&d as   
		select 	 count(fakeid_worker)			  as totworkers,
				 sum(mmc_&d = .)/count(fakeid_worker)			 as left_formal,
				 sum(mmc_&d ^= .)/count(fakeid_worker)			 as stayed_formal,
				 sum(mmc_&o=mmc_&d)/sum(mmc_&d ^= .) 		     as same_mmc,
				 sum(cbo94_&o=cbo94_&d)/sum(mmc_&d ^= .)     	 as same_cbo94,
				 sum(cbo943d_&o=cbo943d_&d)/sum(mmc_&d ^= .)     as same_cbo943d,
		 		 sum(cbo942d_&o=cbo942d_&d)/sum(mmc_&d ^= .)     as same_cbo942d,
		 		 sum(cnae95_&o=cnae95_&d)/sum(mmc_&d ^= .)   	 as same_cnae95,
				 sum(cnae952d_&o=cnae952d_&d)/sum(mmc_&d ^= .)   as same_cnae952d,
				 sum(cnae953d_&o=cnae953d_&d)/sum(mmc_&d ^= .)   as same_cnae953d,
				 
				 sum(cbo94_&o=cbo94_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .)   	 as same_mmc_cbo94,
				 sum(cbo943d_&o=cbo943d_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .)   as same_mmc_cbo943d,
				 sum(cbo942d_&o=cbo942d_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .)   as same_mmc_cbo942d,
				 sum(cnae95_&o=cnae95_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .) 	 as same_mmc_cnae95,
				 sum(cnae953d_&o=cnae953d_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .) as same_mmc_cnae953d,
				 sum(cnae952d_&o=cnae952d_&d & mmc_&o=mmc_&d)/sum(mmc_&d ^= .) as same_mmc_cnae952d
		from flow&o&d;
	quit;

	proc transpose data=stats&o&d out=longstats_&o&d; run;

	data longstats_&o&d;
		set longstats_&o&d;
		origin_year = &o;
		dest_year=&d;
	run;

	proc datasets library=work memtype=data nolist;
    		delete stats: flow:;
	run; quit;

%mend moves;

/**************** Execute /****************/

%flows(y=1985);
%flows(y=1986);
%flows(y=1987);
%flows(y=1988);
%flows(y=1989);
%flows(y=1990);
%flows(y=1991);
%flows(y=1992);
%flows(y=1993);
%flows(y=1994);
%flows(y=1995);
%flows(y=1996);
%flows(y=1997);
%flows(y=1998);
%flows(y=1999);
%flows(y=2000);

%moves(o=1985,d=1986);
%moves(o=1986,d=1987);
%moves(o=1987,d=1988);
%moves(o=1988,d=1989);
%moves(o=1989,d=1990);
%moves(o=1990,d=1991);
%moves(o=1991,d=1992);
%moves(o=1992,d=1993);
%moves(o=1993,d=1994);
%moves(o=1994,d=1995);
%moves(o=1995,d=1996);
%moves(o=1996,d=1997);
%moves(o=1997,d=1998);
%moves(o=1998,d=1999);
%moves(o=1999,d=2000);

data allstats;
	set longstats_:;
	rename _NAME_=flowtype COL1=flow;
run;

proc export data=allstats
	outfile="/proj/patkin/projects/monopsonies/transitions/&outdate/worker_transitions_stats.csv"
	dbms=csv
	replace;
run;