
global filepath "/Users/jfogel/Networks/Data/IBGE/Conta_da_producao_2002_2017_xls"

forval i=1/16{
	tempfile `i'
	import excel "$filepath/Tabela22.xls", sheet("Tabela22.`i'") cellrange(A5:A5) clear
	local sector = A[1]
	import excel "$filepath/Tabela22.xls", sheet("Tabela22.`i'") cellrange(A6:F22) firstrow clear
	gen sector_pt = "`sector'"
	
	rename ANO year
	rename VALORDOANOANTERIOR 			lag_nom_gdp
	rename ÍNDICEDEVOLUME 				volume_index
	rename VALORAPREÇOSDOANOANTERIOR 	gdp_last_year_price
	rename ÍNDICEDEPREÇO 				inflation_rate
	rename VALORAPREÇOCORRENTE			nominal_gdp

	
	gen price_index  = 1 in 1
	local N = _N
	replace price_index = price_index[_n-1] * inflation_rate in 2/`N'
	gen real_gdp = nominal_gdp / price_index
	gen sector_ibge = `i'-1
	save ``i''
}


forval i=1/15{
	append using ``i''
}




* For some reason the first column (after year) is just the lag of the final column. 
* Also VALORAPREÇOSDOANOANTERIOR = VALORAPREÇOCORRENTE/ÍNDICEDEPREÇO (it's real value of production)
* I don't know what the volume_index is

save "$filepath/IBGE_processed_Rio.dta", replace

sort sector_ibge year 
drop lag_nom_gdp volume_index

rename (real_gdp price_index sector_ibge) (y_s p_s s)
keep y_s p_s s year

* We aren't using aggregates since we want to use the CES aggregator instead
drop if s==0

export delimited "$filepath/sectors.csv", replace



forval i=1/33{
	qui import excel "$filepath/Tabela`i'.xls", sheet("Tabela`i'.1") cellrange(A4:A4) clear
	local region = A[1]
	di "Table `i' corresponds to `region'"
}
/*
Brazil has 26 states and one federal district. These are nested within 5 regions. 
These tables corresponds to these 32 geographies plus table 33 is the entire country.

Table 1 corresponds to Região Norte
Table 2 corresponds to Rondônia
Table 3 corresponds to Acre
Table 4 corresponds to Amazonas
Table 5 corresponds to Roraima
Table 6 corresponds to Pará
Table 7 corresponds to Amapá
Table 8 corresponds to Tocantins
Table 9 corresponds to Região Nordeste
Table 10 corresponds to Maranhão
Table 11 corresponds to Piauí
Table 12 corresponds to Ceará
Table 13 corresponds to Rio Grande do Norte
Table 14 corresponds to Paraíba
Table 15 corresponds to Pernambuco
Table 16 corresponds to Alagoas
Table 17 corresponds to Sergipe
Table 18 corresponds to Bahia
Table 19 corresponds to Região Sudeste
Table 20 corresponds to Minas Gerais
Table 21 corresponds to Espírito Santo
Table 22 corresponds to Rio de Janeiro
Table 23 corresponds to São Paulo
Table 24 corresponds to Região Sul
Table 25 corresponds to Paraná
Table 26 corresponds to Santa catarina
Table 27 corresponds to Rio Grande do Sul
Table 28 corresponds to Região Centro-Oeste
Table 29 corresponds to Mato Grosso do Sul
Table 30 corresponds to Mato Grosso
Table 31 corresponds to Goiás
Table 32 corresponds to Distrito Federal
Table 33 corresponds to Brasil
*/
