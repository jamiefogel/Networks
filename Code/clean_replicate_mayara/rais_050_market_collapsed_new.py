"""
This pared‐down script computes only the five final variables needed for the regression:
    year, mkt_emp, ice_dwErpTRAINS, ice_dwTRAINS, ice_dwTRAINS_Hf

It reads in the collapsed firm‐level data, computes a firm’s earnings share (earndshare)
and its square, merges with tariff change data to get the TRAINS shocks, then computes ICE shocks.
Market employment (mkt_emp) is computed by summing firm employment within each market.
"""

import pandas as pd
import numpy as np
from config import root
from spec_parser import parse_spec

# --------------------------------------------------------------------
# 1. File paths (only those needed)
# --------------------------------------------------------------------
monopsas_path =root + "/Code/clean_replicate_mayara/monopsonies/sas"
crosswalk_path =  root + "/Code/replicate_mayara/monopsonies/sas"
EXPORT_PATH    = monopsas_path   # Where to write outputs

# Input data files
FILE_RAIS_COLLAPSED_TEMPLATE = f"{monopsas_path}/rais_collapsed_firm_{{suffix}}.parquet"
FILE_CNAE95_TARIFF_CHANGES = f"{crosswalk_path}/cnae95_tariff_changes_1990_1994.parquet"
FILE_CROSSWALK_CNAE95      = f"{crosswalk_path}/crosswalk_cnae95_ibgesubsector.parquet"


# --------------------------------------------------------------------
# 2. Minimal processing function
# --------------------------------------------------------------------
def process_level2(market_vars, file_suffix, file_rais_collapsed, df_cross_cnae95, base_year=1991, export_path=EXPORT_PATH):
    """
    Minimal processing:
      - Reads firm-market-level data created by _030_040 and filters to base_year.
      - Computes firm-level earnings shares (earndshare) within each market.
      - Merges with tariff change data (to get TRAINS shocks) and computes ICE shocks:
          • ice_dwTRAINS    = sum((earndshare / sum_e) * chng19941990TRAINS)
          • ice_dwErpTRAINS = sum((earndshare / sum_e) * chng19941990ErpTRAINS)
          • ice_dwTRAINS_Hf = sum((earndshare^2 / sum_e2) * chng19941990TRAINS)
      - Computes market employment (mkt_emp) as the sum of firm employment.
      - Outputs a final dataset with only the five variables of interest.
    """
    # Load the collapsed firm-level data and ensure column names are lowercase
    df = pd.read_parquet(file_rais_collapsed)
    df.rename(columns=str.lower, inplace=True)

    # Filter to base_year and firms with positive employment
    df_base = df.query("year == @base_year and emp > 0").copy() 

    # -------------------------------
    # Compute firm-level earnings shares (df_all) for the base year (1991)
    # -------------------------------
    # Group by market-defining variables to compute total earnings and employment
    group_cols = market_vars  # market_vars should be a list of columns defining a market
    sum_cols = df_base.groupby(group_cols).agg(
        sum_decearn = ('totdecearnmw', 'sum'),
        sum_emp     = ('emp', 'sum')
    ).reset_index()
    # Merge totals back to each firm row to compute its share of earnings
    df_all = pd.merge(df_base, sum_cols, on=group_cols, how='inner')
    # Compute earnings share and its square (for later ICE calculation)
    df_all['earndshare'] = df_all['totdecearnmw'] / df_all['sum_decearn']
    df_all['earndshare2'] = df_all['earndshare'] ** 2

    # -------------------------------
    # Merge with tariff change data to compute TRAINS shocks (df_trade)
    # -------------------------------
    # We need the tariff changes for each firm based on its cnae95 code.
    # Only keep the columns needed for ICE shocks.
    cols_to_keep = ['cnpj_raiz', 'totdecearnmw', 'cnae95'] + market_vars
    # This is uniquely identified by cnae95
    df_trade = pd.read_parquet(FILE_CNAE95_TARIFF_CHANGES)
    # Merge tariff changes (only TRAINS and ErpTRAINS) with firm data
    df_trade = pd.merge(
        df_base[cols_to_keep],
        df_trade[['cnae95', 'chng19941990TRAINS', 'chng19941990ErpTRAINS']],
        on='cnae95',
        how='inner'
    )
    assert df_trade[['cnpj_raiz','cbo942d','mmc']].duplicated().sum()==0, "Duplicate Observations found in the dataset"
    # After the merge, the df_trade is uniquely identified by firm X market. It is specific to the base year (1991) and contains base year earnings and tariff changes for that firm-market

    assert df_trade['chng19941990TRAINS'].isna().sum()==0, "Missing values of trade shock found"
    # Drop rows with missing TRAINS change (we need these shocks)
    # XXJSF: this is unnecessary because there are no missing values. B/c this was created by an inner merge above
    df_trade = df_trade.dropna(subset=['chng19941990TRAINS']) 
    
    # -------------------------------
    # Merge df_all and df_trade to prepare for ICE calculation
    # -------------------------------
    # Use firm identifier and market_vars as merge keys
    # XXJSF This should also only include firms that have a trade shock (does this coincide with tradable firms?)
    # XXJSF earnings shares will not add up to 1 within markets in df_ice_input because we have done inner merges and thus only kept tradable firms. This will also be true in df_final. 
    # Ben thinks this is all fine
    merge_keys = ['cnpj_raiz'] + market_vars
    df_ice_input = pd.merge(df_all, df_trade, on=merge_keys, how='inner')

    # Compute group-level denominators: sum of earnings shares (should be 1, but if some firms drop out it might differ)
    # XXJSF: This is within-tradables shares, I think.
    group_sums = df_ice_input.groupby(market_vars).agg(
        sum_e = ('earndshare', 'sum'),
        sum_e2 = ('earndshare2', 'sum')
    ).reset_index()
    # Merge group sums back to each row in the merged dataset
    df_ice_input = pd.merge(df_ice_input, group_sums, on=market_vars, how='left')

    # -------------------------------
    # Compute ICE shocks for each market group
    # - These are tariff shocks weighted by the firm's base year payroll market share and the firm's base-year market Herfindahl
    # -------------------------------
    def compute_ice(g):
        out = {}
        # ICE computed as weighted sums over firms in the market
        out['ice_dwTRAINS'] = ((g['earndshare'] / g['sum_e']) * g['chng19941990TRAINS']).sum()
        out['ice_dwErpTRAINS'] = ((g['earndshare'] / g['sum_e']) * g['chng19941990ErpTRAINS']).sum()
        out['ice_dwTRAINS_Hf'] = ((g['earndshare2'] / g['sum_e2']) * g['chng19941990TRAINS']).sum()
        return pd.Series(out)

    #XX we want to keep sum_e sum_e2 sum_emp. These are market-level so either can merge the back on or keep them in the collapse but make sure not to sum or otherwise alter in the collapse
    df_ice_final = df_ice_input.groupby(market_vars, as_index=False).apply(compute_ice)
    
   
    '''
    # -------------------------------
    # Compute market employment (mkt_emp)
    # -------------------------------
    # Sum employment for each market (and add the year)
    df_mkt_emp = df_base.groupby(market_vars).agg(mkt_emp=('emp', 'sum')).reset_index()
    # XXBM Why is the below block necessary? JSF Because the year variable is used in Stata, although we could delete it in both places
    # Ensure that 'year' is included in the market key: if not, add it as a constant
    if 'year' not in market_vars:
        df_mkt_emp['year'] = base_year
    '''
        
        
        
        
    # 3g: shares table for all years
    df_shares = pd.merge(
        df.drop(columns='ibgesubsector'),
        df_cross_cnae95[['cnae95','ibgesubsector']],
        on='cnae95',
        how='left'
    )
    group_shares_cols = market_vars + [ 'year']
    sum_for_shares = df_shares.groupby(group_shares_cols).agg({
        'emp': 'sum',
        'totdecearnmw': 'sum'
    }).rename(columns={
        'emp': 'sum_emp',
        'totdecearnmw': 'sum_totdecearnmw'
    }).reset_index()
    df_shares = pd.merge(df_shares, sum_for_shares, on=group_shares_cols, how='inner')

    # Identify tradable vs. nontradable
    mask_trad  = (df_shares['ibgesubsector'] <= 14) | (df_shares['ibgesubsector'] == 25)
    mask_ntrad = (df_shares['ibgesubsector'] >= 13) & (df_shares['ibgesubsector'] <= 23)
    df_shares['temp']       = df_shares['emp'] * mask_trad
    df_shares['empshare']   = df_shares['emp'] / df_shares['sum_emp']
    df_shares['Tempshare']  = df_shares['emp'].where(mask_trad, 0) / df_shares['sum_emp']
    df_shares['NTempshare'] = df_shares['emp'].where(mask_ntrad, 0) / df_shares['sum_emp']
    df_shares['earndshare'] = df_shares['totdecearnmw'] / df_shares['sum_totdecearnmw']
    df_shares['Tearndshare'] = df_shares['totdecearnmw'].where(mask_trad, 0) / df_shares['sum_totdecearnmw']
    df_shares['NTearndshare'] = df_shares['totdecearnmw'].where(mask_ntrad, 0) / df_shares['sum_totdecearnmw']

        
        
    # 3j: Construct final "mktout"
    group_shares = df_shares.groupby(market_vars + ['year'], as_index=False)
    df_mktout = group_shares.agg(
        mkt_temp = ('temp','sum'),
        mkt_emp  = ('emp','sum'),
        mkt_wdbill = ('totdecearnmw','sum'),
        mkt_avgdearn = ('avgdecearn', lambda x: (df_shares.loc[x.index,'emp']*x).sum() / df_shares.loc[x.index,'emp'].sum()),
        avg_firmemp  = ('emp','mean'),
        hf_emp       = ('empshare', lambda x: (x**2).sum()),
        hf_wdbill    = ('earndshare', lambda x: (x**2).sum()),
        hf_Temp      = ('Tempshare', lambda x: (x**2).sum()),
        hf_Twdbill   = ('Tearndshare', lambda x: (x**2).sum()),
        hf_NTemp     = ('NTempshare', lambda x: (x**2).sum()),
        hf_NTwdbill  = ('NTearndshare', lambda x: (x**2).sum()),
        mkt_firms    = ('cnpj_raiz','nunique')
    )
        
    df_regsfile = pd.merge(df_mktout, df_ice_final, on=market_vars, how='left')

        
    '''
    # -------------------------------
    # Merge market employment with ICE shocks and keep only required variables
    # -------------------------------
    # Merge on market_vars (if year was not part of market_vars, it will be added from df_mkt_emp)
    # XXJSF: Some left_only but no right_only
    df_final = pd.merge(df_mkt_emp, df_ice_final, on=market_vars, how='inner')
    '''
    
    # Rearrange columns so that the final output contains exactly: year, mkt_emp, ice_dwErpTRAINS, ice_dwTRAINS, ice_dwTRAINS_Hf
    final_cols = ['year', 'mkt_emp', 'ice_dwErpTRAINS', 'ice_dwTRAINS', 'ice_dwTRAINS_Hf'] + market_vars
    df_regsfile = df_regsfile[final_cols]
   
    # Export the final dataset (Stata .dta format) to the export_path
    out_file = f"{export_path}/regsfile_{file_suffix}.dta"
    df_regsfile.to_stata(out_file, write_index=False)
   
    print(f"Finished processing. Final output saved to: {out_file}")
    # XXJSF: This output data set only contains market employment and three different market-level trade shock variables. I think something is wrong here because it doesn't even have identifier vars to do the merge

# --------------------------------------------------------------------
# 3. Main execution block
# --------------------------------------------------------------------
if __name__ == "__main__":
    # Parse specification to get market variables and file suffix
    #chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)
    market_vars = ["mmc", "cbo942d"]
    file_suffix = "mmc_cbo942d"
   
    
    # Construct file name for the collapsed data using the suffix
    file_rais_collapsed = FILE_RAIS_COLLAPSED_TEMPLATE.format(suffix=file_suffix)
    df_cross_cnae95   = pd.read_parquet(FILE_CROSSWALK_CNAE95)      

   
    # Run the minimal processing to produce the five variables needed
    process_level2(
        market_vars,
        file_suffix,
        file_rais_collapsed=file_rais_collapsed,
        df_cross_cnae95=df_cross_cnae95,
        base_year=1991,
        export_path=EXPORT_PATH
    )
   
    print("\nAll done!")
    
    # I think the output of this script is actually only used by 4_1, not by 3_1. 3_1 uses as an instrument actual changes in tariffs without having to do any weighting
    