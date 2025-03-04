"""
This script replicates the SAS macro '%append(level2=...)' by reading in a collapsed 
firm‐level dataset, computing ICE shocks, Herfindahl shares, employment percentiles, 
size‐bin flags, and other market-level variables. It outputs final Stata (.dta) files 
that serve as inputs for subsequent estimation.

We run two variants sequentially:
  1) "original": Markets are defined as the intersection of mmc and cbo942d 
     (level2 = "cbo942d"), reading from rais_collapsed_firm_mmc_cbo942d.parquet 
     and exporting regsfile_mmc_cbo942d.dta.
  2) "gamma": Markets are defined using the gamma variable, reading from 
     rais_collapsed_firm_gamma.parquet and exporting regsfile_gamma.dta.
"""

import pandas as pd
import numpy as np
from config import root
import sys

# --------------------------------------------------------------------
# 1. File paths
# --------------------------------------------------------------------
base_path = root + "/Code/replicate_mayara"
MONOPASAS_PATH = f"{base_path}/monopsonies/sas"
EXPORT_PATH    = MONOPASAS_PATH   # Where to write .dta outputs

FILE_RAIS_COLLAPSED_MMC_CBO = f"{MONOPASAS_PATH}/rais_collapsed_firm_mmc_cbo942d.parquet"
FILE_RAIS_COLLAPSED_GAMMA   = f"{MONOPASAS_PATH}/rais_collapsed_firm_gamma.parquet"

# Input data files (Parquet assumed)
FILE_CROSSWALK_INDMATCH    = f"{MONOPASAS_PATH}/crosswalk_ibgesubsector_indmatch.parquet"
FILE_THETA_INDMATCH        = f"{MONOPASAS_PATH}/theta_indmatch.parquet"
FILE_CNAE95_TARIFF_CHANGES = f"{MONOPASAS_PATH}/cnae95_tariff_changes_1990_1994.parquet"
FILE_CROSSWALK_CNAE95      = f"{MONOPASAS_PATH}/crosswalk_cnae95_ibgesubsector.parquet"

# --------------------------------------------------------------------
# 2. Load crosswalks and tariff tables
# --------------------------------------------------------------------
df_cross_indmatch = pd.read_parquet(FILE_CROSSWALK_INDMATCH)    
df_theta_indmatch = pd.read_parquet(FILE_THETA_INDMATCH)        
df_cnae95_tariff  = pd.read_parquet(FILE_CNAE95_TARIFF_CHANGES) 
df_cross_cnae95   = pd.read_parquet(FILE_CROSSWALK_CNAE95)      

# --------------------------------------------------------------------
# 3. Main function
# --------------------------------------------------------------------


def process_level2(
    market_vars,
    file_suffix,
    file_rais_collapsed,
    base_year=1991,
    export_path=EXPORT_PATH
):
    """
    Reads the collapsed firm-level data, computes ICE shocks, Herfindahl shares, etc.
    variant = "original" or "gamma"
    level2 = "cbo942d" (for original) or "none" (for gamma)
    """
    # Load the collapsed data
    df = pd.read_parquet(file_rais_collapsed)
    df.rename(columns=str.lower, inplace=True)
    
    # Merge crosswalk_indmatch + theta_indmatch => compute weighted average theta
    tmp_cross = pd.merge(
        df_cross_indmatch[['ibgesubsector', 'indmatch']],
        df_theta_indmatch[['indmatch', 'theta']],
        on='indmatch',
        how='inner'
    )
    df_for_theta = pd.merge(
        df[['ibgesubsector', 'emp', 'year']],
        tmp_cross[['ibgesubsector', 'theta']],
        on='ibgesubsector',
        how='inner'
    )
    df_for_theta = df_for_theta.query("year == @base_year and emp > 0")
    group_t = df_for_theta.groupby('ibgesubsector', as_index=False)
    df_theta = group_t.apply(lambda g: pd.Series({
        'theta': (g['theta'] * g['emp']).sum() / g['emp'].sum()
    })).reset_index()

    # Build "all" for base_year
    df_base = df.query("year == @base_year and emp > 0").copy()
    group_cols = market_vars
    sum_cols = df_base.groupby(group_cols).agg({
        'totdecearnmw': 'sum',
        'emp': 'sum'
    }).rename(columns={
        'totdecearnmw': 'sum_decearn',
        'emp': 'sum_emp'
    }).reset_index()
    df_all = pd.merge(df_base, sum_cols, on=group_cols, how='inner')
    df_all['earndshare']  = df_all['totdecearnmw'] / df_all['sum_decearn']
    df_all['earndshare2'] = df_all['earndshare']**2
    df_all['empshare']    = df_all['emp'] / df_all['sum_emp']

    # Keep relevant columns
    df_all[['fakeid_firm','ibgesubsector', 'earndshare','earndshare2','empshare'] + market_vars].drop_duplicates()
   
    # 3c: "tradables" => merges with cnae95_tariff
    cols_to_keep = ['fakeid_firm', 'totdecearnmw','cnae95'] + market_vars
    group_key = market_vars

    df_trad = pd.merge(
        df_base[cols_to_keep],
        df_cnae95_tariff[['cnae95','chng19941990TRAINS','chng19941990ErpTRAINS','chng19941990Kume']],
        on='cnae95',
        how='inner'
    )
    df_trad = df_trad.dropna(subset=['chng19941990TRAINS'])
    sum_trad = df_trad.groupby(group_key)['totdecearnmw'].sum().reset_index(name='sum_decearn')
    df_trad  = pd.merge(df_trad, sum_trad, on=group_key, how='inner')
    df_trad['Tearndshare'] = df_trad['totdecearnmw'] / df_trad['sum_decearn']

    df_trad = df_trad[['fakeid_firm', 'Tearndshare','chng19941990TRAINS','chng19941990ErpTRAINS','chng19941990Kume'] + market_vars] 
   
    # 3d: Merge with df_theta => compute betadw_rf, beta_rf
    df_beta = pd.merge(df_all, df_theta[['ibgesubsector','theta']], on='ibgesubsector', how='inner')
    def compute_betas(g):
        denom_dw = (g['earndshare'] / g['theta']).sum()
        denom_rf = (g['empshare']   / g['theta']).sum()
        g['betadw_rf'] = (g['earndshare'] / g['theta']) / denom_dw
        g['beta_rf']   = (g['empshare']   / g['theta']) / denom_rf
        return g
    group_beta = market_vars
    df_beta = df_beta.groupby(group_beta, as_index=False).apply(compute_betas)

    # 3e: sums
    merge_keys = ['fakeid_firm'] + market_vars
    group_sum  = market_vars
   
    df_sums_merged = pd.merge(df_all, df_trad, on=merge_keys, how='inner')
    df_sums = df_sums_merged.groupby(group_sum, as_index=False).agg({
        'empshare': 'sum',
        'earndshare': 'sum',
        'earndshare2': 'sum',
        'Tearndshare': 'sum'
    }).rename(columns={
        'empshare': 'sum_empshare',
        'earndshare': 'sum_earndshare',
        'earndshare2': 'sum_earndshare2',
        'Tearndshare': 'sum_Tearndshare'
    })

    # 3f: ICE
    df_ice_1 = pd.merge(df_all, df_trad, on=merge_keys, how='inner')
    df_ice_2 = pd.merge(
        df_ice_1,
        df_beta[['fakeid_firm'] + group_beta + ['betadw_rf','beta_rf']],
        on=['fakeid_firm'] + group_beta,
        how='inner'
    )
    df_ice = pd.merge(df_ice_2, df_sums, on=group_sum, how='inner')
    def compute_ice(g):
        c_train = g['chng19941990TRAINS']
        c_erp   = g['chng19941990ErpTRAINS']
        c_kume  = g['chng19941990Kume']
        earnd   = g['earndshare']
        earnd2  = g['earndshare2']
        tearnd  = g['Tearndshare']
        sum_e   = g['sum_earndshare'].iloc[0]
        sum_e2  = g['sum_earndshare2'].iloc[0]
        sum_t   = g['sum_Tearndshare'].iloc[0]
        sum_emp = g['sum_empshare'].iloc[0]
        bdw_rf  = g['betadw_rf']
        b_rf    = g['beta_rf']
        out = {}
        out['ice_bdwTRAINS']   = (bdw_rf * c_train).sum()
        out['ice_bTRAINS']     = (b_rf   * c_train).sum()
        out['ice_dwTRAINS']    = ((earnd / sum_e)     * c_train).sum()
        out['ice_TRAINS']      = ((g['empshare'] / sum_emp) * c_train).sum()
        out['ice_dwErpTRAINS'] = ((earnd / sum_e)     * c_erp).sum()
        out['ice_dwKume']      = ((earnd / sum_e)     * c_kume).sum()
        out['ice_dwTRAINS_Hf'] = ((earnd2 / sum_e2)   * c_train).sum()
        out['iceT_dwTRAINS']   = ((tearnd / sum_t)    * c_train).sum()
        return pd.Series(out)

    group_ice = market_vars
    df_ice_final = df_ice.groupby(group_ice, as_index=False).apply(compute_ice)

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

    # 3h: percentile cutoffs
    base_cols = ['fakeid_firm', 'emp','ibgesubsector'] + market_vars
    group_keys = market_vars
    out_prefix = f"regsfile_{file_suffix}"

    df_basey = df_shares.loc[df_shares['year'] == base_year, base_cols].copy()
    df_q = (
        df_basey
        .groupby(group_keys)["emp"]
        .agg(
            p5  = lambda x: x.quantile(0.05),
            p10 = lambda x: x.quantile(0.10),
            p25 = lambda x: x.quantile(0.25),
            p50 = lambda x: x.quantile(0.50),
            p75 = lambda x: x.quantile(0.75),
            p90 = lambda x: x.quantile(0.90),
            p95 = lambda x: x.quantile(0.95),
            firms = "size"
        )
        .reset_index()
    )
    out_qfile = f"{export_path}/{out_prefix}_{base_year}_emp_pctiles.dta"
    df_q.to_stata(out_qfile, write_index=False)

    # Tradables only
    basey_trad_mask = (df_basey['ibgesubsector'] <= 14) | (df_basey['ibgesubsector'] == 25)
    df_basey_trad = df_basey[basey_trad_mask].copy()
    df_qt = (
        df_basey_trad
        .groupby(group_keys)["emp"]
        .agg(
            p5  = lambda x: x.quantile(0.05),
            p10 = lambda x: x.quantile(0.10),
            p25 = lambda x: x.quantile(0.25),
            p50 = lambda x: x.quantile(0.50),
            p75 = lambda x: x.quantile(0.75),
            p90 = lambda x: x.quantile(0.90),
            p95 = lambda x: x.quantile(0.95),
            firms = "size"
        )
        .reset_index()
    )
    out_qtfile = f"{export_path}/{out_prefix}_{base_year}_empT_pctiles.dta"
    df_qt.to_stata(out_qtfile, write_index=False)

    # 3i: merge percentile tags back to define flags
    df_tag = pd.merge(df_basey, df_q, on=group_keys, how='left')
    df_tag = pd.merge(df_tag, df_qt, on=group_keys, how='left', suffixes=('', '_trad'))
    tag_trad_mask = (df_tag['ibgesubsector'] <= 14) | (df_tag['ibgesubsector'] == 25)

    # Create flags
    by = str(base_year)
    df_tag[f'top10_{by}']    = (df_tag['emp'] >= df_tag['p90']).astype(int)
    df_tag[f'bot90_{by}']    = (df_tag['emp']  < df_tag['p90']).astype(int)
    df_tag[f'top5_{by}']     = (df_tag['emp'] >= df_tag['p95']).astype(int)
    df_tag[f'bot95_{by}']    = (df_tag['emp']  < df_tag['p95']).astype(int)
    df_tag[f'bot25_{by}']    = (df_tag['emp'] <= df_tag['p25']).astype(int)
    df_tag[f'mid2550_{by}']  = ((df_tag['emp'] >= df_tag['p25']) & (df_tag['emp'] <= df_tag['p50'])).astype(int)
    df_tag[f'mid5075_{by}']  = ((df_tag['emp'] >= df_tag['p50']) & (df_tag['emp'] <= df_tag['p75'])).astype(int)
    df_tag[f'top25_{by}']    = (df_tag['emp'] >= df_tag['p75']).astype(int)
    df_tag[f'gt50_{by}']     = (df_tag['emp'] > 50).astype(int)
    df_tag[f'gt100_{by}']    = (df_tag['emp'] > 100).astype(int)
    df_tag[f'gt1000_{by}']   = (df_tag['emp'] > 1000).astype(int)
    df_tag[f'lt50_{by}']     = (df_tag['emp'] <= 50).astype(int)
    df_tag[f'lt100_{by}']    = (df_tag['emp'] <= 100).astype(int)
    df_tag[f'lt1000_{by}']   = (df_tag['emp'] <= 1000).astype(int)
    df_tag[f'top10_T_{by}']  = ((df_tag['emp'] >= df_tag['p90_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot90_T_{by}']  = ((df_tag['emp']  < df_tag['p90_trad']) & tag_trad_mask).astype(int)
    df_tag[f'top5_T_{by}']   = ((df_tag['emp'] >= df_tag['p95_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot95_T_{by}']  = ((df_tag['emp']  < df_tag['p95_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot25_T_{by}']  = ((df_tag['emp'] <= df_tag['p25_trad']) & tag_trad_mask).astype(int)
    df_tag[f'mid2550_T_{by}'] = ((df_tag['emp'] >= df_tag['p25_trad']) & (df_tag['emp'] <= df_tag['p50_trad']) & tag_trad_mask).astype(int)
    df_tag[f'mid5075_T_{by}'] = ((df_tag['emp'] >= df_tag['p50_trad']) & (df_tag['emp'] <= df_tag['p75_trad']) & tag_trad_mask).astype(int)
    df_tag[f'top25_T_{by}']   = ((df_tag['emp'] >= df_tag['p75_trad']) & tag_trad_mask).astype(int)
    df_tag[f'gt50_T_{by}']    = ((df_tag['emp'] > 50) & tag_trad_mask).astype(int)
    df_tag[f'gt100_T_{by}']   = ((df_tag['emp'] > 100) & tag_trad_mask).astype(int)
    df_tag[f'gt1000_T_{by}']  = ((df_tag['emp'] > 1000) & tag_trad_mask).astype(int)
    df_tag[f'lt50_T_{by}']    = ((df_tag['emp'] <= 50) & tag_trad_mask).astype(int)
    df_tag[f'lt100_T_{by}']   = ((df_tag['emp'] <= 100) & tag_trad_mask).astype(int)
    df_tag[f'lt1000_T_{by}']  = ((df_tag['emp'] <= 1000) & tag_trad_mask).astype(int)

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
        mkt_firms    = ('fakeid_firm','nunique')
    )

    # 3k: Merge base-year flags => aggregator_flags
    merge_keys_flag = ['fakeid_firm'] + market_vars
    df_tag_for_merge = df_tag.drop(columns=[
        'emp','p5','p10','p25','p50','p75','p90','p95',
        'p5_trad','p10_trad','p25_trad','p50_trad','p75_trad','p90_trad','p95_trad'
    ])
    df_shares_tags = pd.merge(df_shares, df_tag_for_merge, on=merge_keys_flag, how='left')

    def aggregator_flags(g):
        out = {}
        by = str(base_year)
        out[f'top5_{by}_emp']     = (g['emp'] * g[f'top5_{by}']).sum()
        out[f'bot95_{by}_emp']    = (g['emp'] * g[f'bot95_{by}']).sum()
        out[f'top10_{by}_emp']    = (g['emp'] * g[f'top10_{by}']).sum()
        out[f'bot90_{by}_emp']    = (g['emp'] * g[f'bot90_{by}']).sum()
        out[f'top25_{by}_emp']    = (g['emp'] * g[f'top25_{by}']).sum()
        out[f'mid5075_{by}_emp']  = (g['emp'] * g[f'mid5075_{by}']).sum()
        out[f'mid2550_{by}_emp']  = (g['emp'] * g[f'mid2550_{by}']).sum()
        out[f'bot25_{by}_emp']    = (g['emp'] * g[f'bot25_{by}']).sum()
        out[f'gt50_{by}_emp']     = (g['emp'] * g[f'gt50_{by}']).sum()
        out[f'gt100_{by}_emp']    = (g['emp'] * g[f'gt100_{by}']).sum()
        out[f'gt1000_{by}_emp']   = (g['emp'] * g[f'gt1000_{by}']).sum()
        out[f'lt50_{by}_emp']     = (g['emp'] * g[f'lt50_{by}']).sum()
        out[f'lt100_{by}_emp']    = (g['emp'] * g[f'lt100_{by}']).sum()
        out[f'lt1000_{by}_emp']   = (g['emp'] * g[f'lt1000_{by}']).sum()

        out[f'top5_T_{by}_emp']    = (g['emp'] * g[f'top5_T_{by}']).sum()
        out[f'bot95_T_{by}_emp']   = (g['emp'] * g[f'bot95_T_{by}']).sum()
        out[f'top10_T_{by}_emp']   = (g['emp'] * g[f'top10_T_{by}']).sum()
        out[f'bot90_T_{by}_emp']   = (g['emp'] * g[f'bot90_T_{by}']).sum()
        out[f'top25_T_{by}_emp']   = (g['emp'] * g[f'top25_T_{by}']).sum()
        out[f'mid5075_T_{by}_emp'] = (g['emp'] * g[f'mid5075_T_{by}']).sum()
        out[f'mid2550_T_{by}_emp'] = (g['emp'] * g[f'mid2550_T_{by}']).sum()
        out[f'bot25_T_{by}_emp']   = (g['emp'] * g[f'bot25_T_{by}']).sum()
        out[f'gt50_T_{by}_emp']    = (g['emp'] * g[f'gt50_T_{by}']).sum()
        out[f'gt100_T_{by}_emp']   = (g['emp'] * g[f'gt100_T_{by}']).sum()
        out[f'gt1000_T_{by}_emp']  = (g['emp'] * g[f'gt1000_T_{by}']).sum()
        out[f'lt50_T_{by}_emp']    = (g['emp'] * g[f'lt50_T_{by}']).sum()
        out[f'lt100_T_{by}_emp']   = (g['emp'] * g[f'lt100_T_{by}']).sum()
        out[f'lt1000_T_{by}_emp']  = (g['emp'] * g[f'lt1000_T_{by}']).sum()
        return pd.Series(out)

    merge_key_final = ['year'] + market_vars
    df_sizebins = df_shares_tags.groupby(merge_key_final, as_index=False).apply(aggregator_flags)
    df_mktout = pd.merge(df_mktout, df_sizebins, on=merge_key_final, how='left')

    # 3l: Merge ICE shocks => final regsfile
    merge_key_ice = market_vars    
    df_regsfile = pd.merge(df_mktout, df_ice_final, on=merge_key_ice, how='left')

    # Final output
    out_regsfile = f"{export_path}/regsfile_{file_suffix}.dta"
    df_regsfile.to_stata(out_regsfile, write_index=False)

    print(f"Finished market vars={market_vars}. Output => {out_regsfile}")
    print(f"Percentile data => {out_qfile}, {out_qtfile}")

# --------------------------------------------------------------------
if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print("Usage: python rais_040.py <spec>")
        sys.exit(1)
    chosen_spec = sys.argv[1]
        
    # Build a dictionary for faster lookup
    from metafile import specs
    spec_dict = {spec["name"]: spec for spec in specs}
    
    if chosen_spec not in spec_dict:
        print(f"Spec '{chosen_spec}' not recognized. Options are: {', '.join(spec_dict.keys())}")
        sys.exit(1)
    
    market_vars = spec_dict[chosen_spec]["market_vars"]
    file_suffix = spec_dict[chosen_spec]["file_suffix"]
    collapsed_prefix = f"rais_collapsed_firm_{file_suffix}"
    
    print(f"Running spec: {chosen_spec}")
    print(f"Market variables: {market_vars}")
    print(f"File suffix: {file_suffix}")
    
    
    # Original version (markets = mmc + cbo942d)
    process_level2(
        market_vars,
        file_suffix,
        file_rais_collapsed=f"{MONOPASAS_PATH}/rais_collapsed_firm_{file_suffix}.parquet",
        base_year=1991,
        export_path=EXPORT_PATH
    )


    print("\nAll done!")
