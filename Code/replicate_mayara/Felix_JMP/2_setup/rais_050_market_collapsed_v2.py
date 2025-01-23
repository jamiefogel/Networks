import pandas as pd
import numpy as np
from config import root

# Paths
base_path = root + "/Code/replicate_mayara"
MONOPASAS_PATH = f"{base_path}/monopsonies/sas"
# --------------------------------------------------------------------
# 1. Define file paths (adjust these to match your environment)
# --------------------------------------------------------------------

EXPORT_PATH    = MONOPASAS_PATH   # Where to write the .dta outputs

# Input data files (Parquet assumed):
FILE_RAIS_COLLAPSED_NONE   = f"{MONOPASAS_PATH}/rais_collapsed_firm_mmc_none.parquet"
FILE_RAIS_COLLAPSED_CBO    = f"{MONOPASAS_PATH}/rais_collapsed_firm_mmc_cbo942d.parquet"
FILE_CROSSWALK_INDMATCH    = f"{MONOPASAS_PATH}/crosswalk_ibgesubsector_indmatch.parquet"
FILE_THETA_INDMATCH        = f"{MONOPASAS_PATH}/theta_indmatch.parquet"
FILE_CNAE95_TARIFF_CHANGES = f"{MONOPASAS_PATH}/cnae95_tariff_changes_1990_1994.parquet"
FILE_CROSSWALK_CNAE95      = f"{MONOPASAS_PATH}/crosswalk_cnae95_ibgesubsector.parquet"

# --------------------------------------------------------------------
# 2. Load the crosswalks and tariff tables from Parquet
# --------------------------------------------------------------------

df_cross_indmatch = pd.read_parquet(FILE_CROSSWALK_INDMATCH)    # columns: ibgesubsector, indmatch
df_theta_indmatch = pd.read_parquet(FILE_THETA_INDMATCH)        # columns: indmatch, theta
df_cnae95_tariff  = pd.read_parquet(FILE_CNAE95_TARIFF_CHANGES) # columns: cnae95, chng19941990TRAINS, chng19941990ErpTRAINS, ...
df_cross_cnae95   = pd.read_parquet(FILE_CROSSWALK_CNAE95)      # columns: cnae95, ibgesubsector, ...

# --------------------------------------------------------------------
# 3. Define a function for the main logic for each "level2"
# --------------------------------------------------------------------

def process_level2(level2, file_rais_collapsed, base_year=1991, export_path=EXPORT_PATH):
    """
    Translates the SAS macro '%append(level2=...)' but omits any exporter/importer logic
    and uses Parquet for input, Stata for output.

    This function:
      1) Reads the input Parquet file (rais_collapsed_firm_mmc_{level2}.parquet).
      2) Computes ICE shocks, Herfindahl shares, percentile cutoffs, size bin flags, etc.
      3) Produces final 'regsfile_mmc_{level2}.dta' plus the percentile cutoff files.
    """
    # --- Read the "rais_collapsed_firm_mmc_{level2}" data ---
    df = pd.read_parquet(file_rais_collapsed)

    # Ensure columns are lowercase for consistency
    df.rename(columns=str.lower, inplace=True)

    # Make sure we have the columns we need:
    #   fakeid_firm, ibgesubsector, mmc, {level2}, emp, totdecearnmw, avgdecearn, year, cnae95
    # The code also expects year=..., emp>0, etc. We apply relevant filters below.

    # ----------------------------------------------------------------
    # 3a. Compute "theta" by ibgesubsector
    #     (join crosswalk_ibgesubsector_indmatch => theta_indmatch => your df
    #      then sum(b.theta * c.emp)/sum(c.emp) for base_year, c.emp>0)
    # ----------------------------------------------------------------
    # Merge crosswalk_indmatch + theta_indmatch
    tmp_cross = pd.merge(
        df_cross_indmatch[['ibgesubsector','indmatch']],
        df_theta_indmatch[['indmatch','theta']],
        on='indmatch', how='inner'
    )

    # Merge to the main df, filtering to base_year & emp>0
    df_for_theta = pd.merge(
        df[['ibgesubsector','emp','year']],
        tmp_cross[['ibgesubsector','theta']],
        on='ibgesubsector', how='inner'
    )
    df_for_theta = df_for_theta.query("year == @base_year and emp > 0 and emp == emp")

    # Group by ibgesubsector => define final 'theta'
    group_t = df_for_theta.groupby('ibgesubsector', as_index=False)
    df_theta = group_t.apply(lambda g: pd.Series({
        'theta': (g['theta'] * g['emp']).sum() / g['emp'].sum()
    })).reset_index()

    # ----------------------------------------------------------------
    # 3b. Build "all" for base_year with earndshare, earndshare^2, empshare
    # ----------------------------------------------------------------
    df_base = df.query("year == @base_year and emp>0 and emp == emp").copy()
    group_cols = ['mmc', level2]

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

    df_all = df_all[['fakeid_firm','ibgesubsector','mmc', level2,
                     'earndshare','earndshare2','empshare']].drop_duplicates()

    # ----------------------------------------------------------------
    # 3c. "tradables": merge with cnae95_tariff_changes
    # ----------------------------------------------------------------
    df_trad = pd.merge(
        df_base[['fakeid_firm','mmc', level2, 'totdecearnmw','cnae95']],
        df_cnae95_tariff[['cnae95','chng19941990TRAINS','chng19941990ErpTRAINS','chng19941990Kume']],
        on='cnae95', how='inner'
    )
    df_trad = df_trad.dropna(subset=['chng19941990TRAINS'])

    sum_trad = df_trad.groupby(['mmc', level2])['totdecearnmw'].sum().reset_index(name='sum_decearn')
    df_trad  = pd.merge(df_trad, sum_trad, on=['mmc', level2], how='inner')
    df_trad['Tearndshare'] = df_trad['totdecearnmw'] / df_trad['sum_decearn']

    df_trad = df_trad[['fakeid_firm','mmc', level2,
                       'Tearndshare','chng19941990TRAINS','chng19941990ErpTRAINS','chng19941990Kume']]

    # ----------------------------------------------------------------
    # 3d. "beta_rf": merge with df_theta to get betadw_rf, beta_rf
    # ----------------------------------------------------------------
    df_beta = pd.merge(df_all, df_theta[['ibgesubsector','theta']], on='ibgesubsector', how='inner')

    def compute_betas(g):
        denom_dw = (g['earndshare'] / g['theta']).sum()
        denom_rf = (g['empshare']   / g['theta']).sum()
        g['betadw_rf'] = (g['earndshare'] / g['theta']) / denom_dw
        g['beta_rf']   = (g['empshare']   / g['theta']) / denom_rf
        return g

    df_beta = df_beta.groupby(['mmc', level2], as_index=False).apply(compute_betas)

    # ----------------------------------------------------------------
    # 3e. "sums": sum of empshare, earndshare, earndshare2, Tearndshare
    # ----------------------------------------------------------------
    df_sums_merged = pd.merge(df_all, df_trad, on=['fakeid_firm','mmc', level2], how='inner')
    df_sums = df_sums_merged.groupby(['mmc', level2], as_index=False).agg({
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

    # ----------------------------------------------------------------
    # 3f. "ice": create ICE shock variables
    # ----------------------------------------------------------------
    df_ice_1 = pd.merge(df_all, df_trad, on=['fakeid_firm','mmc', level2], how='inner')
    df_ice_2 = pd.merge(
        df_ice_1,
        df_beta[['fakeid_firm','mmc', level2,'betadw_rf','beta_rf']],
        on=['fakeid_firm','mmc', level2], how='inner'
    )
    df_ice   = pd.merge(df_ice_2, df_sums, on=['mmc', level2], how='inner')

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

    df_ice_final = df_ice.groupby(['mmc', level2], as_index=False).apply(compute_ice)

    # ----------------------------------------------------------------
    # 3g. Build the "shares" table for *all* years
    #     (Omits exporter/importer logic entirely)
    # ----------------------------------------------------------------
    df_shares = pd.merge(
        df.drop(columns = 'ibgesubsector' ), #XX This was showing up on both sides of the merge and creating a _x and _y version
        df_cross_cnae95[['cnae95','ibgesubsector']],
        on='cnae95', how='left'
    )

    group_shares_cols = ['mmc', level2, 'year']
    sum_for_shares = df_shares.groupby(group_shares_cols).agg({
        'emp': 'sum',
        'totdecearnmw': 'sum'
    }).rename(columns={
        'emp': 'sum_emp',
        'totdecearnmw': 'sum_totdecearnmw'
    }).reset_index()

    df_shares = pd.merge(df_shares, sum_for_shares, on=group_shares_cols, how='inner')

    # Identify "tradable" sectors for T-flags
    mask_trad  = (df_shares['ibgesubsector'] <= 14) | (df_shares['ibgesubsector'] == 25)
    mask_ntrad = (df_shares['ibgesubsector'] >= 13) & (df_shares['ibgesubsector'] <= 23)

    df_shares['temp']       = df_shares['emp'] * mask_trad
    df_shares['empshare']   = df_shares['emp'] / df_shares['sum_emp']
    df_shares['Tempshare']  = df_shares['emp'].where(mask_trad, 0) / df_shares['sum_emp']
    df_shares['NTempshare'] = df_shares['emp'].where(mask_ntrad, 0) / df_shares['sum_emp']

    df_shares['earndshare'] = df_shares['totdecearnmw'] / df_shares['sum_totdecearnmw']
    df_shares['Tearndshare'] = df_shares['totdecearnmw'].where(mask_trad, 0) / df_shares['sum_totdecearnmw']
    df_shares['NTearndshare'] = df_shares['totdecearnmw'].where(mask_ntrad, 0) / df_shares['sum_totdecearnmw']

    # ----------------------------------------------------------------
    # 3h. Produce percentile cutoffs for emp in base_year (p5, p10, p25, p50, p75, p90, p95)
    # ----------------------------------------------------------------
    base_cols = ['fakeid_firm','mmc', level2, 'emp','ibgesubsector']
    df_basey  = df_shares.loc[df_shares['year'] == base_year, base_cols].copy()
    '''
    def multi_quantiles(series):
        qvals = [0.05,0.10,0.25,0.50,0.75,0.90,0.95]
        quant = series.quantile(qvals).to_dict()
        return pd.Series({
            'p5':  quant.get(0.05, np.nan),
            'p10': quant.get(0.10, np.nan),
            'p25': quant.get(0.25, np.nan),
            'p50': quant.get(0.50, np.nan),
            'p75': quant.get(0.75, np.nan),
            'p90': quant.get(0.90, np.nan),
            'p95': quant.get(0.95, np.nan),
            'firms': len(series)
        })

    df_q = df_basey.groupby(['mmc', level2])['emp'].apply(multi_quantiles).reset_index()
    # Convert df_q from "long" to "wide" so columns p5, p10, etc. exist as actual columns:
    df_q = df_q.melt(id_vars=['mmc', level2], var_name='level2', value_name='emp_val')
    df_q = df_q.pivot(index=['mmc', level2], columns='level2', values='emp_val').reset_index()
    df_q.columns.name = None
    '''
    df_q = (
        df_basey
        .groupby(["mmc", level2])["emp"]
        .agg(
            p5 = lambda x: x.quantile(0.05),
            p10= lambda x: x.quantile(0.10),
            p25= lambda x: x.quantile(0.25),
            p50= lambda x: x.quantile(0.50),
            p75= lambda x: x.quantile(0.75),
            p90= lambda x: x.quantile(0.90),
            p95= lambda x: x.quantile(0.95),
            # Emulate SAS `_freq_` as the number of rows or distinct firms
            # 'size' => the row count in that group
            # or 'nunique' => distinct fakeid_firm
            firms="size"
        )
        .reset_index()
    )

    # Write to Stata (like SAS "proc export"):
    out_qfile = f"{export_path}/regsfile_mmc_{level2}_{base_year}_emp_pctiles.dta"
    df_q.to_stata(out_qfile, write_index=False)

    # Tradables only
    basey_trad_mask = (df_basey['ibgesubsector'] <= 14) | (df_basey['ibgesubsector'] == 25)
    df_basey_trad = df_basey[basey_trad_mask].copy()
    df_qt = (
        df_basey_trad
        .groupby(["mmc", level2])["emp"]
        .agg(
            p5 = lambda x: x.quantile(0.05),
            p10= lambda x: x.quantile(0.10),
            p25= lambda x: x.quantile(0.25),
            p50= lambda x: x.quantile(0.50),
            p75= lambda x: x.quantile(0.75),
            p90= lambda x: x.quantile(0.90),
            p95= lambda x: x.quantile(0.95),
            # Emulate SAS `_freq_` as the number of rows or distinct firms
            # 'size' => the row count in that group
            # or 'nunique' => distinct fakeid_firm
            firms="size"
        )
        .reset_index()
    )
    #df_qt = df_basey_trad.groupby(['mmc', level2])['emp'].apply(multi_quantiles).reset_index()

    out_qtfile = f"{export_path}/regsfile_mmc_{level2}_{base_year}_empT_pctiles.dta"
    df_qt.to_stata(out_qtfile, write_index=False)

    # ----------------------------------------------------------------
    # 3i. Merge percentile tags back to define top10_baseyear, etc.
    # ----------------------------------------------------------------
    # Merge df_basey with df_q => call that df_tag
    df_tag = pd.merge(df_basey, df_q, on=['mmc', level2], how='left')
    # Also merge the "tradables" quantiles:
    df_tag = pd.merge(df_tag, df_qt, on=['mmc', level2], how='left', suffixes=('', '_trad'))

    # We define a mask for whether the row is tradable (for T_ flags):
    # (SAS code uses "a.ibgesubsector <=14 | a.ibgesubsector=25")
    tag_trad_mask = (df_tag['ibgesubsector'] <= 14) | (df_tag['ibgesubsector'] == 25)

    # EXACT duplication of all flags from SAS:
    # (a.emp>=b.p90) => top10_{base_year}, etc.
    df_tag[f'top10_{base_year}']    = (df_tag['emp'] >= df_tag['p90']).astype(int)
    df_tag[f'bot90_{base_year}']    = (df_tag['emp']  < df_tag['p90']).astype(int)
    df_tag[f'top5_{base_year}']     = (df_tag['emp'] >= df_tag['p95']).astype(int)
    df_tag[f'bot95_{base_year}']    = (df_tag['emp']  < df_tag['p95']).astype(int)
    df_tag[f'bot25_{base_year}']    = (df_tag['emp'] <= df_tag['p25']).astype(int)
    df_tag[f'mid2550_{base_year}']  = ((df_tag['emp'] >= df_tag['p25']) & (df_tag['emp'] <= df_tag['p50'])).astype(int)
    df_tag[f'mid5075_{base_year}']  = ((df_tag['emp'] >= df_tag['p50']) & (df_tag['emp'] <= df_tag['p75'])).astype(int)
    df_tag[f'top25_{base_year}']    = (df_tag['emp'] >= df_tag['p75']).astype(int)
    df_tag[f'gt50_{base_year}']     = (df_tag['emp'] > 50).astype(int)
    df_tag[f'gt100_{base_year}']    = (df_tag['emp'] > 100).astype(int)
    df_tag[f'gt1000_{base_year}']   = (df_tag['emp'] > 1000).astype(int)
    df_tag[f'lt50_{base_year}']     = (df_tag['emp'] <= 50).astype(int)
    df_tag[f'lt100_{base_year}']    = (df_tag['emp'] <= 100).astype(int)
    df_tag[f'lt1000_{base_year}']   = (df_tag['emp'] <= 1000).astype(int)

    # T_ versions (use p90_trad, p95_trad, etc. AND must be in the tradable sector)
    df_tag[f'top10_T_{base_year}']   = ((df_tag['emp'] >= df_tag['p90_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot90_T_{base_year}']   = ((df_tag['emp']  < df_tag['p90_trad']) & tag_trad_mask).astype(int)
    df_tag[f'top5_T_{base_year}']    = ((df_tag['emp'] >= df_tag['p95_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot95_T_{base_year}']   = ((df_tag['emp']  < df_tag['p95_trad']) & tag_trad_mask).astype(int)
    df_tag[f'bot25_T_{base_year}']   = ((df_tag['emp'] <= df_tag['p25_trad']) & tag_trad_mask).astype(int)
    df_tag[f'mid2550_T_{base_year}'] = ((df_tag['emp'] >= df_tag['p25_trad']) & (df_tag['emp'] <= df_tag['p50_trad']) & tag_trad_mask).astype(int)
    df_tag[f'mid5075_T_{base_year}'] = ((df_tag['emp'] >= df_tag['p50_trad']) & (df_tag['emp'] <= df_tag['p75_trad']) & tag_trad_mask).astype(int)
    df_tag[f'top25_T_{base_year}']   = ((df_tag['emp'] >= df_tag['p75_trad']) & tag_trad_mask).astype(int)
    df_tag[f'gt50_T_{base_year}']    = ((df_tag['emp'] > 50) & tag_trad_mask).astype(int)
    df_tag[f'gt100_T_{base_year}']   = ((df_tag['emp'] > 100) & tag_trad_mask).astype(int)
    df_tag[f'gt1000_T_{base_year}']  = ((df_tag['emp'] > 1000) & tag_trad_mask).astype(int)
    df_tag[f'lt50_T_{base_year}']    = ((df_tag['emp'] <= 50) & tag_trad_mask).astype(int)
    df_tag[f'lt100_T_{base_year}']   = ((df_tag['emp'] <= 100) & tag_trad_mask).astype(int)
    df_tag[f'lt1000_T_{base_year}']  = ((df_tag['emp'] <= 1000) & tag_trad_mask).astype(int)

    # ----------------------------------------------------------------
    # 3j. Construct final "mktout" by grouping over mmc, level2, year
    # ----------------------------------------------------------------
    group_shares = df_shares.groupby(['mmc', level2, 'year'], as_index=False)
    df_mktout = group_shares.agg(
        mkt_temp = ('temp','sum'),
        mkt_emp  = ('emp','sum'),
        mkt_wdbill = ('totdecearnmw','sum'),
        mkt_avgdearn = ('avgdecearn', lambda x: (df_shares.loc[x.index,'emp']*x).sum() / df_shares.loc[x.index,'emp'].sum()),
        avg_firmemp = ('emp','mean'),
        hf_emp     = ('empshare', lambda x: (x**2).sum()),
        hf_wdbill  = ('earndshare', lambda x: (x**2).sum()),
        hf_Temp    = ('Tempshare', lambda x: (x**2).sum()),
        hf_Twdbill = ('Tearndshare', lambda x: (x**2).sum()),
        hf_NTemp   = ('NTempshare', lambda x: (x**2).sum()),
        hf_NTwdbill= ('NTearndshare', lambda x: (x**2).sum()),
        mkt_firms  = ('fakeid_firm','nunique')
    )
     

    # ----------------------------------------------------------------
    # 3k. Merge the base-year flags onto the full df_shares => sum the flagged emp
    # ----------------------------------------------------------------
    # We do a left join on [fakeid_firm, mmc, level2] so each year’s data can pick up the base-year flags
    # Then group by mmc, level2, year => sum up the "emp" for each flag
    df_tag_for_merge = df_tag.drop(columns=[
        'emp','p5','p10','p25','p50','p75','p90','p95',
        'p5_trad','p10_trad','p25_trad','p50_trad','p75_trad','p90_trad','p95_trad'
    ])

    df_shares_tags = pd.merge(df_shares, df_tag_for_merge, on=['fakeid_firm','mmc', level2], how='left')

    def aggregator_flags(g):
        """
        Sums 'emp' for each of the 28 flags:
          top5_1991, bot95_1991, top10_1991, bot90_1991, top25_1991, mid5075_1991, mid2550_1991, bot25_1991,
          gt50_1991, gt100_1991, gt1000_1991, lt50_1991, lt100_1991, lt1000_1991,
          top5_T_1991, bot95_T_1991, top10_T_1991, bot90_T_1991, top25_T_1991, mid5075_T_1991, mid2550_T_1991, bot25_T_1991,
          gt50_T_1991, gt100_T_1991, gt1000_T_1991, lt50_T_1991, lt100_T_1991, lt1000_T_1991
        """
        out = {}
        # We'll define base = str(base_year) so we can do f'top5_{base}_baseyear'
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

    df_sizebins = df_shares_tags.groupby(['mmc', level2, 'year'], as_index=False).apply(aggregator_flags)

    # Merge df_sizebins into df_mktout
    df_mktout = pd.merge(df_mktout, df_sizebins, on=['mmc', level2, 'year'], how='left')

    # ----------------------------------------------------------------
    # 3l. Merge ICE shocks => final "regsfile" DataFrame
    # ----------------------------------------------------------------
    df_regsfile = pd.merge(df_mktout, df_ice_final, on=['mmc', level2], how='left')

    # ----------------------------------------------------------------
    # 3m. Export final "regsfile_mmc_{level2}.dta"
    # ----------------------------------------------------------------
    out_regsfile = f"{export_path}/regsfile_mmc_{level2}.dta"
    df_regsfile.to_stata(out_regsfile, write_index=False)

    print(f"Finished level2={level2}, wrote {out_regsfile}")
    print(f"Also wrote: {out_qfile} and {out_qtfile} (percentile data)")


# --------------------------------------------------------------------
# 4. Run for level2=none and level2=cbo942d
# --------------------------------------------------------------------

if __name__ == "__main__":
    process_level2(
        level2="none",
        file_rais_collapsed=FILE_RAIS_COLLAPSED_NONE,
        base_year=1991,
        export_path=EXPORT_PATH
    )

    process_level2(
        level2="cbo942d",
        file_rais_collapsed=FILE_RAIS_COLLAPSED_CBO,
        base_year=1991,
        export_path=EXPORT_PATH
    )

    print("All done!")
