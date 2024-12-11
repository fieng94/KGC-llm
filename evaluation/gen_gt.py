import pandas as pd
from cand_gen.triple_filter import compute_missing_df

def gen_gt(missing_df:pd.DataFrame, filtred_df:pd.DataFrame, df_size:int = 500) -> pd.DataFrame:
    """
    Function to generate ground truth
    """
    true_cand_df = filtred_df[filtred_df.apply(tuple, axis=1).isin(missing_df.apply(tuple, axis=1))]
    false_cand_df = filtred_df[~filtred_df.apply(tuple, axis=1).isin(missing_df.apply(tuple, axis=1))]
    true_cand_df['Missing'] = 1
    false_cand_df['Missing'] = 0
    test_df = pd.concat([true_cand_df.sample(int(df_size/2)), false_cand_df.sample(int(df_size/2))])
    return test_df