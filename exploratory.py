import pandas as pd
from re import search


def get_id_like_columns(cols: list) -> list:
    candidates = []
    for col in cols:
        if search("id$|_id", col) is not None:
            candidates.append(col)
    return candidates


def explore_unique_keys(cons: pd.DataFrame, cons_email: pd.DataFrame, cons_sub: pd.DataFrame) -> None:
    def comparison_to_cons(cons_comp: pd.DataFrame, tag: str):
        for col in get_id_like_columns(list(cons.columns)):
            cons_max = cons[col].max()
            cons_min = cons[col].min()
            cons_un = cons[col].nunique()
            cons_set = set(cons[col].unique())
            print(f"Cons column (len {len(cons)}): {col} has a maximum of {cons_max} and a minimum of {cons_min} and {cons_un} unique values\n")
            for col_comp in get_id_like_columns(list(cons_comp.columns)):
                print(f"{tag} column: {col} in cons when compared to {col_comp} in cons_{tag}")
                comp_max = cons_comp[col_comp].max()
                comp_min = cons_comp[col_comp].min()
                comp_un = cons_comp[col_comp].nunique()
                comp_set = set(cons_comp[col_comp].unique())
                print(f"{tag} column (len {len(cons_comp)}): {col_comp} has a maximum of {comp_max} and a minimum of {comp_min} and {comp_un} unique values")
                cons_in_comp = len(cons_set.difference(comp_set))
                print(f" - Of {cons_un} unique in cons file, there are {cons_in_comp} that are not in this column of the {tag} file")
                comp_in_cons = len(comp_set.difference(cons_set))
                print(f" - Of {comp_un} unique in {tag} file, there are {comp_in_cons} that are not in this column of the cons file")
                print("")

    comparison_to_cons(cons_email,"Email")
    print("\n\n\n")
    comparison_to_cons(cons_sub, "sub")

