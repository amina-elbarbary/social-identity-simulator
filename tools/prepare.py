import re
from typing import OrderedDict

import duckdb
import pandas as pd
import pyreadr

regex_3 = r"([^_]+)_([^_]+)_([^_]+)"
regex_2 = r"([^_]+)_([^_]+)"
regex_1 = r"([^_]+)"

indicator_groups = {
    "edu_avg": {"indicator": "edu", "group": "avg", "id": "edu_avg"},
    "edu_low": {"indicator": "edu", "group": "low", "id": "edu_low"},
    "edu_high": {"indicator": "edu", "group": "high", "id": "edu_high"},
    "inc_avg": {"indicator": "inc", "group": "avg", "id": "inc_avg"},
    "inc_low": {"indicator": "inc", "group": "low", "id": "inc_low"},
    "inc_high": {"indicator": "inc", "group": "high", "id": "inc_high"},
    "cit_ger": {"indicator": "cit", "group": "ger", "id": "cit_ger"},
    "cit_nonger": {"indicator": "cit", "group": "nonger", "id": "cit_nonger"},
    "loc_east": {"indicator": "loc", "group": "east", "id": "loc_east"},
    "loc_west": {"indicator": "loc", "group": "west", "id": "loc_west"},
    "gen_male": {"indicator": "gen", "group": "male", "id": "gen_male"},
    "gen_female": {"indicator": "gen", "group": "female", "id": "gen_female"},
    "age_1": {"indicator": "age", "group": "1", "id": "age_1"},
    "age_2": {"indicator": "age", "group": "2", "id": "age_2"},
    "age_3": {"indicator": "age", "group": "3", "id": "age_3"},
    "age_4": {"indicator": "age", "group": "4", "id": "age_4"},
    "age_5": {"indicator": "age", "group": "5", "id": "age_5"},
}

ind_mapping = {
    "avgedu": indicator_groups["edu_avg"],
    "ledu": indicator_groups["edu_low"],
    "hedu": indicator_groups["edu_high"],
    "lowedu": indicator_groups["edu_low"],
    "highedu": indicator_groups["edu_high"],
    "avginc": indicator_groups["inc_avg"],
    "avginv": indicator_groups["inc_avg"],
    "linc": indicator_groups["inc_low"],
    "hinc": indicator_groups["inc_high"],
    "lowinc": indicator_groups["inc_low"],
    "highinc": indicator_groups["inc_high"],
    "german": indicator_groups["cit_ger"],
    "nongerman": indicator_groups["cit_nonger"],
    "east": indicator_groups["loc_east"],
    "west": indicator_groups["loc_west"],
    "male": indicator_groups["gen_male"],
    "female": indicator_groups["gen_female"],
    "age1": indicator_groups["age_1"],
    "age2": indicator_groups["age_2"],
    "age3": indicator_groups["age_3"],
    "age4": indicator_groups["age_4"],
    "age5": indicator_groups["age_5"],
}


def get_variables(v):
    ret = re.findall(regex_3, v)
    if len(ret) > 0:
        return ret[0]

    ret = re.findall(regex_2, v)
    if len(ret) > 0:
        return ret[0]

    ret = re.findall(regex_1, v)
    if len(ret) > 0:
        return (ret[0],)


def transform_single(
    df: pd.DataFrame,
    key: str,
    combination: int,
):
    variables = get_variables(key)
    variables = [ind_mapping.get(v, None) for v in variables]
    if all(v is None for v in variables):
        return

    df = df[pd.notna(df.year)].copy()

    df.columns = [
        c.replace("age5", "age")
        .replace("inc3", "inc")
        .replace("res.edu", "edu")
        .replace("res.inc", "inc")
        .replace("res.age", "age")
        .replace("si.in.", "")
        for c in df.columns
    ]

    df["indicator_combination"] = combination

    homo = ["female", "german", "east", "edu", "inc", "age"]
    si_50 = ["gen_d50", "cit_d50", "edu_d50", "loc_d50", "inc_d50", "age_d50"]
    si_25 = ["gen_d25", "cit_d25", "edu_d25", "loc_d25", "inc_d25", "age_d25"]
    si_75 = ["gen_d75", "cit_d75", "edu_d75", "loc_d75", "inc_d75", "age_d75"]

    cols = homo + si_50 + si_25 + si_75

    d = df.loc[:, ["year"] + cols]
    d["combination"] = combination

    def kk(frame, c, t):
        ret = pd.DataFrame([])
        ret["year"] = frame["year"]
        ret["combination_id"] = frame["combination"]
        ret["indicator"] = c
        ret["indicator_type"] = t
        ret["value"] = frame[c]
        return ret

    homo_dfs = [kk(d, c, "Homogeneity") for c in homo]
    si_25_dfs = [kk(d, c, "Class mobilization") for c in si_25]
    si_50_dfs = [kk(d, c, "No mobilization") for c in si_50]
    si_75_dfs = [kk(d, c, "Identity mobilization") for c in si_75]

    ret = pd.concat(homo_dfs + si_50_dfs + si_25_dfs + si_75_dfs)
    ret.to_parquet(f"d/{key}.parquet", index=None)
    return variables, f"d/{key}.parquet"


def transform_data(
    data: OrderedDict[str, pd.DataFrame],
    connection: duckdb.DuckDBPyConnection,
):
    indicators = set()
    combinations = []
    file_paths = []
    for idx, key in enumerate(data.keys()):
        result = transform_single(
            df=data[key],
            key=key,
            combination=idx,
        )
        if result is None:
            continue
        variables, file_path = result
        file_paths.append(file_path)
        for v in variables:
            indicators.add((v["indicator"], v["group"]))
            combinations.append((v["indicator"], v["group"], idx))

    indicators = [(idx, *it) for idx, it in enumerate(indicators)]
    combinations = [
        (it[2], [x for x in indicators if x[1] == it[0] and x[2] == it[1]][0][0])
        for it in combinations
    ]
    connection.executemany(
        "INSERT INTO indicator VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
        [it for it in indicators],
    )
    connection.executemany(
        "INSERT INTO combination VALUES (?) ON CONFLICT DO NOTHING",
        [(it[0],) for it in combinations],
    )
    connection.executemany(
        "INSERT INTO combination_indicator VALUES (?, ?) ON CONFLICT DO NOTHING",
        [it for it in combinations],
    )

    for f in file_paths:
        connection.execute(
            f"INSERT INTO distances "
            f"SELECT year, combination_id, indicator, indicator_type, value "
            f'FROM "{f}"'
        )


def main():
    data = pyreadr.read_r("data.RData")

    with duckdb.connect("duck.db") as con:
        with open("sql/create.sql", "r") as f:
            sql = f.read()
            statements = [s.strip() for s in sql.split(";")]
        for s in statements:
            con.execute(s)
        transform_data(data, con)


if __name__ == "__main__":
    main()
