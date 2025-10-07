import polars as pl
import argparse
import json
from functools import partial
from typing import cast

import os

parser = argparse.ArgumentParser(description="")

parser.add_argument(
    "--input_csvs",
    nargs="+",
    type=str,
    help="GPT enriched synonym CSVs for conversion",
)
parser.add_argument(
    "--output_dir",
    type=str,
    default=".",
    help="Directory for writing the cTAKES dictionary style BSV",
)


def parse_term_json(
    term_key: str,
    term_json: str,
) -> str:
    try:
        term_dictionary = json.loads(term_json)
    except Exception:
        term_dictionary = {}
    term_value = term_dictionary.get(term_key)
    if term_value is not None:
        return term_value
    ValueError(f"Empty term for {term_key} in {term_json}")
    return ""


def create_ctakes_bsv(
    df: pl.DataFrame, output_dir: str, term_key: str = "term"
) -> None:
    local_parse_term = partial(parse_term_json, term_key)
    cleaned_terms_df = df.with_columns(
        pl.col(term_key).map_elements(local_parse_term).alias(term_key)
    ).drop_nulls()

    def normalize_term(term: str) -> str:
        return " ".join(term.split()).lower()

    cleaned_terms_df.with_columns(
        pl.col(term_key).map_elements(normalize_term).alias(term_key),
        pl.lit("000").alias("TUI"),
    ).select("root-cui", "TUI", "term").write_csv(
        os.path.join(output_dir, "total.bsv"),
        separator="|",
        include_header=False,
    )


def create_cui_mappings(df: pl.DataFrame, output_dir: str) -> None:
    def normalize_term(term: str) -> str:
        return " ".join(term.strip().split()).title()

    def is_rt(normalized_term: str) -> bool:
        return normalized_term == "Radiation Therapy (Procedure)"

    cui_to_event_type = (
        df.with_columns(pl.col("ctae").map_elements(normalize_term).alias("event_type"))
        .select("root-cui", "event_type")
        .unique()
    )

    def linify(s: str) -> str:
        return s + "\n"

    for rt, group in cui_to_event_type.group_by(is_rt(cast(str, pl.col("event_type")))):
        (rt,) = rt
        cuis = group.select("root-cui").unique().to_series()
        with open(
            os.path.join(output_dir, "rt_cuis.txt" if rt else "ctae_cuis.txt"), mode="w"
        ) as f:
            f.writelines(map(linify, cuis))


def main() -> None:
    args = parser.parse_args()
    df = pl.concat(pl.read_csv(input_csv) for input_csv in args.input_csvs)
    create_ctakes_bsv(df, args.output_dir)
    create_cui_mappings(df, args.output_dir)


if __name__ == "__main__":
    main()
