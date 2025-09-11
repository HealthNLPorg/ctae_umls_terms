import polars as pl
import argparse
import json
from functools import partial

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
    return term_dictionary.get(term_key)


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
        pl.col(term_key).map_elements(normalize_term).alias(term_key)
    ).write_csv(
        os.path.join(output_dir, "total.bsv"),
        separator="|",
        include_header=False,
    )


def create_cui_mappings(df: pl.DataFrame, output_dir: str) -> None:
    def normalize_term(term: str) -> str:
        return " ".join(term.split()).title()

    df.with_columns(pl.col("ctae").map_elements(normalize_term).alias("ctae")).select(
        "root-cui", "ctae"
    ).unique().write_csv(
        os.path.join(output_dir, "cui_to_event_type.tsv"),
        separator="\t",
        include_header=False,
    )


def main() -> None:
    args = parser.parse_args()
    df = pl.concat(pl.read_csv(input_csv) for input_csv in args.input_csvs)
    create_ctakes_bsv(df, args.output_dir)


if __name__ == "__main__":
    main()
