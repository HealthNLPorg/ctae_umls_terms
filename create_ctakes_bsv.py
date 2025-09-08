import polars as pl
import argparse
import json
from functools import partial
import pathlib

import os

parser = argparse.ArgumentParser(description="")

parser.add_argument(
    "--input_csv",
    type=str,
    help="GPT enriched synonym CSV for conversion",
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


def create_ctakes_bsv(input_csv: str, output_dir: str, term_key: str = "term") -> None:
    df = pl.read_csv(input_csv)
    local_parse_term = partial(parse_term_json, term_key)
    cleaned_terms_df = df.with_columns(
        pl.col(term_key).map_elements(local_parse_term).alias(term_key)
    ).drop_nulls()

    def normalize_term(term: str) -> str:
        return " ".join(term.split()).lower()

    cleaned_terms_df.with_columns(
        pl.col(term_key).map_elements(normalize_term).alias(term_key)
    ).write_csv(
        os.path.join(output_dir, f"{pathlib.Path(input_csv)}.bsv"),
        separator="|",
        include_header=False,
    )


def main() -> None:
    args = parser.parse_args()
    create_ctakes_bsv(args.input_csv, args.output_dir)


if __name__ == "__main__":
    main()
