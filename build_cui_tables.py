# General setup.
# There are several client apis avaiable for interaction with the UMLS server.
# We are using one named umls-python-client.
# https://palasht75.github.io/umls-python-client-homepage/
# https://github.com/palasht75/umls-python-client
import requests
import argparse
import logging
from collections.abc import Iterable
from itertools import chain
from functools import lru_cache, partial
from typing import Any
import json

# import csv

from umls_python_client import UMLSClient
import os

parser = argparse.ArgumentParser(description="")
parser.add_argument(
    "--umls_api_key",
    type=str,
    help="Your UMLS API key",
)
parser.add_argument(
    "--source_dir",
    type=str,
    default=".",
    help="Directory containing the root CUIs file",
)
parser.add_argument(
    "--target_dir",
    type=str,
    default=".",
    help="Where we want to write the resources",
)
parser.add_argument(
    "--root_cui_fn",
    type=str,
    default="RootCuis.txt",
    help="Filename containing collection of CUIs for which we want descendants and synonyms",
)
parser.add_argument(
    "--cui_name_fn",
    type=str,
    default="CuiNames.csv",
    help="Destination filename for CUI names",
)
parser.add_argument(
    "--cui_synonym_fn",
    type=str,
    default="CuiSynonyms.csv",
    help="Destination filename for CUI names",
)
parser.add_argument(
    "--page_size",
    type=int,
    default=1_000,
    help="Need to check what this does",
)
parser.add_argument(
    "--lowercase_synonyms",
    type=bool,
    default=True,
    help="Whether to normalize synonyms by lowercasing",
)
logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


@lru_cache
def collect_descendant_cuis(
    umls_api_key: str, page_size: int, url: str
) -> Iterable[str]:
    resp = requests.get(
        url,
        params={
            "apiKey": umls_api_key,
            "language": "ENG",
            "pageSize": page_size,
        },
    )
    resp.raise_for_status()
    for atom in resp.json().get("result"):
        desc_concept = atom.get("concept")
        desc_url_path = desc_concept.split("/")
        desc_cui = desc_url_path[-1]
        yield desc_cui


@lru_cache
def get_cui_atoms_dict(cui_api, cui: str) -> dict:
    return json.loads(cui_api.get_atoms(cui=cui, language="ENG"))


def collect_cuis(cui_api, umls_api_key: str, page_size: int, cui: str) -> Iterable[str]:
    atoms_dict = get_cui_atoms_dict(cui_api, cui)
    local_collect_descendant_cuis = partial(
        collect_descendant_cuis, umls_api_key, page_size
    )
    for atom in atoms_dict.get("result"):
        if atom is not None:
            descendants_url = atom.get("descendants")
            if descendants_url is not None and descendants_url != "NONE":
                yield from local_collect_descendant_cuis(descendants_url)


def atom_to_synonym(lowercase_synonym: bool, atom: dict) -> str | None:
    tt = atom.get("termType")
    if tt == "FN" or tt == "OF" or tt == "OAF" or tt == "OAP":
        return None
    return atom.get("name").lower() if lowercase_synonym else atom.get("name")


def get_cui_synonymns(cui_api, lowercase_synonyms: bool, cui: str) -> Iterable[str]:
    atoms_dict = get_cui_atoms_dict(cui_api, cui)
    get_synonym = partial(atom_to_synonym, lowercase_synonyms)
    for atom in atoms_dict.get("result"):
        synonym = get_synonym(atom)
        if synonym is not None:
            yield synonym


def save_cui_synonym_to_table(
    cui: str,
    unique_synonyms: Iterable[str],
    cui_synonym_path: str,
) -> None:
    with open(cui_synonym_path, "a") as f:
        for synonym in unique_synonyms:
            f.write(f"{cui},{synonym}\n")


def save_cui_name_to_table(cui_api, cui_name_path: str, cui: str) -> None:
    cui_info = cui_api.get_cui_info(cui=cui)
    cui_dict = json.loads(cui_info)
    cui_name = cui_dict.get("result").get("name")
    with open(cui_name_path, "a") as f:
        f.write(f"{cui},{cui_name}\n")


def build_umls_tables(
    umls_api_key: str,
    source_dir: str,
    target_dir: str,
    root_cui_fn: str,
    cui_name_fn: str,
    cui_synonym_fn: str,
    page_size: int,
    lowercase_synonyms: bool,
) -> None:
    root_cui_path = os.path.join(source_dir, root_cui_fn)
    cui_name_path = os.path.join(target_dir, cui_name_fn)
    cui_synonym_path = os.path.join(target_dir, cui_synonym_fn)
    logger.info(f"Working directory: {source_dir}")
    if os.path.exists(cui_name_path):
        logger.info(f"Erasing CUI name file: {cui_name_path}")
        os.remove(cui_name_path)
    if os.path.exists(cui_synonym_path):
        logger.info(f"Erasing CUI synonym file: {cui_synonym_path}")
        os.remove(cui_synonym_path)

    client = UMLSClient(api_key=umls_api_key)
    cui_api = client.cuiAPI

    logger.info(f"Reading Root CUIs from: {root_cui_path}")
    local_collect_cuis = partial(collect_cuis, cui_api, umls_api_key, page_size)

    def is_str_and_not_comment(line: Any) -> bool:
        return isinstance(line, str) and not line.startswith("#")

    with open(root_cui_path, "r") as f:
        raw_root_cuis = map(str.strip, filter(is_str_and_not_comment, f))

        all_unique_cuis = sorted(
            set(
                chain.from_iterable(
                    chain((root_cui,), local_collect_cuis(root_cui))
                    for root_cui in raw_root_cuis
                )
            )
        )

    local_save_cui_name_to_table = partial(
        save_cui_name_to_table,
        cui_api,
        cui_name_path,
    )
    local_get_cui_synonyms = partial(
        get_cui_synonymns,
        cui_api,
        lowercase_synonyms,
    )
    for cui in all_unique_cuis:
        local_save_cui_name_to_table(cui)
        save_cui_synonym_to_table(
            cui, sorted(set(local_get_cui_synonyms(cui))), cui_synonym_path
        )


def main():
    args = parser.parse_args()
    build_umls_tables(
        args.umls_api_key,
        args.source_dir,
        args.target_dir,
        args.root_cui_fn,
        args.cui_name_fn,
        args.cui_synonym_fn,
        args.page_size,
        args.lowercase_synonyms,
    )


if __name__ == "__main__":
    main()
