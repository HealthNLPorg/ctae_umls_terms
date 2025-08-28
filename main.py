# General setup.
# There are several client apis avaiable for interaction with the UMLS server.
# We are using one named umls-python-client.
# https://palasht75.github.io/umls-python-client-homepage/
# https://github.com/palasht75/umls-python-client
import requests
import argparse
import logging
from collections.abc import Iterable
from functools import lru_cache
import json

# import csv
from requests.exceptions import HTTPError

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
logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


def collect_descendant_cuis(url: str, api_key: str, page_size: int) -> set[str]:
    """
    Collect cuis for descendants.
    - Parameters:
        url (str): The url to use to fetch descendants.
    - Returns:
        set: set of unique descendant cuis.
    """
    resp = requests.get(
        url,
        params={
            "apiKey": api_key,
            "language": "ENG",
            #            "pageNumber": page,
            "pageSize": page_size,
            #            "includeObsolete": False,
            #            "includeSuppressible": False
        },
    )
    resp.raise_for_status()
    desc_cuis = set()
    for atom in resp.json().get("result"):
        desc_concept = atom.get("concept")
        desc_url_path = desc_concept.split("/")
        desc_cui = desc_url_path[-1]
        desc_cuis.add(desc_cui)
    return desc_cuis

@lru_cache
def get_cui_atoms_dict(cui_api, cui: str) -> dict:
    return json.loads(cui_api.get_atoms(cui=cui, language="ENG"))


# cui_atoms = cui_api.get_atoms(cui=cui, language="ENG")
# atoms_dict = json.loads(cui_atoms)
def collect_cuis(atoms_dict: dict[str, dict[str, str]]) -> set[str]:
    all_desc_cuis = set()
    for atom in atoms_dict.get("result"):
        if atom is not None:
            descendants_url = atom.get("descendants")
            if descendants_url is not None and descendants_url != "NONE":
                desc_cuis = collect_descendant_cuis(descendants_url)
                all_desc_cuis.update(desc_cuis)
    return all_desc_cuis


def get_cui_synonymns(
    atoms_dict: dict[str, dict[str, str]], lowercase_synonyms: bool
) -> set[str]:
    synonyms = set()
    for atom in atoms_dict.get("result"):
        #        print(atom)
        tt = atom.get("termType")
        if tt == "FN" or tt == "OF" or tt == "OAF" or tt == "OAP":
            # certain UMLS terms are a -Functional- type, which isn't a synonym.
            continue
        if lowercase_synonyms:
            synonyms.add(atom.get("name").lower())
        else:
            synonyms.add(atom.get("name"))
    return synonyms


# """
# Save all synonym information to file.
# - Parameters:
#     cui (str): The UMLS Concept Unique Identifier.
# """
# cui_atoms = cui_api.get_atoms(cui=cui, language="ENG")
# atoms_dict = json.loads(cui_atoms)
def save_cui_synonyms(
    cui: str,
    synonyms: Iterable[str],
    cui_synonym_path: str,
) -> None:
    with open(cui_synonym_path, "a") as f:
        for synonym in synonyms:
            f.write(f"{cui},{synonym}\n")


def save_cui_name(cui_api, cui: str, cui_name_path: str) -> None:
    """
    Save basic cui information to file.
    - Parameters:
        cui (str): The UMLS Concept Unique Identifier.
    """
    cui_info = cui_api.get_cui_info(cui=cui)
    cui_dict = json.loads(cui_info)
    cui_name = cui_dict.get("result").get("name")
    with open(cui_name_path, "a") as f:
        f.write(f"{cui},{cui_name}\n")


def process(
    api_key: str,
    source_dir: str,
    target_dir: str,
    root_cui_fn: str,
    cui_name_fn: str,
    cui_synonym_fn: str,
    lowercase_synonyms: bool,

) -> None:
    # TODO migrate these and other global or
    # hardcoded variables to function arguments
    # and from there to cli options
    # api_key = "6e862256-725b-4400-b9f7-8b6ae7e371a0"
    # root_cui_fn = "RootCuis.txt"
    # cui_name_fn = "CuiNames.csv"
    # cui_synonym_fn = "CuiSynonyms.csv"

    root_cui_path = os.path.join(source_dir, root_cui_fn)
    cui_name_path = os.path.join(source_dir, cui_name_fn)
    cui_synonym_path = os.path.join(source_dir, cui_synonym_fn)
    logger.info(f"Working Directory: {source_dir}")
    if os.path.exists(cui_name_path):
        logger.info("Erasing Cui Name File:", cui_name_path)
        os.remove(cui_name_path)
    if os.path.exists(cui_synonym_path):
        logger.info("Erasing Cui Synonym File:", cui_synonym_path)
        os.remove(cui_synonym_path)

    # Initialize the CUIAPI class with your API key.
    # TODO - retain in case we need to extend
    # cui_api = client.cuiAPI
    # source_api = client.sourceAPI
    # Initialize the uml-python-client main client.
    # client = UMLSClient(api_key=api_key)
    # For case-sensitivity, set LOWERCASE_SYNONYMS = False
    # LOWERCASE_SYNONYMS = True
    # PAGE_SIZE = 1000

    logger.info(f"Reading Root CUIs from: {root_cui_path}")
    with open(root_cui_path, "r") as f:
        root_cuis = set(line.strip() for line in f)

    # Collect all descendant cuis for the root cuis.
    all_cuis = set()
    for root_cui in root_cuis:
        all_cuis.add(root_cui)
        desc_cuis = collect_cuis(root_cui)
        all_cuis.update(desc_cuis)

    # Save all cuis and their synonyms to file.
    for cui in all_cuis:
        save_cui_name(cui)
        synonyms = get_cui_synonymns()
        save_cui_synonyms(
            cui,
        )


def main():
    args = parser.parse_args()
    process(
        args.api_key,
        args.source_dir,
        args.target_dir,
        args.root_cui_fn,
        args.cui_name_fn,
        args.cui_synonym_fn,
    )


if __name__ == "__main__":
    main()
