import os

DATA_DIRECTORY = "data"
MAPPINGS_DIRECTORY = "mappings"
DEDUPLICATE_BRAND_DIR = "deduplicate_brand"
DOCS_DIR = "docs"
GRAPHS_DIR = "graphs"

DATA_SOURCES = ["iri"]
BRANDS_CSV = "brands.csv"
PREPROCESSED_CSV = "preprocessed_data.csv"
BRANDS_THAT_MATCH_CSV = os.path.join(MAPPINGS_DIRECTORY, "brands_that_match.csv")
MANUAL_CLUSTERS_JSON = os.path.join(MAPPINGS_DIRECTORY, "manual_clusters.json")
CLOSEST_BRANDS_CSV = os.path.join(MAPPINGS_DIRECTORY, "closest_brands.csv")
DELIVERABLE_MAPPED_BRANDS_CSV = os.path.join(MAPPINGS_DIRECTORY, "deliverable_mapped_brands.csv")

HELIOS_COVERAGE_MD = os.path.join(DOCS_DIR, "helios_coverage.md")


def get_brand_clusters_file(df_name):
    return os.path.join(MAPPINGS_DIRECTORY, f"symbol_brand_brandstring_clusters_{df_name}.json")

def DUPLICATE_FILE(prefix):
    return os.path.join(DEDUPLICATE_BRAND_DIR, f"{prefix}_duplicates.csv")
def PREPROCESSED_FILE(prefix):
    return os.path.join(DEDUPLICATE_BRAND_DIR, f'{prefix}_preprocessed.csv')
MISC_NAME = "misc"
BRAND_PREFIX_TO_FILE_NAME = {
    "A": "A_brands.csv",
    "B": "B_brands.csv",
    "C": "C_brands.csv",
    "D": "D_brands.csv",
    "E": "E_brands.csv",
    "F": "F_brands.csv",
    "G": "G_brands.csv",
    "H": "H_brands.csv",
    "I": "I_brands.csv",
    "J": "J_brands.csv",
    "K": "K_brands.csv",
    "L": "L_brands.csv",
    "M": "M_brands.csv",
    "N": "N_brands.csv",
    "O": "O_brands.csv",
    "P": "P_brands.csv",
    "Q": "Q_brands.csv",
    "R": "R_brands.csv",
    "S": "S_brands.csv",
    "T": "T_brands.csv",
    "U": "U_brands.csv",
    "V": "V_brands.csv",
    "W": "W_brands.csv",
    "X": "X_brands.csv",
    "Y": "Y_brands.csv",
    "Z": "Z_brands.csv",
    MISC_NAME: "misc.csv"
}
