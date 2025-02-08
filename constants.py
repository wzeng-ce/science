import os

CLOSEST_DIST_LIMIT = 0.39
DATA_SOURCES = ["iri"]
BRANDS_CSV = "brands.csv"
A_BRANDS_CSV = "A_brands.csv"
PREPROCESSED_CSV = "preprocessed_data.csv"
MATCHES_CSV = "jarowinkler_matches.csv"
BRANDS_THAT_MATCH_CSV = "brands_that_match.csv"
CLOSEST_BRANDS_CSV = "closest_brands.csv"
DELIVERABLE_MAPPED_BRANDS_CSV = "deliverable_mapped_brands.csv"

DATA_DIRECTORY = "data"
DEDUPLICATE_BRAND_DIR = "deduplicate_brand"
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
