import os

CLOSEST_DIST_LIMIT = 0.39

BRANDS_CSV = "brands.csv"
A_BRANDS_CSV = "A_brands_url.csv"
PREPROCESSED_CSV = "preprocessed_data.csv"
MATCHES_CSV = "jarowinkler_matches.csv"
NON_DUPLICATES_CSV = "nonduplicates.csv"
MAPPED_BRANDS_WITH_INDICES_CSV = "mapped_brands_with_indices.csv"
DELIVERABLE_MAPPED_BRANDS_CSV = "deliverable_mapped_brands.csv"

DATA_DIRECTORY = "data"
DEDUPLICATE_BRAND_DIR = "deduplicate_brand"
def DUPLICATE_FILE(prefix):
    return os.path.join(DEDUPLICATE_BRAND_DIR, f"{prefix}_duplicates.csv")
def PREPROCESSED_FILE(prefix):
    return os.path.join(DEDUPLICATE_BRAND_DIR, f'{prefix}_preprocessed.csv')
MISC_NAME = "misc"
BRAND_PREFIX_TO_FILE_NAME = {
    "A": "A_brands_url.csv",
    "B": "B_brands_url.csv",
    "C": "C_brands_url.csv",
    "D": "D_brands_url.csv",
    "E": "E_brands_url.csv",
    "F": "F_brands_url.csv",
    "G": "G_brands_url.csv",
    "H": "H_brands_url.csv",
    "I": "I_brands_url.csv",
    "J": "J_brands_url.csv",
    "K": "K_brands_url.csv",
    "L": "L_brands_url.csv",
    "M": "M_brands_url.csv",
    "N": "N_brands_url.csv",
    "O": "O_brands_url.csv",
    "P": "P_brands_url.csv",
    "Q": "Q_brands_url.csv",
    "R": "R_brands_url.csv",
    "S": "S_brands_url.csv",
    "T": "T_brands_url.csv",
    "U": "U_brands_url.csv",
    "V": "V_brands_url.csv",
    "W": "W_brands_url.csv",
    "X": "X_brands_url.csv",
    "Y": "Y_brands_url.csv",
    "Z": "Z_brands_url.csv",
    MISC_NAME: "misc.csv"
}
