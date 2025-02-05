import os
import argparse
import constants
import pandas as pd
import json
from google.cloud import bigquery
from rapidfuzz import process, distance
from rapidfuzz.process import extractOne
import unicodedata
import re

os.makedirs(constants.DEDUPLICATE_BRAND_DIR, exist_ok=True)

def read_data(file_name):
    return pd.read_csv(file_name)

def create_duplicate_csv(df):
    duplicates = []
    for brand, group in df.groupby("brand"):
        if len(group) > 1:
            for index, row in group.iterrows():
                duplicates.append({"brand": brand, "unprocessed_index": index, "asin": row["asin"]})

    duplicates_df = pd.DataFrame(duplicates)
    return duplicates_df


def preprocess_data():
    # Remove na brands
    print("Preprocessing now...")
    for key, csv_file in constants.BRAND_PREFIX_TO_FILE_NAME.items():
        df = read_data(os.path.join(constants.DATA_DIRECTORY, csv_file))
        df_cleaned = df.dropna(subset=['brand'])
        df['brand'] = df_cleaned['brand'].apply(preprocess_brand)
        duplicates_df = create_duplicate_csv(df)
        duplicate_file = constants.DUPLICATE_FILE(key)
        duplicates_df.to_csv(duplicate_file, index=False)
        print(f"Duplicate entries saved to {duplicate_file}. Dropping duplicates in preprocessed file")
        df = df.drop_duplicates(subset="brand", keep="first")
        df = df.sort_values(by='brand')
        df = df.reset_index(drop=True)
        preprocessed_file = constants.PREPROCESSED_FILE(key)
        df.to_csv(preprocessed_file, index=False)
        print(f"preprocessed entries saved to {preprocessed_file}.")

def preprocess_brand(brand):
    # upper case
    # remove accents
    # remove hyphens
    # get rid of extra whitespaces
    try:
        brand = brand.upper()
        normalized_brand = unicodedata.normalize('NFD', brand)
        brand = ''.join(c for c in normalized_brand if not unicodedata.combining(c))
        brand = brand.replace("-", " ").strip()
        brand = ' '.join(brand.split())
        brand = re.sub(r'[\'"]', '', brand)
    except Exception as e:
        raise Exception(f"Unable to preprocess {brand}: {e}")
    return brand

def get_brand_id_map(df_name):
    if df_name == "iri":
        source_dataset = read_data("Nov 2024 BV product_brand_id sales - iri.csv")
    elif df_name == "gs1":
        source_dataset = read_data("Nov 2024 BV product_brand_id sales - gs1.csv")

    print("Creating brand_id to brand string map")
    ranked_df = read_data("Nov 2024 BV product_brand_id sales - sales rank.csv")

    # Merge the DataFrames on product_brand_id to match rows directly
    merged_df = pd.merge(
        source_dataset[['brand_id', 'brand']],
        ranked_df[['product_brand_id', 'product_symbol_id']],
        left_on='brand_id',
        right_on='product_brand_id',
        how='inner'
    )

    # Group by (product_symbol_id, product_brand_id) and aggregate brand strings into lists
    merged_df['brand'] = merged_df['brand'].astype(str).apply(preprocess_brand)
    brand_id_to_list_of_brand_strings = (
        merged_df.groupby(['product_symbol_id', 'product_brand_id'])['brand']
        .apply(lambda x: sorted(x.tolist()))
        .to_dict()
    )

    brand_id_to_list_of_brand_strings_str = {
        f"{int(key[0]) if pd.notna(key[0]) else 'None'},{int(key[1]) if pd.notna(key[1]) else 'None'}": value
        for key, value in brand_id_to_list_of_brand_strings.items()
    }

    with open(f"priority_map_{df_name}.json", "w") as json_file:
        json.dump(brand_id_to_list_of_brand_strings_str, json_file, indent=4)
    print(f"Priority map saved to 'priority_map_{df_name}.json'")

def get_preprocessed_data(preprocessed_data_cache, first_character):
    # if file not in cache, open the file
    if first_character not in preprocessed_data_cache:
        if first_character.isalpha() and first_character.isupper():
            preprocessed_file = constants.PREPROCESSED_FILE(first_character)
        else:
            preprocessed_file = constants.PREPROCESSED_FILE(constants.MISC_NAME)
        preprocessed_data_cache[first_character] = read_data(preprocessed_file)
    return preprocessed_data_cache[first_character]

def get_duplicate_data(duplicate_data_cache, first_character):
    # if file not in cache, open the file
    if first_character not in duplicate_data_cache:
        if first_character.isalpha() and first_character.isupper():
            duplicate_file = constants.DUPLICATE_FILE(first_character)
        else:
            duplicate_file = constants.DUPLICATE_FILE(constants.MISC_NAME)
        duplicate_data_cache[first_character] = read_data(duplicate_file)
    return duplicate_data_cache[first_character]

def match_brand_str_to_brand_id(df_name):
    # Get symbol_id, brand_id to [brand_strings] map
    # For every brand_string, look for an exact match in their corresponding {prefix}_preprocessed_data.csv
    mapped_brand_string_to_brand_id = []
    preprocessed_data_cache = {}

    def read_map(df_name):
        print("Reading brand_id to brand string map")
        with open(f"priority_map_{df_name}.json", "r") as json_file:
            brand_id_to_list_of_brand_strings = json.load(json_file)
        return brand_id_to_list_of_brand_strings


    brand_id_to_brand_strings_map = read_map(df_name)
    for key, list_of_brand_strings in brand_id_to_brand_strings_map.items():
        symbol_id, brand_id = key.split(",")
        for brand_string in list_of_brand_strings:
            first_character = brand_string[0]
            preprocessed_df = get_preprocessed_data(preprocessed_data_cache, first_character)

            result = extractOne(
                brand_string,
                preprocessed_df['brand'].tolist(),
                scorer=distance.JaroWinkler.distance,
                score_cutoff=0.0
            )

            if result:
                extracted_string, calculated_distance, extracted_index = result
                mapped_brand_string_to_brand_id.append(
                    (
                        brand_string,
                        brand_id,
                        symbol_id,
                        preprocessed_df.iloc[extracted_index]["asin"],
                        extracted_index,
                    )
                )

    output_df = pd.DataFrame(
        mapped_brand_string_to_brand_id,
        columns=["brand_string", "brand_id", "symbol_id", "asin", "preprocessed_index"]
    )

    print(f"skip {output_df[output_df['brand_string'] == 'NOBRAND'].shape[0]} entries with NOBRAND for now")
    output_df = output_df[output_df["brand_string"] != "NOBRAND"]
    output_df.to_csv(constants.MAPPED_BRANDS_WITH_INDICES_CSV, index=False)
    print(f"Mapped brand data saved to {constants.MAPPED_BRANDS_WITH_INDICES_CSV}")


def get_all_matches():
    mapped_df = read_data(constants.MAPPED_BRANDS_WITH_INDICES_CSV)
    cached_data = {}
    all_merged_dfs = []
    for index, row in mapped_df.iterrows():
        brand_string = row["brand_string"]
        first_character = brand_string[0]
        duplicate_df = get_duplicate_data(cached_data, first_character)
        filtered_df = duplicate_df[duplicate_df["brand"] == brand_string]
        merged_df = pd.merge(
            mapped_df,
            filtered_df,
            left_on="brand_string",
            right_on="brand",
            how="inner"
        )
        merged_df = merged_df.rename(columns={"asin_y": "asin"})
        merged_df = merged_df.drop(["asin_x", "brand", "preprocessed_index", "unprocessed_index"], axis=1, errors="ignore")
        all_merged_dfs.append(merged_df)
    return all_merged_dfs


def create_mapped_brands(list_of_dfs):
    final_mapped_df = pd.concat(list_of_dfs, ignore_index=True)
    final_mapped_df = final_mapped_df.sort_values(by="symbol_id")

    file_path = constants.DELIVERABLE_MAPPED_BRANDS_CSV
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        final_mapped_df = pd.concat([existing_df, final_mapped_df], ignore_index=True)
        final_mapped_df = final_mapped_df.sort_values(by="symbol_id")

    final_mapped_df.to_csv(file_path, index=False)
    print(f"{final_mapped_df.shape[0]} entries mapped. All mapped entries saved to {file_path}")


def clean_data():
    # mapped_brands_with_indices will be stale
    print(f"Cleaning duplicate and preprocessed data. {constants.MAPPED_BRANDS_WITH_INDICES_CSV} will be stale")
    mapped_df = read_data(constants.MAPPED_BRANDS_WITH_INDICES_CSV)
    for key, csv_file in constants.BRAND_PREFIX_TO_FILE_NAME.items():
        duplicate_file = constants.DUPLICATE_FILE(key)
        duplicate_df = read_data(duplicate_file)
        if key == "misc":
            brands_to_drop = mapped_df[~mapped_df["brand_string"].str.match(r'^[A-Z]', na=False)]["brand_string"].tolist()
        else:
            brands_to_drop = mapped_df[mapped_df["brand_string"].str.startswith(key, na=False)]["brand_string"].tolist()
        filtered_duplicate_df = duplicate_df[~duplicate_df["brand"].isin(brands_to_drop)]
        filtered_duplicate_df.to_csv(duplicate_file, index=False)
        preprocessed_file = constants.PREPROCESSED_FILE(key)
        preprocessed_df = read_data(preprocessed_file)
        indices_to_drop = mapped_df[mapped_df["brand_string"].str.startswith(key, na=False)]["preprocessed_index"].tolist()
        filtered_preprocessed_df = preprocessed_df.drop(indices_to_drop)
        filtered_preprocessed_df.to_csv(preprocessed_file, index=False)


def count_duplicates():
    print("Counting brands...")
    for key, csv_file in constants.BRAND_PREFIX_TO_FILE_NAME.items():
        duplicate_file = constants.DUPLICATE_FILE(key)
        duplicate_df = read_data(duplicate_file)
        result = (
            duplicate_df.groupby("brand")
            .size()
            .reset_index(name="count")
            .sort_values(by="count", ascending=False)
        )

        output_file = f"{key}_brand_count_index.csv"
        result.to_csv(os.path.join(constants.DEDUPLICATE_BRAND_DIR, output_file), index=False)
        print(f"Saved brand counts to {output_file}")


def distance_check(output_dir=constants.DEDUPLICATE_BRAND_DIR):
    preprocessed_df = preprocess_data(output_dir)
    names = preprocessed_df['brand'].tolist()
    matches = process.cdist(names, names, workers=4, scorer=distance.JaroWinkler.distance)
    import pdb
    pdb.set_trace()
    #
    # def calculate_jaro_winkler(s1, s2):
    #     return distance.JaroWinkler.distance(s1, s2, prefix_weight=0.20)
    #
    # all_pair_results = []
    # unique_brand_results = []
    # indices_to_drop = []
    #
    # # Iterate through names and calculate Jaro-Winkler distances
    # # we skip the last element because all other pairs have compared with the last element already
    #
    # for i, name1 in enumerate(names[:-1]):
    #     closest_dist = float('inf')
    #     for j, name2 in enumerate(names):
    #         if i < j:
    #             dist = calculate_jaro_winkler(name1, name2)
    #             all_pair_results.append((i, j, name1, name2, dist))
    #             if dist < closest_dist:
    #                 closest_dist = dist
    #
    #     # Brands that never get within the CLOSEST_DIST_LIMIT are filtered out as unique
    #     # Remove the brand from the df because they are unique
    #     if closest_dist >= CLOSEST_DIST_LIMIT:
    #         unique_brand_results.append((i, name1, closest_dist))
    #         indices_to_drop.append(i)
    names = preprocessed_df['brand'].tolist()
    unique_brand_results = []
    indices_to_drop = []

    for i, name1 in enumerate(names):
        closest_match = process.extractOne(
            name1, names, scorer=distance.JaroWinkler.distance, processor=None, score_cutoff=constants.CLOSEST_DIST_LIMIT
        )
        import pdb
        pdb.set_trace()
        if closest_match:
            closest_name, closest_dist, closest_idx = closest_match
            # Keep only if closest distance is below the threshold
            if closest_dist < constants.CLOSEST_DIST_LIMIT and closest_idx != i:
                continue  # This brand is not unique

        # If no close matches are found, mark as unique
        unique_brand_results.append((i, name1))
        indices_to_drop.append(i)

    # Drop unique brands from the DataFrame
    filtered_df = preprocessed_df.drop(index=indices_to_drop).reset_index(drop=True)
    # Convert unique results to DataFrame
    unique_brand_df = pd.DataFrame(unique_brand_results, columns=['Index', 'Brand'])

    filtered_df.to_csv('filtered_data.csv', index=False)
    unique_brand_df.to_csv('unique_brands.csv', index=False)
    # filtered_df = preprocessed_df.drop(index=indices_to_drop).reset_index(drop=True)
    #
    # all_pair_df = pd.DataFrame(all_pair_results, columns=['Index1', 'Index2', 'Name1', 'Name2', 'Distance'])
    # unique_brand_df = pd.DataFrame(unique_brand_results, columns=['Index', 'Name1', 'Closest Distance'])
    # all_pair_df.to_csv(os.path.join(output_dir, 'all_pair_results.csv'), index=False)
    # unique_brand_df.to_csv(os.path.join(output_dir, 'unique_brand_results.csv'), index=False)
    # filtered_df.to_csv(os.path.join(output_dir, 'filtered_data.csv'), index=False)


def load_into_bq():
    client = bigquery.Client()
    table_id = f"cei-data-science.webscrape.brand_string_to_brand_id_map"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrites the table
    )

    with open(constants.DELIVERABLE_MAPPED_BRANDS_CSV, "rb") as file:
        load_job = client.load_table_from_file(file, table_id, job_config=job_config)

    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")


def main():
    """Main entry point for the script."""
    # Argument parser setup
    parser = argparse.ArgumentParser(description="String matching using RecordLinkage or RapidFuzz")
    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="preprocess the data",
    )
    parser.add_argument(
        "--map_source_to_preprocessed_data",
        choices=["gs1", "iri"],
        help="Specify the source dataset. Accepted values are 'gs1' or 'iri'."
    )
    parser.add_argument(
        "--get_count",
        action="store_true",
        help="get a count of all the brands and store their indices csvs in alphabetical order",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="upload into BQ",
    )
    args = parser.parse_args()
    if args.preprocess:
        preprocess_data()
    elif args.map_source_to_preprocessed_data:
        # Create brand_id to brand string map from iri or gs1
        get_brand_id_map(args.map_source_to_preprocessed_data)
        match_brand_str_to_brand_id(args.map_source_to_preprocessed_data)
        # compares mapped_brands_with_indices and duplicates.csv and maps the duplicates
        create_mapped_brands(get_all_matches())
        # mapped_brands_with_indices will be stale
        # deletes duplicates and preprocess entries that are already mapped
        clean_data()
    elif args.get_count:
        count_duplicates()
    elif args.upload:
        load_into_bq()
    else:
        # distance_check()
        print("Invalid selection")


if __name__ == "__main__":
    main()
