import os
import argparse
import constants
import pandas as pd
import json
from google.cloud import bigquery
from rapidfuzz import process, distance
from rapidfuzz.process import extractOne, extract
import unicodedata
import re
from tabulate import tabulate
import pprint
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
        df = df.drop_duplicates(subset="brand", keep="first")
        df = df.sort_values(by='brand')
        df = df.reset_index(drop=True)
        preprocessed_file = constants.PREPROCESSED_FILE(key)
        df.to_csv(preprocessed_file, index=False)
    print(f"Dropping duplicates in preprocessed file")
    print(f"Duplicate and preprocessed entries saved to {constants.DATA_DIRECTORY}.")

def preprocess_brand(brand):
    try:
        brand = brand.upper()
        # Remove accents
        normalized_brand = unicodedata.normalize('NFKD', brand)
        brand = ''.join(c for c in normalized_brand if not unicodedata.combining(c))

        # Replace hyphens and plus signs with placeholders
        brand = brand.replace("-", "_hyphen_")
        brand = brand.replace("+", "_plus_")

        # Remove single and double quotes
        brand = re.sub(r'[\'"]', '', brand)
        # Collapse multiple spaces into one
        brand = ' '.join(brand.split())
        # Trim extra spaces
        brand = brand.strip()
    except Exception as e:
        raise Exception(f"Unable to preprocess {brand}: {e}")
    return brand

def get_brand_id_map(df_name):
    if df_name == "iri":
        source_dataset = read_data(os.path.join(constants.MAPPINGS_DIRECTORY, "Nov 2024 BV product_brand_id sales - iri.csv"))
    elif df_name == "gs1":
        source_dataset = read_data(os.path.join(constants.MAPPINGS_DIRECTORY, "Nov 2024 BV product_brand_id sales - gs1.csv"))

    print("Creating brand_id to brand string map")
    ranked_df = read_data(os.path.join(constants.MAPPINGS_DIRECTORY, "Nov 2024 BV product_brand_id sales - sales rank.csv"))

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
    file_path = constants.get_brand_clusters_file(df_name)
    with open(file_path, "w") as json_file:
        json.dump(brand_id_to_list_of_brand_strings_str, json_file, indent=4)
    print(f"clusters saved to {file_path}")

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

def get_original_data(original_data_cache, first_character):
    if first_character not in original_data_cache:
        if first_character.isalpha() and first_character.isupper():
            original_file = constants.BRAND_PREFIX_TO_FILE_NAME[first_character]
        else:
            original_file = constants.BRAND_PREFIX_TO_FILE_NAME[constants.MISC_NAME]
        csv_file = os.path.join(constants.DATA_DIRECTORY, original_file)
        original_data_cache[first_character] = read_data(csv_file)
    return original_data_cache[first_character]

def read_cluster_json(df_name):
    print("Reading brand_id to brand string map")
    file_path = constants.get_brand_clusters_file(df_name)
    with open(file_path, "r") as json_file:
        brand_id_to_list_of_brand_strings = json.load(json_file)
    return brand_id_to_list_of_brand_strings

def load_manual_clusters():
    """Load manually defined brand clusters from JSON."""
    if os.path.exists(constants.MANUAL_CLUSTERS_JSON):
        with open(constants.MANUAL_CLUSTERS_JSON, "r") as file:
            return json.load(file)
    return {}

def merge_json_maps(primary_map, secondary_map):
    """
    Merges two JSON dictionaries.
    - If a key exists in both, it appends values from secondary_map to primary_map.
    - If a key exists only in secondary_map, it is added to primary_map.
    """
    merged_map = primary_map.copy()  # Start with the primary map

    for key, values in secondary_map.items():
        if key in merged_map:
            merged_map[key] = sorted(merged_map[key] + values)
        else:
            merged_map[key] = values
    return merged_map

def match_brand_str_to_brand_id(df_name):
    # Assumes that clusters are already built
    # Get symbol_id, brand_id to [brand_strings] map
    # For every brand_string, look for an exact match in their corresponding {prefix}_preprocessed_data.csv
    mapped_brand_string_to_brand_id = []
    preprocessed_data_cache = {}
    # combine manual mapping and data source mapping
    manual_map = load_manual_clusters()
    brand_id_to_brand_strings_map = merge_json_maps(read_cluster_json(df_name), manual_map)
    # find the exact match to brand_string
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
                product_asin = preprocessed_df.iloc[extracted_index]["asin"]
                mapped_brand_string_to_brand_id.append(
                    (
                        brand_string,
                        brand_id,
                        symbol_id,
                        product_asin,
                    ))

    output_df = pd.DataFrame(
        mapped_brand_string_to_brand_id,
        columns=["brand_string", "brand_id", "symbol_id", "asin"]
    )

    print(f"skip {output_df[output_df['brand_string'] == 'NOBRAND'].shape[0]} entries with NOBRAND for now")
    output_df = output_df[output_df["brand_string"] != "NOBRAND"]
    output_df.to_csv(constants.BRANDS_THAT_MATCH_CSV, index=False)
    print(f"Mapped brand data saved to {constants.BRANDS_THAT_MATCH_CSV}")

def find_original_brand_strings(original_df, asin):
    """
    brand string has been preprocessed
    get the original brand string before the preprocessing
    returns a list because there may be more than 1 brand string associated to an asin
    """
    return original_df[original_df['asin'] == asin]['brand'].tolist()


def get_all_matches():
    """"
    Get brand_string matches from brands_that_match
    Go through duplicates.csv to get all duplicate brand_strings
    """
    mapped_df = read_data(constants.BRANDS_THAT_MATCH_CSV)
    output_rows = []
    duplicate_cached_data = {}
    original_data_cache = {}
    # Group by first character for batch processing
    grouped_mapped = mapped_df.groupby(mapped_df["brand_string"].str[0])
    for first_character, group in grouped_mapped:
        duplicate_df = get_duplicate_data(duplicate_cached_data, first_character)
        # Get original brand string, before preprocessing
        original_df = get_original_data(original_data_cache, first_character)
        for _, row in group.iterrows():
            brand_string = row["brand_string"]
            # Filter duplicate_df for matches with the current brand_string
            filtered_df = duplicate_df[duplicate_df["brand"] == brand_string]
            if filtered_df.empty:
                # No matches in duplicates, so save all entries of single ASIN
                # Unfortunately, there can be multiple brand strings per ASIN
                list_of_original_brand_strings_per_asin = find_original_brand_strings(original_df, row['asin'])
                output_rows.extend(
                    {
                        "brand_string": original_brand_string,
                        "brand_id": row["brand_id"],
                        "symbol_id": row["symbol_id"],
                        "asin": row["asin"]
                    }
                    for original_brand_string in list_of_original_brand_strings_per_asin
                )
                continue

            all_new_rows = []
            for _, filtered_row in filtered_df.iterrows():
                # Get all original brand strings for the matched ASIN
                list_of_original_brand_strings_per_asin = find_original_brand_strings(original_df, filtered_row['asin'])
                new_rows = [
                    {
                        "brand_string": original_brand_string,
                        "brand_id": row["brand_id"],
                        "symbol_id": row["symbol_id"],
                        "asin": filtered_row["asin"]
                    }
                    for original_brand_string in list_of_original_brand_strings_per_asin
                ]
                if new_rows:
                    all_new_rows.extend(new_rows)
                else:
                    print(f"Warning: No brand strings found for ASIN {filtered_row['asin']}")
            output_rows.extend(all_new_rows)
    return pd.DataFrame(output_rows)


def write_mapped_brands(final_mapped_df):
    final_mapped_df = pd.concat(final_mapped_df, ignore_index=True)
    final_mapped_df = final_mapped_df.sort_values(by="symbol_id")
    final_mapped_df.to_csv(constants.DELIVERABLE_MAPPED_BRANDS_CSV, index=False)
    print(f"{final_mapped_df.shape[0]} entries mapped. All mapped entries saved to {constants.DELIVERABLE_MAPPED_BRANDS_CSV}")


def clean_data():
    # mapped_brands_with_indices will be stale
    print(f"Cleaning duplicate and preprocessed data. {constants.BRANDS_THAT_MATCH_CSV} will be stale")
    mapped_df = read_data(constants.BRANDS_THAT_MATCH_CSV)
    for key, csv_file in constants.BRAND_PREFIX_TO_FILE_NAME.items():
        duplicate_file = constants.DUPLICATE_FILE(key)
        duplicate_df = read_data(duplicate_file)
        # get all brands in duplicates that have the same NAME (these will have different ASIN)
        if key == constants.MISC_NAME:
            brands_to_drop = mapped_df[mapped_df["brand_string"].str.match(r'^\d', na=False)]['brand_string'].tolist()
        else:
            brands_to_drop = mapped_df[mapped_df["brand_string"].str.startswith(key, na=False)]["brand_string"].tolist()

        dropped_brands_duplicate_df = duplicate_df[~duplicate_df["brand"].isin(brands_to_drop)]
        dropped_brands_duplicate_df.to_csv(duplicate_file, index=False)

        preprocessed_file = constants.PREPROCESSED_FILE(key)
        preprocessed_df = read_data(preprocessed_file)
        # Find all the singular entries (no duplicates) and delete using by using their asin
        if key == constants.MISC_NAME:
            asins_to_drop = mapped_df[mapped_df["brand_string"].str.match(r'^\d', na=False)]['asin'].tolist()
        else:
            asins_to_drop = mapped_df[mapped_df["brand_string"].str.startswith(key, na=False)]['asin'].tolist()
        dropped_asins_preprocessed_df = preprocessed_df[~preprocessed_df['asin'].isin(asins_to_drop)]
        dropped_asins_preprocessed_df.to_csv(preprocessed_file, index=False)


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
    print(f"Saved brand counts to brand_count_index.csv")


def find_nearest_match(df_name):
    # Get symbol_id, brand_id to [brand_strings] map
    # For every brand_string, look for the top 10 closest match within a 0.15 distance
    # in their corresponding {prefix}_preprocessed_data.csv
    mapped_brand_string_to_brand_id = []
    preprocessed_data_cache = {}
    print("Getting nearest matches")
    # combine manual mapping and data source mapping
    manual_map = load_manual_clusters()
    brand_id_to_brand_strings_map = merge_json_maps(read_cluster_json(df_name), manual_map)
    for key, list_of_brand_strings in brand_id_to_brand_strings_map.items():
        symbol_id, brand_id = key.split(",")
        for iri_brand_string in list_of_brand_strings:
            first_character = iri_brand_string[0]
            preprocessed_df = get_preprocessed_data(preprocessed_data_cache, first_character)
            result = extract(
                iri_brand_string,
                preprocessed_df['brand'].tolist(),
                scorer=distance.JaroWinkler.distance,
                score_cutoff=0.15,
                limit=10
            )
            if result:
                for tup in result:
                    extracted_string, jarowinkler_distance, extracted_index = tup
                    mapped_brand_string_to_brand_id.append(
                        (
                            iri_brand_string,
                            extracted_string,
                            jarowinkler_distance,
                            brand_id,
                            symbol_id,
                            preprocessed_df.iloc[extracted_index]["asin"],
                        )
                    )


    output_df = pd.DataFrame(
        mapped_brand_string_to_brand_id,
        columns=["iri_brand_string", "found_brand_string", "jarowinkler_distance", "brand_id", "symbol_id", "asin"]
    )
    output_df = output_df.sort_values(by="jarowinkler_distance", ascending=True)
    output_df.to_csv(constants.CLOSEST_BRANDS_CSV, index=False)



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

def create_report():
    def get_csv_row_counts(path):
        """
        Counts the number of rows in all CSV files within a given directory.
        """
        row_counts = {}
        if os.path.isfile(path) and path.endswith(".csv"):
            try:
                df = pd.read_csv(path)
                row_counts[os.path.basename(path)] = len(df)
            except Exception as e:
                print(f"Error reading {path}: {e}")

        elif os.path.isdir(path):
            for file in os.listdir(path):
                if file.endswith(".csv"):
                    file_path = os.path.join(path, file)
                    try:
                        df = pd.read_csv(file_path)
                        row_counts[file] = len(df)
                    except Exception as e:
                        print(f"Error reading {file}: {e}")
        else:
            print(f"Invalid path: {path} (not a CSV file or directory)")
        return row_counts

    total_entries = get_csv_row_counts(constants.DATA_DIRECTORY)
    mapped_entries = get_csv_row_counts(constants.DELIVERABLE_MAPPED_BRANDS_CSV)
    total_products = sum(total_entries.values())
    coverage_percentage = (list(mapped_entries.values())[0] / total_products) * 100 if total_products > 0 else 0
    client = bigquery.Client()

    pre_tagging_mapped_entries_query = """
    SELECT 
        product_brand_id AS brand_id,
        STRING_AGG(DISTINCT product_brand, ', ') AS pre_product_brands,
        COUNT(*) AS pre_total_entries,
        SUM(price_paid) AS pre_total_price_paid
    FROM `cei-data-science.helios_raw.helios_cpg_products`
    WHERE 
        product_brand_id IS NOT NULL
        AND product_brand IS NOT NULL
    GROUP BY product_brand_id;
    """

    query_job = client.query(pre_tagging_mapped_entries_query)
    pre_tagging_df = query_job.to_dataframe()

    post_tagging_mapped_entries_query = """
        WITH coverage_either_price_paid AS (
            SELECT 
                ds2.brand_id,
                STRING_AGG(DISTINCT ds1.product_brand, ', ') AS post_product_brands,
                COUNT(*) AS post_total_entries,
                SUM(ds1.price_paid) AS post_total_price_paid
            FROM 
                `cei-data-science.helios_raw.helios_cpg_products` AS ds1
            LEFT JOIN (
                SELECT DISTINCT 
                    asin, 
                    brand_id
                FROM 
                    `cei-data-science.webscrape.brand_string_to_brand_id_map`
            ) AS ds2
            ON ds1.asin = ds2.asin
            WHERE 
                ds2.brand_id IS NOT NULL
                OR (ds1.product_brand_id IS NOT NULL AND ds1.product_brand IS NOT NULL)
            GROUP BY ds2.brand_id
        )
        SELECT * FROM coverage_either_price_paid;
    """
    query_job = client.query(post_tagging_mapped_entries_query)
    post_tagging_df = query_job.to_dataframe()
    # calculate percent increase
    merged_df = pre_tagging_df.merge(post_tagging_df, on="brand_id")
    merged_df["entries_percent_increase"] = (((merged_df["post_total_entries"] - merged_df["pre_total_entries"]) /
                                             merged_df["pre_total_entries"]) * 100).round(2)
    merged_df["price_percent_increase"] = (((merged_df["post_total_price_paid"] - merged_df["pre_total_price_paid"]) /
                                           merged_df["pre_total_price_paid"]) * 100).round(2)
    # combine product_brands and concatenate
    merged_df["pre_product_brands"] = merged_df["pre_product_brands"].apply(
        lambda x: x.split(", ") if isinstance(x, str) else [])
    merged_df["post_product_brands"] = merged_df["post_product_brands"].apply(
        lambda x: x.split(", ") if isinstance(x, str) else [])
    merged_df["product_brands"] = merged_df.apply(
        lambda row: list(set(row["pre_product_brands"] + row["post_product_brands"])), axis=1
    )
    merged_df = merged_df.drop(columns=["pre_product_brands", "post_product_brands"])

    merged_df = merged_df.sort_values(by="post_total_entries", ascending=False)
    markdown_table = merged_df.head(100).to_markdown(index=False)
    md_content = f"""# Tagging Coverage Report
## Summary Statistics
- **Total CSVs Processed**: {len(total_entries)}
- **Total Products Processed**: {total_products}
- **Total Products Mapped**: {list(mapped_entries.values())[0]}
- **POSEIDON Mapping Coverage (%)**: {round(coverage_percentage, 2)}%

## Detailed Breakdown
{markdown_table}
    """
    with open(constants.HELIOS_COVERAGE_MD, "w") as f:
        f.write(md_content)
    print(f"Docs saved to : {constants.HELIOS_COVERAGE_MD}")


def main():
    parser = argparse.ArgumentParser(description="brand str to brand id")
    parser.add_argument(
        "--preprocess",
        action="store_true",
        help="preprocess the data",
    )
    parser.add_argument(
        "--map_data",
        action="store_true",
        help="Map brand_ids to their brands"
    )
    parser.add_argument(
        "--get_count",
        action="store_true",
        help="get a count of all the brands and store their indices csvs in alphabetical order",
    )
    parser.add_argument(
        "--get_closest_match",
        action="store_true",
        help="get closest match with jaro winkler (weighted prefix)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="upload into BQ",
    )
    parser.add_argument(
        "--create_clusters_from_sources",
        action="store_true",
        help="Map brand_ids to their brands"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="create a report"
    )
    args = parser.parse_args()
    if args.preprocess:
        preprocess_data()
    elif args.create_clusters_from_sources:
        # Create brand_id to brand string map from iri or gs1
        for data_source in constants.DATA_SOURCES:
            get_brand_id_map(data_source)

    elif args.map_data:
        list_of_mapped_dfs = []
        for data_source in constants.DATA_SOURCES:
            match_brand_str_to_brand_id(data_source)
            # compares mapped_brands_with_indices and duplicates.csv and maps the duplicates
            list_of_mapped_dfs.append(get_all_matches())
            # mapped_brands_with_indices will be stale
            # deletes duplicates and preprocess entries that are already mapped
            clean_data()
        write_mapped_brands(list_of_mapped_dfs)
        count_duplicates()
    elif args.get_closest_match:
        find_nearest_match('iri')
    elif args.upload:
        load_into_bq()
    elif args.stats:
        create_report()
    else:
        # distance_check()
        print("Invalid selection")


if __name__ == "__main__":
    main()
