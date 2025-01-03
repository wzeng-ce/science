import os
import argparse
import constants
import pandas as pd

# Directory to store CSV files
RECORDLINKAGE_DIR = "recordlinkage"
RAPIDFUZZ_DIR = "rapidfuzz"
os.makedirs(RECORDLINKAGE_DIR, exist_ok=True)
os.makedirs(RAPIDFUZZ_DIR, exist_ok=True)

def read_data(file_name):
    return pd.read_csv(file_name)

def preprocess_data(output_dir):
    df = read_data(constants.BRANDS_CSV)
    df['brand'] = df['brand'].str.upper()
    df = df.sort_values(by='brand')
    df = df.reset_index(drop=True)
    df.to_csv(os.path.join(output_dir, 'preprocessed_data.csv'), index=False)
    return df

def load_preprocessed_data(output_dir):
    print("Preprocessing now...")
    return preprocess_data(output_dir)

def recordlinkage_package(output_dir=RECORDLINKAGE_DIR):
    import recordlinkage

    preprocessed_df = load_preprocessed_data(output_dir)

    # Indexation step
    indexer = recordlinkage.Index()
    indexer.full()
    candidate_links = indexer.index(preprocessed_df)

    compare_cl = recordlinkage.Compare()
    compare_cl.string(
        "brand", "brand", method="jarowinkler", threshold=0.80, label="brand_jarowinkler"
    )
    features = compare_cl.compute(candidate_links, preprocessed_df)

    report = features.sum(axis=1).value_counts().sort_index(ascending=False)

    matches = features[features['brand_jarowinkler'] == 1]
    matches = matches.copy()
    matches['preprocessed_values'] = matches.index.map(
        lambda idx: preprocessed_df.iloc[list(idx)]['brand'].tolist() if isinstance(idx, tuple) else preprocessed_df.iloc[idx]['brand']
    )

    selected_indices = set()
    for indx in matches.index:
        if isinstance(indx, tuple):
            first, second = indx
            selected_indices.add(first)
            selected_indices.add(second)
        else:
            selected_indices.add(indx)

    # Filter rows that are not in the selected indices
    not_selected = preprocessed_df.loc[~preprocessed_df.index.isin(selected_indices)]

    matches.to_csv(os.path.join(output_dir, 'jarowinkler_matches.csv'), index=True)
    not_selected.to_csv(os.path.join(output_dir, 'nonduplicates.csv'), index=True)

def rapidfuzz_package(output_dir=RAPIDFUZZ_DIR):
    from rapidfuzz.distance import JaroWinkler
    similarity = JaroWinkler.similarity("COLOR CLUB", "COLOURPOP SUPER SHOCK")
    print(similarity)
    preprocessed_df = load_preprocessed_data(output_dir)

def main():
    """Main entry point for the script."""
    # Argument parser setup
    parser = argparse.ArgumentParser(description="String matching using RecordLinkage or RapidFuzz")
    parser.add_argument(
        "--method",
        choices=["recordlinkage", "rapidfuzz"],
        required=True,
        help="Choose the string matching method: 'recordlinkage' or 'rapidfuzz'.",
    )
    args = parser.parse_args()

    # Execute the selected method
    if args.method == "recordlinkage":
        recordlinkage_package()
    elif args.method == "rapidfuzz":
        rapidfuzz_package()
    else:
        print("Invalid method selected. Please choose 'recordlinkage' or 'rapidfuzz'.")


if __name__ == "__main__":
    main()
