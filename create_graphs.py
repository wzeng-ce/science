from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os
import constants
import numpy as np

brand_id_to_name = {
    29631: "KIMBERLY-CLARK CORPORATION",
    29641: "THE PROCTER & GAMBLE CO",
    10010: "3M",
    24739: "PURINA",
    29649: "UNILEVER PLC",
    29628: "JOHNSON & JOHNSON",
    28504: "NESTLE TOLL HOUSE CAFE",
    30168: "MARS PETCARE",
    29610: "CHURCH & DWIGHT CO., INC.",
    13536: "LOREAL",
    29643: "RECKITT BENCKISER GROUP P",
    20136: "FRITO-LAY",
    29633: "NEWELL BRANDS INC.",
    29638: "THE QUAKER OATS COMPANY",
    24108: "MONSTER ENERGY",
    29655: "HALEON PLC",
    15833: "SHARKNINJA",
    30515: "CENTRAL GARDEN & PET",
    29629: "KEURIG DR PEPPER INC.",
    29609: "CELSIUS HOLDINGS, INC.",
    29616: "COLGATE-PALMOLIVE COMPA",
    29657: "THE KRAFT HEINZ COMPANY",
    30622: "ELANCO US INC",
    29050: "BEIERSDORF"
}

client = bigquery.Client()
def generate_before_after_graphs():
    before_mapping_query = """
SELECT 
    product_brand_id,
    DATE_TRUNC(trans_date, QUARTER) AS quarter,
    SUM(price_paid) AS total_price_paid
FROM `cei-data-science.helios_raw.helios_cleaned_product_brand`
WHERE product_brand_id IN (29631, 29641, 10010, 24739, 29649, 29628, 28504, 30168, 29610, 13536, 29643, 20136, 29633, 29638, 24108, 29655, 15833, 30515, 29629)
AND DATE(trans_date) > DATE('2020-01-01')
GROUP BY product_brand_id, quarter
ORDER BY product_brand_id, quarter;
    """

    after_mapping_query ="""
WITH brand_mapping AS (
    -- First attempt: Join on product_brand
    SELECT 
        ds1.product_brand_id,
        DATE_TRUNC(ds1.trans_date, QUARTER) AS quarter,
        ds1.price_paid,
        ds2.brand_id
    FROM `cei-data-science.helios_raw.helios_cleaned_product_brand` AS ds1
    LEFT JOIN (
        SELECT DISTINCT brand_string, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.product_brand_cleaned = ds2.brand_string

    UNION ALL

    -- Second attempt: Join on ASIN ONLY IF brand was NULL
    SELECT 
        ds1.product_brand_id,
        DATE_TRUNC(ds1.trans_date, QUARTER) AS quarter,
        ds1.price_paid,
        ds2.brand_id
    FROM `cei-data-science.helios_raw.helios_cleaned_product_brand` AS ds1
    LEFT JOIN (
        SELECT DISTINCT asin, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.asin = ds2.asin
    WHERE ds1.product_brand_cleaned IS NULL
)

SELECT 
    COALESCE(brand_id, product_brand_id) AS product_brand_id,
    quarter,
    SUM(price_paid) AS total_price_paid
FROM brand_mapping
WHERE 
    COALESCE(brand_id, product_brand_id) IN (29631, 29641, 10010, 24739, 29649, 29628, 28504, 30168, 29610, 13536, 29643, 20136, 29633, 29638, 24108, 29655, 15833, 30515, 29629)
    AND quarter > DATE('2020-01-01')
GROUP BY product_brand_id, quarter
ORDER BY product_brand_id, quarter;
"""
    after_mapping_df = client.query(after_mapping_query).to_dataframe()
    before_mapping_df = client.query(before_mapping_query).to_dataframe()
    after_mapping_df['quarter'] = pd.to_datetime(after_mapping_df['quarter'], errors='coerce')
    before_mapping_df['quarter'] = pd.to_datetime(before_mapping_df['quarter'], errors='coerce')

    assert set(before_mapping_df['product_brand_id'].unique()) == set(
        after_mapping_df['product_brand_id'].unique()), "The brand IDs must be identical for comparison."
    unique_brands = before_mapping_df['product_brand_id'].unique()

    # Create a separate bar chart for each product_brand_id showing both before and after mapping
    for brand_id in unique_brands:
        before_brand_df = before_mapping_df[before_mapping_df['product_brand_id'] == brand_id]
        after_brand_df = after_mapping_df[after_mapping_df['product_brand_id'] == brand_id]
        # Sort by quarter for proper alignment
        before_brand_df = before_brand_df.sort_values('quarter')
        after_brand_df = after_brand_df.sort_values('quarter')
        common_quarters = sorted(set(before_brand_df['quarter']) | set(after_brand_df['quarter']))
        quarter_labels = [q.to_period('Q').strftime('%YQ%q') for q in common_quarters]
        bar_width = 0.4
        x_indexes = np.arange(len(common_quarters))
        plt.figure(figsize=(10, 5))

        plt.bar(x_indexes - bar_width / 2,
                before_brand_df.set_index('quarter').reindex(common_quarters)['total_price_paid'].fillna(0),
                width=bar_width, color='blue', alpha=0.6, label="Before Mapping")
        plt.bar(x_indexes + bar_width / 2,
                after_brand_df.set_index('quarter').reindex(common_quarters)['total_price_paid'].fillna(0),
                width=bar_width, color='orange', alpha=0.6, label="After Mapping")
        plt.xlabel("Quarter")
        plt.ylabel("Total Price Paid")
        plt.title(f"Total Price Paid by Quarter for {brand_id_to_name[brand_id]} {brand_id}")
        plt.xticks(x_indexes, quarter_labels, rotation=45)
        plt.legend()

        file_path = os.path.join("graphs", f"{brand_id_to_name[brand_id]}.png")
        plt.savefig(file_path, dpi=300, bbox_inches='tight')


def purina():
    query = """
WITH brand_mapping AS (
    -- First attempt: Join on product_brand
    SELECT 
        ds1.product_brand_id,
        ds1.product_brand_cleaned AS product_brand,
        DATE_TRUNC(ds1.trans_date, QUARTER) AS quarter,
        ds1.price_paid,
        ds2.brand_id
    FROM `cei-data-science.helios_raw.helios_cleaned_product_brand` AS ds1
    LEFT JOIN (
        SELECT DISTINCT brand_string, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.product_brand_cleaned = ds2.brand_string

    UNION ALL

    -- Second attempt: Join on ASIN ONLY IF brand was NULL
    SELECT 
        ds1.product_brand_id,
        ds1.product_brand_cleaned AS product_brand,
        DATE_TRUNC(ds1.trans_date, QUARTER) AS quarter,
        ds1.price_paid,
        ds2.brand_id
    FROM `cei-data-science.helios_raw.helios_cleaned_product_brand` AS ds1
    LEFT JOIN (
        SELECT DISTINCT asin, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.asin = ds2.asin
    WHERE ds1.product_brand_cleaned IS NULL  -- Only match ASIN if brand was missing
)

SELECT 
    product_brand,
    quarter,
    SUM(price_paid) AS total_price_paid
FROM brand_mapping
WHERE 
    COALESCE(brand_id, product_brand_id) IN (24739)
    AND quarter > DATE('2020-01-01')
    AND quarter < DATE('2025-01-01')
GROUP BY product_brand, quarter
ORDER BY product_brand, quarter;
    """

    purina_df = client.query(query).to_dataframe()
    purina_df["quarter"] = pd.to_datetime(purina_df["quarter"])
    purina_graph_dir = os.path.join(constants.GRAPHS_DIR, "PURINA")

    purina_df_filtered = purina_df[purina_df["product_brand"].fillna("").str.startswith("PURINA")]
    unique_purina_brands_filtered = purina_df_filtered["product_brand"].unique()
    batch_size = 10

    # Generate multi-line plots in batches of 10 brands
    for i in range(0, len(unique_purina_brands_filtered), batch_size):
        batch_brands = unique_purina_brands_filtered[i:i + batch_size]
        batch_df = purina_df_filtered[purina_df_filtered["product_brand"].isin(batch_brands)]

        # Pivot DataFrame to reshape for multiple line plotting
        batch_pivot = batch_df.pivot(index="quarter", columns="product_brand", values="total_price_paid")
        plt.figure(figsize=(12, 6))
        for brand in batch_pivot.columns:
            plt.plot(batch_pivot.index, batch_pivot[brand], label=brand, marker='o')

        plt.xlabel("Quarter")
        plt.ylabel("Total Price Paid")
        plt.title(f"Total Sales by Quarter for PURINA Brands (Batch {i // batch_size + 1})")
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.xticks(rotation=45)
        plt.tight_layout()

        file_path = os.path.join(purina_graph_dir, f"purina_batch_{i // batch_size + 1}.png")
        plt.savefig(file_path, dpi=300, bbox_inches='tight')



def main():
    # Argument parser setup
    parser = argparse.ArgumentParser(description="String matching using RecordLinkage or RapidFuzz")
    parser.add_argument(
        "--before_after",
        action="store_true",
        help="generate coverage graphs before we applied brand_string to brand_id mappings",
    )
    parser.add_argument(
        "--purina",
        action="store_true",
        help="preprocess the data",
    )
    args = parser.parse_args()
    if args.before_after:
        generate_before_after_graphs()
    if args.purina:
        purina()


if __name__ == "__main__":
    main()
