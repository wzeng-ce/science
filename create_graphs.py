from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import os
import constants
import numpy as np
import matplotlib.ticker as mticker

# Creating a dictionary to represent the brand_name to product_brand_id mapping
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
def generate_before_mapping_graphs():
    before_mapping_query = """
SELECT 
    product_brand_id,
    DATE_TRUNC(trans_date, QUARTER) AS quarter,
    SUM(price_paid) AS total_price_paid
FROM `cei-data-science.helios_raw.amzn_item_all_20250115`
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
    FROM `cei-data-science.helios_raw.amzn_item_all_20250115` AS ds1
    LEFT JOIN (
        SELECT DISTINCT brand_string, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.product_brand = ds2.brand_string

    UNION ALL

    -- Second attempt: Join on ASIN ONLY IF brand was NULL
    SELECT 
        ds1.product_brand_id,
        DATE_TRUNC(ds1.trans_date, QUARTER) AS quarter,
        ds1.price_paid,
        ds2.brand_id
    FROM `cei-data-science.helios_raw.amzn_item_all_20250115` AS ds1
    LEFT JOIN (
        SELECT DISTINCT asin, brand_id 
        FROM `cei-data-science.webscrape.brand_string_to_brand_id_map`
    ) AS ds2
    ON ds1.asin = ds2.asin
    WHERE ds1.product_brand IS NULL  -- Only match ASIN if brand was missing
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


def nature_bounty():
    query = """
    SELECT
        trans_date,
        product_brand,
        product_brand_id,
        asin,
        price_paid
    FROM `cei-data-science.helios_raw.helios_cpg_products`
    WHERE
        product_brand LIKE '%NATURE%BOUNTY%';
    """

    df = client.query(query).to_dataframe()
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    # we just care about the monthly
    df['year_month'] = df['trans_date'].dt.to_period('M').astype(str)

    print(df.head())
    df_tagged = df[df['product_brand_id'].notnull()].groupby('year_month', as_index=False)['price_paid'].sum()
    df_untagged = df[df['product_brand_id'].isnull()].groupby('year_month', as_index=False)['price_paid'].sum()

    plt.figure(figsize=(12, 6))
    plt.plot(df_tagged['year_month'], df_tagged['price_paid'], linestyle="dashed", marker="o", label="Base Helios Tag")
    plt.plot(df_untagged['year_month'], df_untagged['price_paid'], linestyle="dashed", marker="o", label="Untagged Natures Bounty", alpha=0.7)

    plt.title("Natures Bounty Sales Over Time", fontsize=14)
    plt.xlabel("year_month", fontsize=12)
    plt.ylabel("Total Sales ($)", fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(loc='upper left')
    plt.grid(True)

    df.sort_values(by='year_month').to_csv('natures_bounty_sales_over_time.csv', index=False)
    plt.savefig("natures_bounty_sales_over_time.png", dpi=300, bbox_inches='tight')




def main():
    # Argument parser setup
    parser = argparse.ArgumentParser(description="String matching using RecordLinkage or RapidFuzz")
    parser.add_argument(
        "--generate_before",
        action="store_true",
        help="generate coverage graphs before we applied brand_string to brand_id mappings",
    )
    parser.add_argument(
        "--natures_bounty",
        action="store_true",
        help="preprocess the data",
    )
    args = parser.parse_args()
    if args.generate_before:
        generate_before_mapping_graphs()
    if args.natures_bounty:
        nature_bounty()


if __name__ == "__main__":
    main()
