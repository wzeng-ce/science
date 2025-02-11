from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import argparse

client = bigquery.Client()
def friskies():
    # Get SUM of all price paid by MONTH for EACH brand like FRISKIES
    query = """
    SELECT
        trans_date,
        product_brand,
        product_brand_id,
        asin,
        price_paid
    FROM `cei-data-science.helios_raw.helios_cpg_products`
    WHERE UPPER(product_brand) LIKE '%PURINA FRISKIES%'
    """

    df = client.query(query).to_dataframe()
    df['month'] = pd.to_datetime(df['month'])
    print(df.head())
    # Filter before grouping
    df_tagged = df[df['product_brand_id'].notnull()].groupby('month', as_index=False)['price_paid'].sum()
    df_untagged = df[df['product_brand_id'].isnull()].groupby('month', as_index=False)['price_paid'].sum()

    plt.figure(figsize=(12, 6))
    plt.plot(df_tagged['month'], df_tagged['price_paid'], linestyle="dashed", marker="o", label="Base Helios Tag")
    plt.plot(df_untagged['month'], df_untagged['price_paid'], linestyle="dashed", marker="o", label="Untagged Purina Friskies", alpha=0.7)

    plt.title("PURINA FRISKIES Sales Over Time", fontsize=14)
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("Total Sales ($)", fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(loc='upper left')
    plt.grid(True)

    df.sort_values(by='month').to_csv('friskies_sales_over_time.csv', index=False)
    plt.savefig("friskies_sales_over_time.png", dpi=300, bbox_inches='tight')

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
        "--friskies",
        action="store_true",
        help="preprocess the data",
    )
    parser.add_argument(
        "--natures_bounty",
        action="store_true",
        help="preprocess the data",
    )
    args = parser.parse_args()
    if args.friskies:
        friskies()
    if args.natures_bounty:
        nature_bounty()


if __name__ == "__main__":
    main()
