import os
import constants
from google.cloud import bigquery

client = bigquery.Client()

def get_brands_url_by_letter(letter):
    sql_query = f"""
    CREATE TEMP FUNCTION is_amzn_media(asin STRING, byline STRING)
    RETURNS BOOL
    AS (
      SAFE_CAST(LEFT(asin, 9) AS INT) IS NOT NULL  -- ISBN 10 with or without check digit
      OR REGEXP_CONTAINS(byline, '(Format:)|(Rated:)|(Formato:)|(Clasificado:)')
      OR REGEXP_CONTAINS(byline, r'(^by)|(^.*? by)[A-Z]')  -- e.g., "byOscar Wilde". Capital to minimize false positives
      OR REGEXP_CONTAINS(byline, r'\\(.*?\\)')  -- Broad, so chance for false positives, but small amount selected
    );

    WITH amzn_media AS (
      SELECT 
        * 
      FROM `cei-data-science.webscrape.amzn_product_data`
      WHERE 
        is_amzn_media(url_asin, byline) OR is_amzn_media(parsed_asin, byline)
    ),

    amzn_media_asins AS (
      SELECT DISTINCT url_asin AS asin FROM amzn_media
      UNION DISTINCT
      SELECT DISTINCT parsed_asin AS asin FROM amzn_media
    ),

    amzn_media_product_ids AS (
      SELECT
        product_id
      FROM `data-enrichment-helios-prod.helios_pipeline_product_tagging.product_codes` a
      INNER JOIN amzn_media_asins b
      ON a.product_code = b.asin
    ),

    products_full_no_amzn_media AS (
      SELECT
        a.*,
        c.source
      FROM `data-enrichment-helios-prod.helios_pipeline_product_tagging.products_full` a
      LEFT JOIN amzn_media_product_ids b USING (product_id)
      INNER JOIN `cei-data-science.dev_cpd_products_dbt.product_sources` c USING (source_id)
      WHERE
        source_id <> 100091 OR b.product_id IS NULL
    )

    SELECT DISTINCT 
      a.brand,
      b.url
    FROM products_full_no_amzn_media a
    INNER JOIN (
      SELECT DISTINCT
        url,
        UPPER(brand_clean) AS brand
      FROM `cei-data-science.webscrape.amzn_product_data`
    ) b 
    ON UPPER(a.brand) = b.brand
    WHERE UPPER(a.brand) LIKE '{letter.upper()}%';
    """
    return sql_query

def get_brands_url_not_a_to_z():
    # Base SQL query for brands not starting with A-Z
    sql_query = """
    CREATE TEMP FUNCTION is_amzn_media(asin STRING, byline STRING)
    RETURNS BOOL
    AS (
      SAFE_CAST(LEFT(asin, 9) AS INT) IS NOT NULL  -- ISBN 10 with or without check digit
      OR REGEXP_CONTAINS(byline, '(Format:)|(Rated:)|(Formato:)|(Clasificado:)')
      OR REGEXP_CONTAINS(byline, r'(^by)|(^.*? by)[A-Z]')  -- e.g., "byOscar Wilde". Capital to minimize false positives
      OR REGEXP_CONTAINS(byline, r'\\(.*?\\)')  -- Broad, so chance for false positives, but small amount selected
    );

    WITH amzn_media AS (
      SELECT 
        * 
      FROM `cei-data-science.webscrape.amzn_product_data`
      WHERE 
        is_amzn_media(url_asin, byline) OR is_amzn_media(parsed_asin, byline)
    ),

    amzn_media_asins AS (
      SELECT DISTINCT url_asin AS asin FROM amzn_media
      UNION DISTINCT
      SELECT DISTINCT parsed_asin AS asin FROM amzn_media
    ),

    amzn_media_product_ids AS (
      SELECT
        product_id
      FROM `data-enrichment-helios-prod.helios_pipeline_product_tagging.product_codes` a
      INNER JOIN amzn_media_asins b
      ON a.product_code = b.asin
    ),

    products_full_no_amzn_media AS (
      SELECT
        a.*,
        c.source
      FROM `data-enrichment-helios-prod.helios_pipeline_product_tagging.products_full` a
      LEFT JOIN amzn_media_product_ids b USING (product_id)
      INNER JOIN `cei-data-science.dev_cpd_products_dbt.product_sources` c USING (source_id)
      WHERE
        source_id <> 100091 OR b.product_id IS NULL
    )

    SELECT DISTINCT 
      a.brand,
      b.url
    FROM products_full_no_amzn_media a
    INNER JOIN (
      SELECT DISTINCT
        url,
        UPPER(brand_clean) AS brand
      FROM `cei-data-science.webscrape.amzn_product_data`
    ) b 
    ON UPPER(a.brand) = b.brand
    WHERE NOT (REGEXP_CONTAINS(UPPER(a.brand), r'^[A-Z]'));
    """
    return sql_query


def get_brands_url_data():
    counter = 0
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        query = get_brands_url_by_letter(letter)
        query_job = client.query(query)
        df = query_job.to_dataframe()

        file_name = f"{letter}_brands_url.csv"
        file_path = os.path.join(constants.DATA_DIRECTORY, file_name)
        df.to_csv(file_path, index=False)
        counter += df.shape[0]
        print(f"Saved results for letter {letter} to {file_path}")

    query = get_brands_url_not_a_to_z()
    query_job = client.query(query)
    df = query_job.to_dataframe()
    counter += df.shape[0]
    file_name = "misc.csv"
    file_path = os.path.join(constants.DATA_DIRECTORY, file_name)
    df.to_csv(file_path, index=False)
    print(f"Saved results for non-A-Z brands to {file_path}")
    print(f"Obtained {counter} entries.")

get_brands_url_data()