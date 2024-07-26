from google.cloud import bigquery

def create_bigquery_tables():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Define your dataset name (assuming the dataset is already created)
    dataset_name = 'crypto'
    dataset_ref = client.dataset(dataset_name)

    # Table creation function
    def create_table(table_id, schema, table_description=""):
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table.description = table_description
        table = client.create_table(table)  # API request
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    # Define schemas for each table
    top_coins_info_schema = [
        bigquery.SchemaField("cg_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("image_url", "STRING", mode="NULLABLE"),
    ]

    coin_data_schema = [
        bigquery.SchemaField("cg_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("volume", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("market_cap", "FLOAT", mode="REQUIRED"),
    ]

    pchange_schema = [
        bigquery.SchemaField("cg_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("pct_change_1_day", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_7_days", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_14_days", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_30_days", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_90_days", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_180_days", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pct_change_365_days", "FLOAT", mode="NULLABLE"),
    ]

    pchange_long_schema = [
        bigquery.SchemaField("cg_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("period", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("perc", "FLOAT", mode="NULLABLE"),
    ]

    # Create tables
    create_table("top_coins_info", top_coins_info_schema)
    create_table("coin_data", coin_data_schema)
    create_table("pchange", pchange_schema)
    create_table("pchange_long", pchange_long_schema)

if __name__ == "__main__":
    create_bigquery_tables()