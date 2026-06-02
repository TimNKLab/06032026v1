"""ETL transform stage: Polars data-cleaning functions.

Each submodule reads raw Parquet, applies type casting / null handling /
business-rule enrichment, and writes clean Parquet to the data lake.

No Celery decorators.  No framework dependencies beyond stdlib + Polars.
"""
