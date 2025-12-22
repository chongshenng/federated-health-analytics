"""fed_analytics_app: A Flower / Federated Analytics app."""

import pandas as pd
from sqlalchemy import create_engine


def query_database(  # pylint: disable=too-many-arguments, too-many-positional-arguments
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_password: str,
    table_name: str,
    selected_features: list[str],
) -> pd.DataFrame:
    """Query PostgreSQL database and return selected features as DataFrame.

    Args:
        db_host: Database host address
        db_port: Database port number
        db_name: Database name
        db_user: Database user
        db_password: Database password
        table_name: Name of the table to query
        selected_features: List of column names to select

    Returns:
        DataFrame containing the selected features
    """
    # Create database connection
    engine = create_engine(
        f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # Build query to select only the requested features
    columns = ", ".join(selected_features)
    query_str = f"SELECT {columns} FROM {table_name}"

    # Execute query and load into DataFrame
    df = pd.read_sql(query_str, engine)
    engine.dispose()

    return df
