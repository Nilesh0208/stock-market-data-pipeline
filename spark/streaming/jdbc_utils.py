from typing import Optional

from config.settings import settings


def get_postgres_options(
    table_name: Optional[str] = None,
    query: Optional[str] = None,
) -> dict:
    """
    Build reusable PostgreSQL JDBC options.

    Provide either table_name or query, but not both.
    """

    if table_name and query:
        raise ValueError(
            "Provide either table_name or query, not both."
        )

    if not table_name and not query:
        raise ValueError(
            "Either table_name or query must be provided."
        )

    options = {
        "url": settings.POSTGRES_JDBC_URL,
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": settings.POSTGRES_DRIVER,
    }

    if table_name:
        options["dbtable"] = table_name

    if query:
        options["query"] = query

    return options