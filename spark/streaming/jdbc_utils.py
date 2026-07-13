from config.settings import settings


def get_postgres_options(
    table_name: str,
    allow_uuid_cast: bool = False,
) -> dict:

    jdbc_url = (
        settings.POSTGRES_JDBC_URL_UUID
        if allow_uuid_cast
        else settings.POSTGRES_JDBC_URL
    )

    return {
        "url": jdbc_url,
        "dbtable": table_name,
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": settings.POSTGRES_DRIVER,
    }