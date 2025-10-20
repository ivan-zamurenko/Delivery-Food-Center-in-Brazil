from sqlalchemy import create_engine


def get_engine(db_config):
    """Create and return a SQLAlchemy engine."""
    connection_string = (
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_string, echo=True)
