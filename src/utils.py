from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

proj_path = Path(__file__).parents[1]


class DBConnector:
    """Abstraction of the relational DB Connection
    To set up a testing DB:
    >>>> class TestDB(DBConnector):
    >>>>    url: str = f"sqlite:///{proj_path}/test_raw.db"
    >>>>
    >>>> DBProvider.raw = TestDB()
    Accept all type of connection (even non-local) as long as the method get_engine is overwritted
    """
    url: str
    def __init__(self):
        self._engine = None
        self._session = None

    def get_engine(self):
        if not self._engine:
            self._engine = create_engine(self.url, echo=True)
        return self._engine

    def get_session(self):
        if not self._session:
            self._session = Session(self.get_engine())
        return self._session


class LocalSQLiteRaw(DBConnector):
    url: str = f"sqlite:///{proj_path}/raw.db"


class LocalSQLiteDW(DBConnector):
    url: str = f"sqlite:///{proj_path}/dw.db"


class _DBProvider:
    """Singleton provider of the DBs"""
    def __init__(self):
        self.raw = LocalSQLiteRaw()
        self.dw = LocalSQLiteDW()


class _TypesenseProvider:
    """Singleton provider of the TypesenseClient
    TODO: start service from this class
    """
    def __init__(self):
        self._client = None

    def get_client(self):
        if not self._client:
            import typesense
            self._client = typesense.Client({
                'nodes': [{
                    'host': 'localhost',
                    'port': '8108',
                    'protocol': 'http'
                }],
                'api_key': 'xyz',
                'connection_timeout_seconds': 10
            })
        return self._client


DBProvider = _DBProvider()
TypesenseProvider = _TypesenseProvider()
