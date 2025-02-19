import os
from pathlib import Path

import kagglehub
import polars as pl
from sqlalchemy_orm.session import Session

from src.model import SaleHistory, RawDB, FactSales, DataWarehouse
from src.utils import DBProvider, TypesenseProvider

def etl_raw():
    """Download raw data from Kaggle and load into Raw DB"""
    kaggle_path = Path(kagglehub.dataset_download("gregorut/videogamesales"))
    csv_path = kaggle_path / os.listdir(kaggle_path)[0]

    # Load CSV into polars
    df = pl.read_csv(
        source=csv_path,
        null_values='N/A'
    )

    # Filter years
    df = df.filter(df['Year'].is_not_null()).rename(lambda x: x.lower())

    # Delete some duplicates (same game, year and platform with different rank)
    df = (
        df.group_by('name', 'year', 'platform')
        .agg(
            pl.col('eu_sales').sum().alias('eu_sales'),
            pl.col('jp_sales').sum().alias('jp_sales'),
            pl.col('other_sales').sum().alias('other_sales'),
            pl.col('global_sales').sum().alias('global_sales'),
            pl.col('rank').max().alias('rank')
        )
    )
    engine = DBProvider.raw.get_engine()
    session = Session(bind=engine)
    RawDB.metadata.drop_all(engine)
    RawDB.metadata.create_all(engine)

    for row in df.rows(named=True):
        session.add(SaleHistory(**row))

    session.commit()

def etl_dw():
    """Load the Star Schema into the Data Warehouse"""
    dw_engine = DBProvider.dw.get_engine()

    raw_session = DBProvider.raw.get_session()
    dw_session = DBProvider.dw.get_session()

    DataWarehouse.metadata.drop_all(dw_engine)
    DataWarehouse.metadata.create_all(dw_engine)

    history = raw_session.query(SaleHistory).all()

    for h in history:
        elems = h.as_fact()
        dw_session.add(elems)

    dw_session.commit()


def etl_vectordb():
    """Creates the search schema in Typesense and syncs with DataWarehouse"""
    typesense_client = TypesenseProvider.get_client()

    if 'games_sales' not in [c['name'] for c in typesense_client.collections.retrieve()]:
        print('Create schema')
        game_sales_schema = {
            'name': 'games_sales',
            'fields': [
                {'name': 'game', 'type': 'string'},
                {'name': 'publisher', 'type': 'string'},
                {'name': 'rank', 'type': 'int32'},
                {'name': 'platform', 'type': 'string'},
                {'name': 'year', 'type': 'int32', 'facet': True},
                {'name': 'total_sales', 'type': 'float'},
                {'name': 'description', 'type': 'string'}
            ],
            'default_sorting_field': 'rank'
        }

        typesense_client.collections.create(game_sales_schema)

    update_search_engine()


def update_dw(game_id=None):
    """Sync the DW with the data in Raw. Commonly used when a new SaleHistory is added
    > If no ID is provided then update all the facts
    """
    raw_session = DBProvider.raw.get_session()
    dw_session = DBProvider.dw.get_session()
    query = raw_session.query(SaleHistory)
    if game_id:
        query = query.where(SaleHistory.id == game_id)
    history = query.all()

    facts = [h.as_fact() for h in history]
    dw_session.add_all(facts)
    dw_session.commit()

    # Update search_engine
    if game_id:
        update_search_engine(facts[0].game_id)
    else:
        update_search_engine()

def update_search_engine(game_id=None):
    """If no ID is provided then update all the facts"""
    session = DBProvider.dw.get_session()
    query = session.query(FactSales)
    if game_id:
        query = query.where(FactSales.game_id == game_id)
    facts = query.all()
    docs = [f.as_search_doc() for f in facts]
    TypesenseProvider.get_client().collections["games_sales"].documents.import_(docs, {'action': 'upsert'})

def _testmain():
    etl_raw()
    etl_dw()
    etl_vectordb()

if __name__ == '__main__':
    _testmain()

