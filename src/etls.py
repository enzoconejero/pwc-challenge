import os
from pathlib import Path

import kagglehub
import polars
from sqlalchemy import text
from sqlalchemy_orm.session import Session

from src.model import SaleHistory, RawDB, DimPlatform, DimYear, DimGame, FactSales
from src.utils import DBProvider, TypesenseProvider

def etl_raw():
    """Download raw data from Kaggle and load into relational DB"""
    kaggle_path = Path(kagglehub.dataset_download("gregorut/videogamesales"))
    csv_path = kaggle_path / os.listdir(kaggle_path)[0]

    # Load CSV into polars
    df = polars.read_csv(
        source=csv_path,
        null_values='N/A'
    )

    # Filter years
    df = df.filter(df['Year'].is_not_null()).rename(lambda x: x.lower())
    engine = DBProvider.raw.get_engine()
    session = Session(bind=engine)
    RawDB.metadata.create_all(engine)

    for row in df.rows(named=True):
        session.add(SaleHistory(**row))

    session.commit()

def etl_dw():
    """Load the Data Warehouse"""
    engine = DBProvider.dw.get_engine()
    session = Session(bind=engine)
    RawDB.metadata.drop_all(engine)
    RawDB.metadata.create_all(engine)
    with engine.connect() as conn:
        # Platform
        res = conn.execute(text("SELECT DISTINCT platform FROM history")).fetchall()
        platforms = [DimPlatform(name=name[0]) for name in res]
        session.add_all(platforms)
        session.commit()

        # Year
        res = conn.execute(text("SELECT DISTINCT year FROM history")).fetchall()
        years = [DimYear(year=year[0]) for year in res]
        session.add_all(years)
        session.commit()

        # Game
        res = conn.execute(text("SELECT DISTINCT name, publisher, genre, rank FROM history")).fetchall()
        print(res)
        games = [DimGame(name=game[0], publisher=game[1], genre=game[2], rank=game[3]) for game in res]
        session.add_all(games)
        session.commit()

        # # Facts
        res = conn.execute(text("""
            select dg.id game_id, dy.id year_id, dp.id platform_id, sum(h.global_sales) total_sales
            from history h
            join dim_game dg on h.name = dg.name
            join dim_year dy on h.year = dy.year
            join dim_platform dp on h.platform = dp.name
            group by 1, 2, 3
        """)).fetchall()
        facts = [FactSales(game_id=f[0], year_id=f[1], platform_id=f[2], total_sales=f[3]) for f in res]
        session.add_all(facts)
        session.commit()


def etl_vectordb():
    engine = DBProvider.dw.get_engine()
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

    with engine.connect() as conn:
        res = conn.execute(text("""
            select 
                dg.name, 
                dg.rank, 
                dg.publisher, 
                dy.year, 
                dp.name platform, 
                fs.total_sales, 
                dg.name || " (" || dy.year || ") - " || dp.name desc
            from fact_sales fs
            join dim_game dg on fs.game_id = dg.id
            join dim_year dy on fs.year_id = dy.id
            join dim_platform dp on fs.platform_id = dp.id
        """)).fetchall()

    docs = [
        {
            'game': game_sale_record[0],
            'publisher': game_sale_record[2],
            'rank': game_sale_record[1],
            'platform': game_sale_record[4],
            'year': game_sale_record[3],
            'total_sales': game_sale_record[5],
            'description': game_sale_record[6],
        }
        for game_sale_record in res
    ]

    typesense_client.collections["games_sales"].documents.import_(docs, {'action': 'upsert'})


def _testmain():
    etl_raw()
    etl_dw()
    etl_vectordb()

if __name__ == '__main__':
    _testmain()

