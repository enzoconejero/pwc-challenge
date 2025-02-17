import os
from pathlib import Path
from sys import platform

import kagglehub
import polars
from sqlalchemy import create_engine, select, text
from sqlalchemy_orm.session import Session

from src.model import SaleHistory, RawDB, DimPlatform, DimYear, DimGame, FactSales
from src.utils import sqlite_db_path

engine = create_engine(f"sqlite:///{sqlite_db_path}", echo=True)
session = Session(bind=engine)

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
    RawDB.metadata.create_all(engine)

    for row in df.rows(named=True):
        session.add(SaleHistory(**row))

    session.commit()

def elt_dw():
    """Load the Data Warehouse"""
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


if __name__ == '__main__':
    RawDB.metadata.drop_all(engine)
    etl_raw()
    elt_dw()

