import shutil

import pytest
from fastapi.testclient import TestClient
from src.app import app
from src.model import SaleHistory, RawDB, FactSales, DimGame, DataWarehouse
from src.utils import proj_path, DBConnector, DBProvider


class TestRawDB(DBConnector):
    url: str = f"sqlite:///{proj_path}/test_raw.db"

class TestDW(DBConnector):
    url: str = f"sqlite:///{proj_path}/test_dw.db"

@pytest.fixture(autouse=TestRawDB)
def setup():
    shutil.rmtree(proj_path / 'test_raw.db', ignore_errors=True)
    shutil.rmtree(proj_path / 'test_dw.db', ignore_errors=True)
    DBProvider.raw = TestRawDB()
    DBProvider.dw = TestDW()
    RawDB.metadata.create_all(DBProvider.raw.get_engine())
    DataWarehouse.metadata.create_all(DBProvider.dw.get_engine())

client = TestClient(app)

shutil.rmtree(proj_path / 'test_raw.db', ignore_errors=True)
shutil.rmtree(proj_path / 'test_dw.db', ignore_errors=True)

def test_dummy():
    res = client.get('/')
    assert res.status_code == 200

def test_crud_sale():
    sale_example = SaleHistory(
        name='Minecraft 2',
        rank=1,
        platform='PC',
        year=2030,
        genre='Sandbox',
        publisher='Mojang',
        na_sales=1.0,
        eu_sales=1.0,
        jp_sales=1.0,
        other_sales=1.0,
        global_sales=4.0,
    )

    res = client.post('/sales/', json=sale_example.as_json())
    print(res.json())
    assert res.status_code == 200
    _id = res.json()['id']
    sale_example.id = _id

    # Read
    res = client.get(f'/sales/{_id}')
    assert res.status_code == 200
    assert res.json() == sale_example.as_json()

    # Update
    param = {'id': _id, 'year': 2031}
    sale_example.year = 2031
    res = client.post(f'/sales/', json=param)
    assert res.status_code == 200
    assert res.json() == sale_example.as_json()

    # Delete
    res = client.delete(f'/sales/{_id}')
    assert res.status_code == 200
    assert res.json() == {'deleted': str(_id)}


def test_update_search_engine():
    name = 'This is a very specific game title'
    sale_example = SaleHistory(
        name=name,
        rank=1000,
        platform='PC',
        year=2030,
        genre='Sandbox',
        publisher='EnzoGames',
        na_sales=1.0,
        eu_sales=1.0,
        jp_sales=1.0,
        other_sales=1.0,
        global_sales=4.0,
    )
    res = client.post('/sales/', json=sale_example.as_json())
    assert res.status_code == 200
    _id = res.json()['id']

    # Check exists in Raw
    assert DBProvider.raw.get_session().query(SaleHistory).where(SaleHistory.id == _id).first().name == name

    dw_session = DBProvider.dw.get_session()
    # Check exists in DW
    dim_game = dw_session.query(DimGame).where(DimGame.name == name).first()
    assert dim_game
    fact = dw_session.query(FactSales).where(FactSales.game_id == dim_game.id).first()
    assert fact

    res = client.get('/search/This is a very specific game title')
    assert res.json()[0] == f'{name} (2030) - PC'

