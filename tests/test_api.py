from fastapi.testclient import TestClient
from src.app import app
from src.model import SaleHistory

client = TestClient(app)
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

def test_dummy():
    res = client.get('/')
    assert res.status_code == 200

def test_get_sale():
    res = client.get('/sales/1')
    assert res.status_code == 200

def test_crud_sale():
    # print(sale_example.as_json())
    # Create
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


