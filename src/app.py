
from fastapi import FastAPI

from src.etls import session
from src.model import SaleHistory

app = FastAPI()

@app.get("/health")
def health():
    return {'Hello World'}

@app.get("/show_raw")
def show_raw():
    sample = session.query(SaleHistory).limit(10).all()
    return {
        'sample_size': len(sample),
        'sample': [s.as_dict() for s in sample]
    }

@app.post('/sales/')
def add_sale(sale: dict):
    print('On add sale')

    if "id" in sale:
        # Is an update
        _id = sale["id"]
        sale_reg = session.query(SaleHistory).where(SaleHistory.id == _id).first()
        sale_reg.update_values(sale)
        session.add(sale_reg)
        print('Sale modification')

    else:
        print('New sale')
        # New reg
        sale_reg = SaleHistory(
            name=sale['name'],
            rank=sale['rank'],
            platform=sale['platform'],
            year=sale['year'],
            genre=sale['genre'],
            publisher=sale['publisher'],
            na_sales=sale.get('na_sales', 0),
            eu_sales=sale.get('eu_sales', 0),
            jp_sales=sale.get('jp_sales', 0),
            other_sales=sale.get('other_sales', 0),
            global_sales=sale.get('global_sales', 0),
        )

        session.add(sale_reg)
    session.commit()
    print(sale_reg)
    print('return', sale_reg.as_json())
    return sale_reg.as_json()

@app.get('/sales/{_id}')
def get_sale(_id):
    sale = session.query(SaleHistory).where(SaleHistory.id == _id).first()
    return sale.as_json()

@app.delete("/sales/{_id}")
def delete_sale(_id):
    session.query(SaleHistory).where(SaleHistory.id == _id).delete()
    session.commit()
    return {'deleted': _id}

@app.get("/search/{query}")
def search(query):
    import typesense

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': '8108',
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 10
    })

    search_params = {
        'q': query,
        'query_by': 'description',
        'filter_by': 'year :> 2000'
    }

    results = client.collections["games_sales"].documents.search(search_params)
    response = {
        r["document"]["description"] for r in results["hits"]
    }
    return response

@app.get("/")
def home():
    return {f'Go to /docs'}
