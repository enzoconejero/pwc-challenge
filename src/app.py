
from fastapi import FastAPI

from src.etls import etl_raw, etl_dw, update_dw
from src.model import SaleHistory
from src.utils import DBProvider, TypesenseProvider

app = FastAPI()

@app.get("/health")
def health():
    """Check if it's running"""
    return {'Hello World'}

@app.post('/load_raw')
def load_raw():
    """Trigger the ETL of the RAW data"""
    etl_raw()
    return {'Raw loaded'}

@app.post('/load_datawarehouse')
def load_raw():
    """Trigger the ETL of the DataWarehouse"""
    etl_dw()
    return {'Data Warehouse loaded'}

@app.get("/show_raw")
def show_raw():
    """Show a sample of the RawData"""
    sample = DBProvider.raw.get_session().query(SaleHistory).limit(10).all()
    return {
        'sample_size': len(sample),
        'sample': [s.as_json() for s in sample]
    }

@app.post('/sales/')
def add_sale(sale: dict):
    """Add or Update a sale"""
    print('On add sale')
    db_session = DBProvider.raw.get_session()

    if "id" in sale:
        # Is an update
        _id = sale["id"]
        sale_reg = db_session.query(SaleHistory).where(SaleHistory.id == _id).first()
        sale_reg.update_values(sale)
        db_session.add(sale_reg)
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

        db_session.add(sale_reg)

    db_session.commit()
    update_dw(sale_reg.id)
    # print(sale_reg)
    # print('return', sale_reg.as_json())
    return sale_reg.as_json()

@app.get('/sales/{_id}')
def get_sale(_id):
    """Return an existent sale"""
    sale = DBProvider.raw.get_session().query(SaleHistory).where(SaleHistory.id == _id).first()
    return sale.as_json()

@app.delete("/sales/{_id}")
def delete_sale(_id):
    """Removes an existent sale"""
    session = DBProvider.raw.get_session()
    session.query(SaleHistory).where(SaleHistory.id == _id).delete()
    session.commit()
    return {'deleted': _id}

@app.get("/search/{query}")
def search(query):
    """Search through Typesense"""
    search_params = {
        'q': query,
        'query_by': 'description',
        'filter_by': 'year :> 2000'
    }
    results = TypesenseProvider.get_client().collections["games_sales"].documents.search(search_params)
    response = {r["document"]["description"] for r in results["hits"]}
    return response

@app.post("/search/sync/")
def search_sync():
    """Syncs the DataWarehouse with Typesense engine"""
    update_dw()
    return {'DW and SearchEngine synced'}

@app.get("/")
def home():
    return {f'Go to /docs'}
