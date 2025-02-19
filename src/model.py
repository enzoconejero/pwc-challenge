import json
from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class RawDB(DeclarativeBase):
    pass

class SaleHistory(RawDB):
    __tablename__ = 'history'
    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)
    rank: Mapped[int] = mapped_column(Integer)
    platform: Mapped[str] = mapped_column(String)
    year: Mapped[int]= mapped_column(Integer)
    genre: Mapped[str] = mapped_column(String)
    publisher: Mapped[str] = mapped_column(String)
    na_sales: Mapped[int] = mapped_column(Integer)
    eu_sales: Mapped[int] = mapped_column(Integer)
    jp_sales: Mapped[int] = mapped_column(Integer)
    other_sales: Mapped[int] = mapped_column(Integer)
    global_sales: Mapped[int] = mapped_column(Integer)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name}, {self.platform})'

    def as_json(self):
        param = self.__dict__.copy()
        param.pop('_sa_instance_state')
        return param

    def as_json_string(self):
        return json.dumps(self.as_json())

    def select_statement(self, with_id=False):
        if with_id:
            return f'SELECT * FROM {self.__tablename__} where id = {self.id}'
        else:
            return (f'SELECT * FROM {self.__tablename__} where '
                    f'name = {self.name} '
                    f'AND year = {self.year} '
                    f'AND platform = {self.platform}')

    def update_values(self, values: dict):
        alterable_attrs = self.__dict__.copy()
        alterable_attrs.pop('id')
        alterable_attrs.pop('_sa_instance_state')

        for attr, value in values.items():
            if attr in alterable_attrs and getattr(self, attr) != value:
                setattr(self, attr, value)

        return self

class FactSales(RawDB):
    __tablename__ = 'fact_sales'
    game_id = mapped_column(ForeignKey('dim_game.id'), primary_key=True)
    platform_id = mapped_column(ForeignKey('dim_platform.id'), primary_key=True)
    year_id = mapped_column(ForeignKey('dim_year.id'), primary_key=True)
    total_sales = Column(Integer, nullable=False)

class DimGame(RawDB):
    __tablename__ = 'dim_game'
    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)
    genre: Mapped[str] = mapped_column(String)
    publisher: Mapped[str] = mapped_column(String)
    rank: Mapped[int] = mapped_column(Integer)

class DimPlatform(RawDB):
    __tablename__ = 'dim_platform'
    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)

    def __repr__(self):
        return f'Platform({self.name})'

class DimYear(RawDB):
    __tablename__ = 'dim_year'
    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer)
