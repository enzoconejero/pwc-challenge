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

