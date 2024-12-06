from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import BigInteger, Column, Integer, String, Float, ForeignKey, Table, Boolean


Base = declarative_base()

movie_categories = Table(
    'movie_categories', Base.metadata,
    Column('movie_id', Integer, ForeignKey('movies.id'), primary_key=True),
    Column('category_id', Integer, ForeignKey('categories.id'), primary_key=True)
)

class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    type = Column(String)
    released_year = Column(Integer)
    runtime_minutes = Column(Integer, nullable=True)
    imdb_rating = Column(Float, nullable=True)
    netflix_id = Column(Integer, nullable=True)
    cancelled = Column(Boolean, default=False)
    categories = relationship('Category', secondary=movie_categories, back_populates='movies')

class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    movies = relationship('Movie', secondary=movie_categories, back_populates='categories')

class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    stock_prices = relationship('StockPrice', back_populates='company')

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    id = Column(Integer, primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    trade_time = Column(String, nullable=False)
    trade_time_mills = Column(BigInteger, nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    company = relationship('Company', back_populates='stock_prices')