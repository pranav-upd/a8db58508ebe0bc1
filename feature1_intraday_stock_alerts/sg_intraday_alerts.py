import pytz
import traceback
from datetime import datetime, date
from sqlalchemy import Column, String, Float, Integer, DateTime, Text, Boolean, Date, Index, create_engine, insert, select
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from dotenv import load_dotenv
from dateutil import parser
import logging
import os

Base = declarative_base()
# Setup logging
load_dotenv()
IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(IST)

def today_ist():
    return now_ist().date()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(
    DATABASE_URL,
    echo=True,  # Set to False in production
    pool_size=10,
    max_overflow=5,
    pool_recycle=1800,
    pool_pre_ping=True,
)

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
SessionFactory = sessionmaker(bind=engine)
SessionScoped = scoped_session(SessionFactory)

def get_db_session():
    """Dependency to get a database session."""
    db = SessionScoped()
    try:
        yield db
    finally:
        db.close()


class SgIntradayStockAlerts(Base):
    __tablename__ = "sg_intraday_screener_signals"
    id = Column(Integer, nullable=True, primary_key=True, autoincrement=True)
    screener_run_id = Column(String(160), nullable=True)
    screener_date = Column(Date, nullable=True)
    screener_type = Column(String(100), nullable=True)
    screener = Column(String(100), nullable=True)
    stock_name = Column(String(100), nullable=True)
    #stock_type = Column(String, nullable=True)
    trade_type = Column(String(100), nullable=True)
    ltp = Column(String(100), nullable=True)
    #percent_change = Column(String(100), nullable=True)
    #vol_change = Column(String, nullable=True)
    #deviation_from_pivots = Column(String(100), nullable=True)
    todays_range = Column(String(100), nullable=True)
    #run_history = Column(String, nullable=True)
    #signal_count = Column(Integer, nullable=True)
    #tags = Column(String, nullable=True)
    #bullish_milestone_tags = Column(String, nullable=True)
    #bearish_milestone_tags = Column(String, nullable=True)
    screener_rank = Column(String(100), nullable=True)


class SgIntradayStockAlertsRepository:
    def insert(self, row):
        with engine.connect() as conn:
            result = conn.execute(
                insert(SgIntradayStockAlerts),
                [
                    {
                        "id": row[0],
                        "screener_run_id": row[1],
                        "screener_date": row[2],
                        "screener_type": row[3],
                        "screener": row[4],
                        "stock_name": row[5],
                        "trade_type": row[6],
                        "ltp": row[7],
                        "todays_range": row[8],
                        "screener_rank": row[9]
                    }
               ]
               )
            conn.commit()

def get_data_by_screener_run_id(screener_runid, logger):
    if not screener_runid:
        logger.error("Error: no runid provided.")
    else:
        screener_runid_data = select(SgIntradayStockAlerts).where(SgIntradayStockAlerts.screener_run_id==screener_runid)
        logger.info("Data successfully queried.")
        return screener_runid_data

def get_data_by_screener_date(screener_date, logger):
    if not screener_date:
        logger.error("Error: no runid provided.")
    else:
        screener_datetime_data = select(SgIntradayStockAlerts).where(SgIntradayStockAlerts.screener_date==screener_date)
        logger.info("Data successfully queried.")
        return screener_datetime_data


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    repo = SgIntradayStockAlertsRepository()
    print("All operations done. Check MySQL to verify.")
