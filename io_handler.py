import os
import csv
import logging
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from models import Bonus, Base

def load_urls(file_path: str, logger: logging.Logger) -> List[str]:
    """Loads URLs from a text file."""
    if not os.path.exists(file_path):
        logger.error(f"URL file not found: {file_path}")
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    except Exception as e:
        logger.error(f"Failed to read URL file: {e}")
        return []

def write_bonuses_to_csv(bonuses: List[Bonus], csv_path: str, logger: logging.Logger):
    """
    Correctly serializes detached SQLAlchemy objects and writes them to a CSV file.
    """
    if not bonuses:
        logger.info("No bonuses to write to CSV.")
        return

    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # Get headers from the SQLAlchemy model's table columns
        field_names = [c.name for c in Bonus.__table__.columns]
        file_exists = os.path.isfile(csv_path) and os.path.getsize(csv_path) > 0
        
        # Create a list of clean dictionaries from the detached objects
        rows_to_write = []
        for bonus in bonuses:
            row_dict = bonus.__dict__.copy()
            # Remove the internal SQLAlchemy state key before writing
            row_dict.pop('_sa_instance_state', None)
            rows_to_write.append(row_dict)

        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=field_names, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows_to_write)
        
        logger.info(f"Successfully wrote {len(bonuses)} bonuses to {csv_path}")

    except Exception as e:
        logger.error(f"CSV write failed: {e}")

def write_bonuses_to_db(bonuses: List[Bonus], db_url: str, logger: logging.Logger):
    """Writes a list of Bonus model objects to the database."""
    if not bonuses:
        logger.info("No bonuses to write to database.")
        return
    
    try:
        engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        logger.error(f"DB engine/session creation failed: {e}")
        return

    try:
        # Use merge to prevent errors with objects that might already exist
        # in the session from a different context.
        for bonus in bonuses:
            session.merge(bonus)
        session.commit()
        logger.info(f"Wrote {len(bonuses)} bonuses to database.")
    except SQLAlchemyError as e:
        logger.error(f"DB write failed: {e}")
        session.rollback()
    finally:
        session.close()

# Dummy cache functions for compatibility
def load_run_cache(logger: logging.Logger): return {}
def save_run_cache(data: dict, logger: logging.Logger): pass