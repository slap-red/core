import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel

class AuthData(BaseModel):
    merchant_id: str
    merchant_name: str
    access_id: str
    token: str
    api_url: str

Base = declarative_base()

class Bonus(Base):
    __tablename__ = 'bonuses'
    db_id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String)
    merchant_name = Column(String)
    id = Column(String, index=True)
    name = Column(String)
    amount = Column(Float)
    rollover = Column(Float)
    bonus_fixed = Column(Float)
    min_withdraw = Column(Float)
    max_withdraw = Column(Float)
    withdraw_to_bonus_ratio = Column(Float, nullable=True)
    min_topup = Column(Float)
    max_topup = Column(Float)
    transaction_type = Column(String)
    balance = Column(String)
    bonus = Column(String)
    bonus_random = Column(String)
    reset = Column(String)
    refer_link = Column(String)
    is_auto_claim = Column(Boolean, default=False)
    is_vip_only = Column(Boolean, default=False)
    has_loss_requirement = Column(Boolean, default=False)
    has_topup_requirement = Column(Boolean, default=False)
    loss_req_percent = Column(Float, nullable=True)
    loss_req_amount = Column(Float, nullable=True)
    topup_req_amount = Column(Float, nullable=True)
    claim_type = Column(String, nullable=True)
    raw_claim_config = Column(String)
    raw_claim_condition = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)