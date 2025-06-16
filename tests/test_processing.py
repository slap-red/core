# tests/test_processing.py
import pytest
from processing import process_claim_config
from models import BonusDB

def test_process_claim_config():
    bonus = Bonus(url="http://example.com", merchant_name="Test", id="1", name="Bonus", amount=100.0, rollover=30, is_active=True, is_completed=False, is_sticky=None, activated_date_time=None, expiry_date_time=None, claim_config='{"auto_claim": true, "group_id": 1, "wagering": {"requirement": "30x"}}')
    logger = logging.getLogger("test")
    result = process_claim_config(bonus, logger)
    assert result.is_auto_claim is True
    assert result.is_vip_only is True
    assert result.wagering_requirement == 30.0