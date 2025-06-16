import json
import logging
from typing import Any, List, Dict

from models import Bonus

def _parse_float(value: Any) -> float:
    if value is None: return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def _create_and_map_bonus(data: Dict[str, Any], url: str, merchant_name: str) -> Bonus:
    b = Bonus()
    b.url = url
    b.merchant_name = merchant_name
    b.id = str(data.get("id", ""))
    b.name = str(data.get("name", ""))
    b.amount = _parse_float(data.get("amount"))
    b.rollover = _parse_float(data.get("rollover"))
    b.bonus_fixed = _parse_float(data.get("bonusFixed"))
    b.min_withdraw = _parse_float(data.get("minWithdraw"))
    b.max_withdraw = _parse_float(data.get("maxWithdraw"))
    b.min_topup = _parse_float(data.get("minTopup"))
    b.max_topup = _parse_float(data.get("maxTopup"))
    b.withdraw_to_bonus_ratio = b.min_withdraw / b.bonus_fixed if b.bonus_fixed != 0 else None
    b.transaction_type = str(data.get("transactionType", ""))
    b.balance = str(data.get("balance", ""))
    b.bonus = str(data.get("bonus", ""))
    b.bonus_random = str(data.get("bonusRandom", ""))
    b.reset = str(data.get("reset", ""))
    b.refer_link = str(data.get("referLink", ""))
    b.raw_claim_config = data.get("claimConfig", "")
    b.raw_claim_condition = data.get("claimCondition", "")
    return b

def _parse_claim_config(b: Bonus, logger: logging.Logger) -> Bonus:
    raw_config = b.raw_claim_config
    if not isinstance(raw_config, str) or not raw_config.startswith('['):
        return b
    try:
        config_list = json.loads(raw_config)
        if not isinstance(config_list, list): return b
        for item in config_list:
            if not isinstance(item, str): continue
            iu = item.upper()
            if "AUTO_CLAIM" in iu: b.is_auto_claim = True
            if "VIP" in iu: b.is_vip_only = True
            if "DEPOSIT" in iu: b.claim_type = "DEPOSIT"
            if "RESCUE" in iu: b.claim_type = "RESCUE"
            if "REBATE" in iu: b.claim_type = "REBATE"
            if "LOSS" in iu:
                b.has_loss_requirement = True
                parts = item.split('_')
                if len(parts) > 1:
                    val_str = parts[-1].replace('%', '')
                    if '%' in parts[-1]: b.loss_req_percent = _parse_float(val_str)
                    else: b.loss_req_amount = _parse_float(val_str)
            if "TOPUP" in iu:
                b.has_topup_requirement = True
                parts = item.split('_')
                if len(parts) > 1: b.topup_req_amount = _parse_float(p[-1])
    except Exception as e:
        logger.debug("claim_config_parse_fail", extra={"id": b.id, "err": str(e)})
    return b

def process_bonuses(bonuses_json: List[Dict[str, Any]], url: str, merchant_name: str, logger: logging.Logger) -> List[Bonus]:
    """
    Takes a list of raw bonus dictionaries from the API and returns a list
    of fully processed Bonus objects.
    """
    processed_list = []
    for bonus_data in bonuses_json:
        if not isinstance(bonus_data, dict):
            continue
        bonus_obj = _create_and_map_bonus(bonus_data, url, merchant_name)
        fully_processed_bonus = _parse_claim_config(bonus_obj, logger)
        processed_list.append(fully_processed_bonus)
    return processed_list