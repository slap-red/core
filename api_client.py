import aiohttp, logging, asyncio
from typing import Optional, List, Dict, Any
from models import AuthData

async def get_bonuses(auth: AuthData, session: aiohttp.ClientSession, logger: logging.Logger) -> Optional[List[Dict[str, Any]]]:
    payload = {"module": "/users/syncData", "merchantId": auth.merchant_id, "accessId": auth.access_id, "accessToken": auth.token}
    try:
        async with session.post(auth.api_url, data=payload, proxy=None, timeout=15, ssl=False) as response:
            response.raise_for_status()
            res_json = await response.json()
            if res_json.get("status") != "SUCCESS":
                logger.warning("bonus_api_status_fail", extra={"url": auth.api_url, "response": res_json})
                return None
            bonus_l = res_json.get("data", {}).get("bonus", [])
            promo_l = res_json.get("data", {}).get("promotions", [])
            return (bonus_l if isinstance(bonus_l, list) else []) + (promo_l if isinstance(promo_l, list) else [])
    except Exception as e:
        logger.error("bonus_fetch_fail", extra={"url": auth.api_url, "err": str(e)})
        return None