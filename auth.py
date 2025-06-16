# auth.py 
import configparser, logging, re, time, asyncio
from typing import Optional, Deque
from pydantic import ValidationError
import aiohttp
from models import AuthData

async def get_auth(url: str, config: configparser.ConfigParser, logger: logging.Logger, session: aiohttp.ClientSession, request_tracker: Deque[float]) -> Optional[AuthData]:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    request_tracker.append(time.time())
    
    try:
        async with session.get(url, headers=headers, proxy=None, timeout=15, ssl=False) as response:
            response.raise_for_status()
            html = await response.text()
            if not html:
                logger.warning("auth_html_empty", extra={"url": url})
                return None
            match = re.search(r'var MERCHANTID = (\d+);', html, re.IGNORECASE)
            if not match:
                logger.warning("auth_merch_id_fail", extra={"url": url})
                return None
            merchant_id = match.group(1)
            merchant_name_match = re.search(r'var MERCHANTNAME = ["\'](.*?)["\'];', html, re.IGNORECASE)
            merchant_name = merchant_name_match.group(1) if merchant_name_match else ""
    except Exception as e:
        logger.error("auth_html_fetch_fail", extra={"url": url, "err": str(e)})
        return None

    api_url = f"{url}/api/v1/index.php"
    payload = {"module": "/users/login", "mobile": config.get('auth', 'username'), "password": config.get('auth', 'password'), "merchantId": merchant_id}
    
    try:
        async with session.post(api_url, data=payload, headers=headers, proxy=None, timeout=15, ssl=False) as response:
            response.raise_for_status()
            res_json = await response.json()
            if res_json.get("status") != "SUCCESS":
                logger.warning("auth_api_status_fail", extra={"url": api_url, "response": res_json})
                return None
            auth_payload = {
                "merchant_id": merchant_id, "merchant_name": merchant_name,
                "access_id": res_json.get("data", {}).get("id"),
                "token": res_json.get("data", {}).get("token"),
                "api_url": api_url
            }
            return AuthData.model_validate(auth_payload)
    except Exception as e:
        logger.error("auth_api_request_fail", extra={"url": api_url, "err": str(e)})
        return None