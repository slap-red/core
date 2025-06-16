import asyncio
import aiohttp
import logging
import configparser
import random
import time
import collections
from typing import List, Deque
from urllib.parse import urlparse, urlunparse

import io_handler, ui, processing, auth, models, config, logger_config, api_client

async def process_url(url: str, app_config: configparser.ConfigParser, logger: logging.Logger, session: aiohttp.ClientSession, request_tracker: Deque[float]):
    """
    Processes a single URL and returns a tuple with all necessary results.
    """
    cleaned_url = urlunparse(urlparse(url)._replace(path="", params="", query="", fragment=""))
    
    auth_data = await auth.get_auth(cleaned_url, app_config, logger, session, request_tracker)
    if not auth_data:
        return [], cleaned_url, False, 0

    min_delay = app_config.getfloat('scraper', 'min_request_delay', fallback=1.0)
    max_delay = app_config.getfloat('scraper', 'max_request_delay', fallback=3.0)
    await asyncio.sleep(random.uniform(min_delay, max_delay))

    bonuses_json = await api_client.get_bonuses(auth_data, session, logger)
    if bonuses_json is None:
        return [], cleaned_url, True, 0

    processed_bonuses = processing.process_bonuses(bonuses_json, cleaned_url, auth_data.merchant_name, logger)
    bonus_count = len(processed_bonuses)
    logger.info(f"OK: {cleaned_url} - Found {bonus_count} bonuses.")
    return processed_bonuses, cleaned_url, True, bonus_count

async def main():
    app_config = config.get_config()
    logger = logger_config.setup_logger(app_config)
    
    urls = io_handler.load_urls(app_config.get('scraper', 'url_list_path'), logger)
    
    ui_handler = ui.UIHandler()
    ui_handler.set_total_urls(len(urls))
    
    if not urls:
        return
        
    request_tracker: Deque[float] = collections.deque(maxlen=200)
    total_bonuses_found = 0
    failed_url_count = 0
    
    # Pre-get output settings
    db_enabled = app_config.getboolean('output', 'enable_db_output')
    csv_enabled = app_config.getboolean('output', 'enable_csv_output')
    db_url = app_config.get('output', 'db_connection_string')
    csv_path = app_config.get('output', 'csv_output_path')
    
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                bonuses_list, cleaned_url, success, bonuses_found = await process_url(url.strip(), app_config, logger, session, request_tracker)
                
                if not success:
                    failed_url_count += 1
                
                if bonuses_list:
                    total_bonuses_found += len(bonuses_list)
                    
                    # --- Real-time Output Logic ---
                    if db_enabled:
                        io_handler.write_bonuses_to_db(bonuses_list, db_url, logger)
                    if csv_enabled:
                        io_handler.write_bonuses_to_csv(bonuses_list, csv_path, logger)
                
                ui_handler.update(cleaned_url, success, bonuses_found, request_tracker)

            except Exception as e:
                failed_url_count += 1
                cleaned_url = urlunparse(urlparse(url.strip())._replace(path="", params="", query="", fragment=""))
                ui_handler.update(cleaned_url, False, 0, request_tracker)
                logger.error(f"A task failed for URL {url.strip()}: {e}", extra={"err":str(e)})

    # Final summary printout
    ui_handler.final(total_bonuses_found, failed_url_count)
    logger.info(f"Scraping complete.", extra={"total_bonuses_found": total_bonuses_found, "failed_urls": failed_url_count})

if __name__ == "__main__":
    asyncio.run(main())