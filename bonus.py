# bonus.py
#
# Slap Red Scraper v0.4.0
#
# Main executable for scraping and processing bonus data from websites.
# This script handles configuration, logging, authentication, data scraping,
# and console UI presentation.

import csv
import os
import sys
import time
import requests
import logging
import json
import math
import re
import traceback
from dataclasses import dataclass, field, fields
from typing import List, Set, Tuple, Union, Optional, Dict, Any
from datetime import datetime
import configparser
from urllib.parse import urlparse, urlunparse
import argparse

# --- ABBREVIATIONS USED ---
# (Refer to Abbreviation Dictionary.md)

# --- DATA MODELS ---
# Defines the core data structures used throughout the application.
# Using dataclasses provides type hints, auto-generated __init__, __repr__, etc.
# This section would be a candidate for its own file, e.g., `src/models.py`.

@dataclass
class AuthData:
    """Stores authentication credentials obtained after a successful login."""
    merchant_id: str
    merchant_name: str
    access_id: str
    token: str
    api_url: str

@dataclass
class Bonus:
    """
    Represents a single bonus item, including both raw data from the API
    and newly parsed categorical fields. The fields of this class directly
    correspond to the columns in the output CSV file.
    """
    # Core Identification Fields
    url: str
    merchant_name: str
    id: str
    name: str

    # Standard Bonus Attribute Fields
    amount: float
    rollover: float
    bonus_fixed: float
    min_withdraw: float
    max_withdraw: float
    withdraw_to_bonus_ratio: Optional[float]
    min_topup: float
    max_topup: float
    
    # Raw String/Info Fields
    transaction_type: str
    balance: str
    bonus: str
    bonus_random: str
    reset: str
    refer_link: str

    # Categorical Fields Parsed from claimConfig
    is_auto_claim: bool = False
    is_vip_only: bool = False
    has_loss_requirement: bool = False
    has_topup_requirement: bool = False
    loss_req_percent: Optional[float] = None
    loss_req_amount: Optional[float] = None
    topup_req_amount: Optional[float] = None
    claim_type: Optional[str] = None # e.g., 'RESCUE', 'REBATE', 'DEPOSIT'

    # Raw claimConfig fields for auditing/debugging
    raw_claim_config: str = field(repr=False, default="")
    raw_claim_condition: str = field(repr=False, default="")


# --- CONFIGURATION ---
# Handles loading and validation of settings from the config.ini file.
# This isolates configuration logic from the rest of the application.
# This section would be a candidate for its own file, e.g., `src/config.py`.

@dataclass
class Credentials:
    """Stores user credentials."""
    mobile: str
    password: str

@dataclass
class Settings:
    """Stores general operational settings."""
    url_file: str
    downline_enabled: bool

@dataclass
class LoggingConfig:
    """Stores logging configuration settings."""
    log_file: str
    console: bool
    detail: str  # "LESS", "MORE", "MAX"

@dataclass
class AppConfig:
    """A container for all configuration objects."""
    credentials: Credentials
    settings: Settings
    logging: LoggingConfig

class ConfigLoader:
    """Reads, validates, and provides configuration from a .ini file."""
    def __init__(self, path: str = "config.ini"):
        if not os.path.exists(path):
            print(f"FATAL: Config file not found: {path}. Please create it.", file=sys.stderr)
            sys.exit(1)
        self.config = configparser.ConfigParser()
        self.config.read(path)
        self._config_file_path = path

    def load(self) -> AppConfig:
        """Loads configuration from the .ini file into an AppConfig object."""
        try:
            detail_setting = self.config["logging"].get("detail", "MORE").upper()
            if detail_setting not in ["LESS", "MORE", "MAX"]:
                print(f"Warning: Invalid detail level '{detail_setting}' in '{self._config_file_path}'. Defaulting to MORE.", file=sys.stderr)
                detail_setting = "MORE"

            app_cfg = AppConfig(
                credentials=Credentials(
                    mobile=self.config["credentials"]["mobile"],
                    password=self.config["credentials"]["password"]
                ),
                settings=Settings(
                    url_file=self.config["settings"]["file"],
                    downline_enabled=self.config["settings"].getboolean("downline", fallback=False)
                ),
                logging=LoggingConfig(
                    log_file=self.config["logging"]["log_file"],
                    console=self.config["logging"].getboolean("console", fallback=True),
                    detail=detail_setting
                )
            )
            return app_cfg
        except KeyError as e:
            print(f"FATAL: Config error in '{self._config_file_path}': Missing key {e}", file=sys.stderr)
            sys.exit(1)
        except ValueError as e:
            print(f"FATAL: Config error in '{self._config_file_path}': Invalid value for a boolean field. {e}", file=sys.stderr)
            sys.exit(1)


# --- LOGGER ---
# Handles all logging for the application. It is configured to output structured
# JSON logs for machine readability and optionally formatted text logs for human review.
# This section would be a candidate for its own file, e.g., `src/logger.py`.

class JsonFormatter(logging.Formatter):
    """Custom formatter to output log records as JSON strings."""
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.module, # Will show 'bonus' or 'downline'
            "method": record.funcName,
            "event_msg": record.getMessage(), # Main message (was 'event' string)
            "details": record.details_data if hasattr(record, 'details_data') else {}
        }
        return json.dumps(log_entry)

class Logger:
    """
    Manages logging configuration and dispatch.
    The verbosity is controlled by standard Python logging levels, which are
    mapped from the 'detail' setting in config.ini.
    """
    DETAIL_TO_LEVEL_MAP = {
        "LESS": logging.WARNING,
        "MORE": logging.INFO,
        "MAX": logging.DEBUG,
    }

    def __init__(self, log_file: str, detail_setting: str, console: bool, formatted_log_path: Optional[str] = None):
        self.py_logger = logging.getLogger("ScraperAppLogger")
        if self.py_logger.hasHandlers():
            self.py_logger.handlers.clear()

        effective_log_level = self.DETAIL_TO_LEVEL_MAP.get(detail_setting.upper(), logging.INFO)
        self.py_logger.setLevel(effective_log_level)

        # 1. JSON File Handler (always active)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        try:
            json_file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            json_file_handler.setFormatter(JsonFormatter())
            self.py_logger.addHandler(json_file_handler)
        except Exception as e:
            print(f"CRITICAL: Could not initialize JSON file logger at {log_file}: {e}", file=sys.stderr)

        # 2. Console Handler (optional)
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            # Use a filter to customize console output, e.g., truncating long details
            class ConsoleDetailsFilter(logging.Filter):
                def filter(self, record: logging.LogRecord):
                    record.event_display = record.getMessage()
                    details = record.details_data if hasattr(record, 'details_data') else {}
                    try:
                        details_str = json.dumps(details)
                        record.details_json = "{...}" if len(details_str) > 200 else details_str
                    except TypeError:
                        record.details_json = str(details)[:200] + ("..." if len(str(details)) > 200 else "")
                    return True
            console_handler.addFilter(ConsoleDetailsFilter())
            console_formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] %(event_display)s - Details: %(details_json)s", datefmt="%H:%M:%S")
            console_handler.setFormatter(console_formatter)
            self.py_logger.addHandler(console_handler)
        
        # 3. Formatted Text Log Handler (optional, based on CLI arg)
        if formatted_log_path:
            try:
                text_file_handler = logging.FileHandler(formatted_log_path, mode='a', encoding='utf-8')
                text_formatter = logging.Formatter('%(asctime)s - %(levelname)-8s - %(module)s.%(funcName)s - %(message)s - Details: %(details)s')
                # A filter to make details more readable in plain text
                class TextDetailsFilter(logging.Filter):
                    def filter(self, record: logging.LogRecord):
                        record.details = json.dumps(record.details_data) if hasattr(record, 'details_data') else "{}"
                        return True
                text_file_handler.addFilter(TextDetailsFilter())
                text_file_handler.setFormatter(text_formatter)
                self.py_logger.addHandler(text_file_handler)
            except Exception as e:
                 print(f"CRITICAL: Could not initialize formatted text log at {formatted_log_path}: {e}", file=sys.stderr)


    def emit(self, event_msg: str, details: Optional[Dict[str, Any]] = None, severity_str: str = "INFO"):
        """Logs a message by mapping a severity string to a Python logging level."""
        log_level_numeric = getattr(logging, severity_str.upper(), logging.INFO)
        self.py_logger.log(log_level_numeric, event_msg, extra={'details_data': details or {}})

    def debug(self, msg: str, details: Optional[Dict[str, Any]] = None): self.emit(msg, details, "DEBUG")
    def info(self, msg: str, details: Optional[Dict[str, Any]] = None): self.emit(msg, details, "INFO")
    def warning(self, msg: str, details: Optional[Dict[str, Any]] = None): self.emit(msg, details, "WARNING")
    def error(self, msg: str, details: Optional[Dict[str, Any]] = None): self.emit(msg, details, "ERROR")
    def critical(self, msg: str, details: Optional[Dict[str, Any]] = None): self.emit(msg, details, "CRITICAL")


# --- UTILITIES ---
# A collection of helper functions. In a larger application, these could be
# organized into more specific utility modules, e.g., `src/utils/cache.py`.

CACHE_FILE_PATH = "data/run_metrics_cache.json"

def load_run_cache(logger: Logger) -> dict:
    """Loads run metrics from the JSON cache file, creating it if it doesn't exist."""
    default_cache = {"total_script_runs": 0, "sites": {}}
    os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)

    if not os.path.exists(CACHE_FILE_PATH):
        logger.info("cache_not_found", {"msg": f"Cache '{CACHE_FILE_PATH}' not found. Using new cache."})
        return default_cache
    try:
        with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f: data = json.load(f)
        if "total_script_runs" not in data or "sites" not in data:
            logger.warning("cache_malformed", {"msg": f"Cache '{CACHE_FILE_PATH}' malformed. Re-initializing."})
            return default_cache
        logger.info("cache_loaded", {"msg": f"Loaded cache from '{CACHE_FILE_PATH}'."})
        return data
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.warning("cache_load_err", {"msg": f"Error loading cache '{CACHE_FILE_PATH}': {e}. Re-initializing.", "error": str(e)})
        return default_cache

def save_run_cache(data: dict, logger: Logger):
    """Saves the updated run metrics to the JSON cache file."""
    try:
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
        logger.info("cache_saved", {"path": CACHE_FILE_PATH, "runs": data.get("total_script_runs")})
    except Exception as e:
        logger.error("cache_save_err", {"err": f"Could not save cache to '{CACHE_FILE_PATH}': {e}"})

def progress(value, length=30, title=" ", vmin=0.0, vmax=1.0) -> str:
    """Generates a string for a text-based progress bar."""
    blocks = ["", "▏","▎","▍","▌","▋","▊","▉","█"]
    lsep, rsep = "|", "|"
    norm_val = (value - vmin) / (vmax - vmin) if (vmax - vmin) != 0 else 0.0
    norm_val = min(max(norm_val, 0.0), 1.0)
    v = norm_val * length
    x = math.floor(v); y = v - x; base = 0.125
    i = min(len(blocks) - 1, int(round(base * math.floor(y / base), 3) / base if base != 0 else 0))
    bar_fill = "█" * x + blocks[i]
    bar_str = lsep + bar_fill + " " * (length - len(bar_fill)) + rsep
    return f"{title}{bar_str} {norm_val*100:.1f}%"

def load_urls(file_path: str, logger: Logger) -> List[str]:
    """Loads URLs from a text file, skipping empty lines and comments."""
    if not os.path.exists(file_path):
        logger.error("url_file_missing", {"msg": f"URL file missing: {file_path}."})
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            urls = [url.strip() for url in f if url.strip() and not url.strip().startswith("#")]
        logger.info("url_file_loaded", {"path": file_path, "count": len(urls)})
        return urls
    except Exception as e:
        logger.error("url_file_read_err", {"msg": f"Error reading URL file {file_path}: {str(e)}"})
        return []


# --- SERVICES ---
# These classes encapsulate the core business logic of the application.
# `AuthService` handles authentication, `BonusScraper` handles data fetching and parsing.
# Future modularization could see these in `src/services/`.

class AuthService:
    """Handles URL cleaning, site access, and user authentication."""
    API_PATH = "/api/v1/index.php"
    def __init__(self, logger: Logger): self.logger = logger

    @staticmethod
    def clean_url(url: str) -> str:
        """Robustly cleans a URL to its base (scheme://netloc)."""
        try:
            parsed_url = urlparse(url)
            return urlunparse(parsed_url._replace(path="", params="", query="", fragment=""))
        except Exception as e:
            print(f"Warning: Could not parse URL '{url}' for cleaning: {e}. Using basic strip.", file=sys.stderr)
            return url.split("?")[0].split("#")[0].rstrip('/')

    @staticmethod
    def extract_merchant_info(html: str) -> tuple[Optional[str], Optional[str]]:
        """Extracts Merchant ID and Name from HTML content using regex."""
        match = re.search(r'var MERCHANTID = (\d+);\s*var MERCHANTNAME = ["\'](.*?)["\'];', html, re.IGNORECASE)
        return match.groups() if match else (None, None)

    def login(self, base_url: str, mobile: str, password: str, timeout: int) -> Optional[AuthData]:
        """Orchestrates the login process for a given site."""
        try:
            self.logger.debug("login_initial_get", {"url": base_url})
            response = requests.get(base_url, timeout=timeout)
            response.raise_for_status()
            html = response.text
        except requests.exceptions.RequestException as e:
            self.logger.warning("site_fetch_fail", {"url": base_url, "err": f"Failed to fetch base URL: {str(e)}"})
            return None
        except Exception as e:
            self.logger.error("site_load_exception", {"url": base_url, "ctx": "Initial page load", "err": str(e)})
            return None

        merchant_id, merchant_name = self.extract_merchant_info(html)
        if not merchant_id or not merchant_name:
            self.logger.warning("login_merch_id_fail", {"url": base_url, "reason": "No merchant ID/name in HTML"})
            return None

        api_url_full = base_url + self.API_PATH
        payload = {"module": "/users/login", "mobile": mobile, "password": password, "merchantId": merchant_id,
                   "domainId": "0", "accessId": "", "accessToken": "", "walletIsAdmin": ""}
        self.logger.debug("api_login_req", {"url": api_url_full, "mod": payload.get("module")})

        try:
            response = requests.post(api_url_full, data=payload, timeout=timeout)
            response.raise_for_status()
            res_json = response.json()
            api_status = res_json.get("status")
            if api_status != "SUCCESS":
                self.logger.warning("login_api_status_fail", {"url": api_url_full, "api_response": res_json})
                return None
            data = res_json.get("data", {})
            if not isinstance(data, dict) or not data.get("token"):
                self.logger.warning("login_token_fail", {"url": base_url, "reason": res_json.get("message", "No token/data not dict")})
                return None
            self.logger.info("login_success", {"url": base_url, "merch": merchant_name})
            return AuthData(merchant_id, merchant_name, data.get("id"), data.get("token"), api_url_full)
        except requests.exceptions.RequestException as e:
            self.logger.warning("api_login_req_exception", {"url": api_url_full, "err": f"Login API err: {str(e)}"})
        except json.JSONDecodeError:
            self.logger.error("api_login_json_err", {"url": api_url_full, "err": "JSON decode err login API"})
        except Exception as e:
            self.logger.error("login_process_exception", {"url": base_url, "err": str(e)})
        return None

class BonusScraper:
    """Handles fetching, parsing, and categorizing bonus data."""
    def __init__(self, logger: Logger, request_timeout: int):
        self.logger = logger
        self.request_timeout = request_timeout

    def _parse_float_field(self, value_from_api: Any) -> float:
        """Robustly parses a field that should be a float, handling various data types."""
        if value_from_api is None or value_from_api == '':
            return 0.0
        if isinstance(value_from_api, (int, float)):
            return float(value_from_api)
        if isinstance(value_from_api, str):
            try: return float(value_from_api.strip())
            except (ValueError, TypeError): return 0.0
        if isinstance(value_from_api, dict):
            for key in ['value', 'min']:
                if key in value_from_api: return self._parse_float_field(value_from_api[key])
        return 0.0

    def _parse_claim_config(self, b_data: dict, bonus_obj: Bonus):
        """Parses the 'claimConfig' field to populate categorical bonus attributes."""
        raw_config = b_data.get("claimConfig", "")
        bonus_obj.raw_claim_config = raw_config
        bonus_obj.raw_claim_condition = b_data.get("claimCondition", "")

        if not isinstance(raw_config, str) or not raw_config.startswith('['): return

        try:
            config_list = json.loads(raw_config)
            if not isinstance(config_list, list): return
            
            for item in config_list:
                if not isinstance(item, str): continue
                
                item_upper = item.upper()
                if "AUTO_CLAIM" in item_upper: bonus_obj.is_auto_claim = True
                if "VIP" in item_upper: bonus_obj.is_vip_only = True
                if "DEPOSIT" in item_upper: bonus_obj.claim_type = "DEPOSIT"
                if "RESCUE" in item_upper: bonus_obj.claim_type = "RESCUE"
                if "REBATE" in item_upper: bonus_obj.claim_type = "REBATE"
                
                if "LOSS" in item_upper:
                    bonus_obj.has_loss_requirement = True
                    parts = item.split('_')
                    if len(parts) > 1:
                        if '%' in parts[-1]: bonus_obj.loss_req_percent = self._parse_float_field(parts[-1].replace('%',''))
                        else: bonus_obj.loss_req_amount = self._parse_float_field(parts[-1])
                
                if "TOPUP" in item_upper:
                    bonus_obj.has_topup_requirement = True
                    parts = item.split('_')
                    if len(parts) > 1: bonus_obj.topup_req_amount = self._parse_float_field(parts[-1])

        except json.JSONDecodeError:
            self.logger.debug("claim_config_parse_fail", {"config": raw_config, "bonus_id": bonus_obj.id})

    def fetch_bonuses(self, base_url: str, auth: AuthData, csv_file: str) -> Union[Tuple[int, float, dict, dict], str]:
        C_KW = ["commission", "affiliate"]; D_KW = ["downline first deposit"]; S_KW = ["share bonus", "referrer"]
        old_flags = {"C": False, "D": False, "S": False, "O": False}
        new_flags = {"A": False, "V": False, "L": False, "T": False}

        payload = {"module": "/users/syncData", "merchantId": auth.merchant_id, "domainId": "0", "accessId": auth.access_id, "accessToken": auth.token, "walletIsAdmin": ""}
        self.logger.debug("api_bonus_req", {"url": auth.api_url, "mod": payload.get("module")})
        try:
            response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
            response.raise_for_status()
            res = response.json()
        except requests.exceptions.RequestException as e:
            self.logger.warning("api_bonus_req_exception", {"url": auth.api_url, "err": str(e)}); return "UNRESPONSIVE"
        except json.JSONDecodeError:
            self.logger.error("api_bonus_json_err", {"url": auth.api_url, "err": "JSON decode"}); return "ERROR"
        except Exception as e:
            self.logger.error("bonus_fetch_exception", {"err": f"Bonus fetch for {auth.api_url} fail: {str(e)}"}); return "ERROR"

        if res.get("status") != "SUCCESS":
            self.logger.warning("bns_api_err_detail", {"url": auth.api_url, "response": res})
            return "ERROR"

        bonus_l = res.get("data", {}).get("bonus", [])
        promo_l = res.get("data", {}).get("promotions", [])
        raw_data = (bonus_l if isinstance(bonus_l, list) else []) + (promo_l if isinstance(promo_l, list) else [])

        if not raw_data:
            self.logger.info("bns_data_empty", {"url": base_url, "count": 0})
            return 0, 0.0, old_flags, new_flags

        rows_to_write: List[Bonus] = []
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)

        for b_data in raw_data:
            if not isinstance(b_data, dict): continue
            
            bf = self._parse_float_field(b_data.get("bonusFixed"))
            mw = self._parse_float_field(b_data.get("minWithdraw"))
            
            bonus_obj = Bonus(
                url=base_url, merchant_name=str(auth.merchant_name or ""), id=str(b_data.get("id","")),
                name=str(b_data.get("name","")), transaction_type=str(b_data.get("transactionType","")),
                amount=self._parse_float_field(b_data.get("amount")),
                rollover=self._parse_float_field(b_data.get("rollover")),
                bonus_fixed=bf, min_withdraw=mw,
                max_withdraw=self._parse_float_field(b_data.get("maxWithdraw")),
                withdraw_to_bonus_ratio = mw / bf if bf != 0 else None,
                min_topup=self._parse_float_field(b_data.get("minTopup")),
                max_topup=self._parse_float_field(b_data.get("maxTopup")),
                balance=str(b_data.get("balance", "")),
                bonus=str(b_data.get("bonus", "")),
                bonus_random=str(b_data.get("bonusRandom", "")),
                reset=str(b_data.get("reset", "")),
                refer_link=str(b_data.get("referLink", "")),
            )
            self._parse_claim_config(b_data, bonus_obj)
            rows_to_write.append(bonus_obj)

            name_lc = bonus_obj.name.lower()
            if any(kw in name_lc for kw in C_KW): old_flags["C"] = True
            elif any(kw in name_lc for kw in D_KW): old_flags["D"] = True
            elif any(kw in name_lc for kw in S_KW): old_flags["S"] = True
            else: old_flags["O"] = True

            if bonus_obj.is_auto_claim: new_flags["A"] = True
            if bonus_obj.is_vip_only: new_flags["V"] = True
            if bonus_obj.has_loss_requirement: new_flags["L"] = True
            if bonus_obj.has_topup_requirement: new_flags["T"] = True

        if rows_to_write:
            try:
                header = [f.name for f in fields(Bonus)]
                file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
                with open(csv_file, "a", newline="", encoding="utf-8") as f_app:
                    writer = csv.writer(f_app)
                    if not file_exists_and_not_empty:
                        writer.writerow(header)
                    for bonus_item in rows_to_write:
                        writer.writerow([getattr(bonus_item, f.name) for f in fields(Bonus)])
                self.logger.debug("csv_bns_write", {"file": csv_file, "count": len(rows_to_write)})
            except Exception as e:
                self.logger.error("csv_bns_write_err", {"file": csv_file, "op":"append_bonuses", "err": str(e)})
                return "ERROR"

        total_amount = sum(b.amount for b in rows_to_write)
        self.logger.info("bns_data_summary", {"url": base_url, "count": len(rows_to_write), "amount": total_amount})
        return len(rows_to_write), total_amount, old_flags, new_flags


# --- MAIN EXECUTION ORCHESTRATOR ---
# This is the main control flow of the application.

def run_scraper(args):
    """Initializes services and orchestrates the scraping process."""
    DIR = os.path.dirname(os.path.abspath(__file__))
    XSTAT = 18
    XUI = 155
    T_O = 30

    config_loader = ConfigLoader(path=os.path.join(DIR, "config.ini"))
    try: config = config_loader.load()
    except SystemExit: return
    
    formatted_log_path = os.path.join(DIR, "logs/bonus_formatted.log") if args.log_format == 'text' else None
    log_file_path = config.logging.log_file
    if not os.path.isabs(log_file_path): log_file_path = os.path.join(DIR, log_file_path)

    logger = Logger(log_file_path, config.logging.detail, config.logging.console, formatted_log_path)
    logger.info("jbeg", {"msg": "CLI Scraper starting.", "cfg_ok": True})

    run_cache = load_run_cache(logger)
    run_cache["total_script_runs"] = run_cache.get("total_script_runs", 0) + 1

    failed_sites: List[str] = []
    auth_service = AuthService(logger)
    bonus_scraper = BonusScraper(logger, T_O)

    url_file_path = config.settings.url_file
    if not os.path.isabs(url_file_path): url_file_path = os.path.join(DIR, url_file_path)
    urls = load_urls(url_file_path, logger)

    if not urls:
        logger.info("jend_no_urls", {"status": "No URLs. Exiting."})
        save_run_cache(run_cache, logger)
        return

    total_urls = len(urls)
    metrics = {"new_bonuses": 0, "new_downlines": 0, "new_errors": 0, "new_bonus_amount": 0.0}
    base_data_dir = os.path.join(DIR, "data")
    os.makedirs(base_data_dir, exist_ok=True)

    process_site_for_downlines = None
    if config.settings.downline_enabled:
        try:
            from downline import process_site_for_downlines
            logger.info("run_mode_dnln_enabled")
        except ImportError:
            logger.critical("dnln_module_missing", {"msg": "Module 'downline.py' not found."})
            sys.exit("Downline module missing.")

    bonus_csv_path = os.path.join(base_data_dir, f"{datetime.now().strftime('%Y-%m-%d')}_bonuses.csv")
    downline_csv_path = os.path.join(base_data_dir, "downlines_master.csv")
    
    job_start_time = time.time()

    def format_stat_display(cur, prev, width=XSTAT):
        diff = cur - prev
        s = f"{cur}/{prev}({diff:+})" if cur > 0 or prev > 0 else "-"
        return s.ljust(width)

    try:
        for idx, raw_url in enumerate(urls, 1):
            if idx > 1 and sys.stdout.isatty(): sys.stdout.write('\\x1b[4A\\x1b[J')

            site_time_start = time.time()
            cleaned_url = auth_service.clean_url(raw_url)
            site_key = cleaned_url

            cache_entry = run_cache["sites"].get(site_key, {})
            p_b, pt_b = cache_entry.get("last_run_new_bonuses", 0), cache_entry.get("cumulative_total_bonuses", 0)
            p_d, pt_d = cache_entry.get("last_run_new_downlines", 0), cache_entry.get("cumulative_total_downlines", 0)
            p_e, pt_e = cache_entry.get("last_run_new_errors", 0), cache_entry.get("cumulative_total_errors", 0)
            
            s_b, s_d, s_e, s_b_amt = 0, 0, 0, 0.0
            old_flags, new_flags = {}, {}
            
            auth = auth_service.login(cleaned_url, config.credentials.mobile, config.credentials.password, T_O)

            if not auth:
                s_e = 1; failed_sites.append(f"{cleaned_url} (Login Fail)")
                logger.warning("login_summary_failed", {"url": cleaned_url})
            else:
                logger.info("login_summary_success", {"url": cleaned_url, "merch": auth.merchant_name})
                if config.settings.downline_enabled and process_site_for_downlines:
                    dl_res = process_site_for_downlines(cleaned_url, auth, downline_csv_path, logger, T_O)
                    if isinstance(dl_res, str): s_e += 1; failed_sites.append(f"{cleaned_url} (DL Err)")
                    else: s_d = dl_res
                else:
                    b_res = bonus_scraper.fetch_bonuses(cleaned_url, auth, bonus_csv_path)
                    if isinstance(b_res, str): s_e += 1; failed_sites.append(f"{cleaned_url} (Bonus Err)")
                    else: s_b, s_b_amt, old_flags, new_flags = b_res
            
            metrics["new_bonuses"] += s_b; metrics["new_downlines"] += s_d
            metrics["new_errors"] += s_e; metrics["new_bonus_amount"] += s_b_amt
            run_cache["sites"][site_key] = {
                "last_run_new_bonuses": s_b, "cumulative_total_bonuses": pt_b + s_b,
                "last_run_new_downlines": s_d, "cumulative_total_downlines": pt_d + s_d,
                "last_run_new_errors": s_e, "cumulative_total_errors": pt_e + s_e,
                "old_flags": old_flags, "new_flags": new_flags,
                "last_processed_ts": datetime.now().isoformat()
            }

            site_proc_time = time.time() - site_time_start
            prog_bar = progress(idx / total_urls, length=30)
            l1 = f" {prog_bar} [{idx/total_urls:.2%}] {idx}/{total_urls} ".ljust(XUI)
            
            old_flags_disp = f"[C]{'Y' if old_flags.get('C') else 'N'} [D]{'Y' if old_flags.get('D') else 'N'} [S]{'Y' if old_flags.get('S') else 'N'} [O]{'Y' if old_flags.get('O') else 'N'}"
            l2 = f" Site: {cleaned_url[:40].ljust(40)} | Time: {f'{site_proc_time:.1f}s'.ljust(6)} | Run: #{str(run_cache['total_script_runs']).ljust(5)} | Old Flags: {old_flags_disp} ".ljust(XUI)

            new_flags_disp = f"[A]{'Y' if new_flags.get('A') else 'N'} [V]{'Y' if new_flags.get('V') else 'N'} [L]{'Y' if new_flags.get('L') else 'N'} [T]{'Y' if new_flags.get('T') else 'N'}"
            l3 = f" New Flags: {new_flags_disp}".ljust(XUI)

            sfs = run_cache["sites"][site_key]
            n_b, n_d, n_e = sfs['last_run_new_bonuses'], sfs['last_run_new_downlines'], sfs['last_run_new_errors']
            nt_b, nt_d, nt_e = sfs['cumulative_total_bonuses'], sfs['cumulative_total_downlines'], sfs['cumulative_total_errors']
            stat_b = f"B: {format_stat_display(n_b, p_b)}(T: {format_stat_display(nt_b, pt_b)})"
            stat_d = f"D: {format_stat_display(n_d, p_d)}(T: {format_stat_display(nt_d, pt_d)})" if config.settings.downline_enabled else "".ljust(len(f"D: {format_stat_display(0,0)}(T: {format_stat_display(0,0)})"))
            stat_e = f"E: {format_stat_display(n_e, p_e)}(T: {format_stat_display(nt_e, pt_e)})"
            l4 = f" Stats: {stat_b} | {stat_d} | {stat_e} ".ljust(XUI)

            if sys.stdout.isatty():
                sys.stdout.write(f"{l1}\\n{l2}\\n{l3}\\n{l4}\\n"); sys.stdout.flush()
            elif idx % 50 == 0 or idx == total_urls:
                logger.info("batch_progress_update", {"proc": idx, "total": total_urls})

        if sys.stdout.isatty(): sys.stdout.write("\\n")

        job_time = time.time() - job_start_time
        summary = {"duration_seconds": round(job_time, 2), "urls_processed": total_urls, **metrics, "failed_sites_count": len(failed_sites)}
        logger.info("jend_summary", summary)
        if failed_sites: logger.warning("jend_failed_sites", {"sites": failed_sites, "count": len(failed_sites)})

    except KeyboardInterrupt:
        logger.warning("job_user_interrupt", {"msg": "User interrupted (Ctrl+C)."})
    except Exception as e:
        logger.critical("job_critical_error", {"err": str(e), "trace": traceback.format_exc()})
    finally:
        save_run_cache(run_cache, logger)
        logger.info("shutdown_msg", {"msg": "CLI Scraper finished."})
        logging.shutdown()

# --- SCRIPT ENTRY POINT ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slap Red Scraper v0.4.0")
    parser.add_argument('--log-format', type=str, choices=['text'], help="Generate an additional human-readable text log file.")
    args = parser.parse_args()

    script_dir_abs = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir_abs)
    
    run_scraper(args)
