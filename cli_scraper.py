import csv
import os
import sys
import time
import requests
import logging
import json
import math
from dataclasses import dataclass, field # field is not used, but was in original. Keeping for consistency.
from typing import List, Set, Tuple, Union, Optional, Dict, Any
from datetime import datetime, timedelta
import configparser

# --- START OF src/models.py ---
@dataclass
class AuthData:
    merchant_id: str
    merchant_name: str
    access_id: str
    token: str
    api_url: str

@dataclass
class Downline:
    url: str
    id: str
    name: str
    count: int
    amount: float
    register_date_time: str

@dataclass
class Bonus:
    url: str
    merchant_name: str
    id: str
    name: str
    transaction_type: str
    bonus_fixed: float
    amount: float
    min_withdraw: float
    max_withdraw: float
    withdraw_to_bonus_ratio: Optional[float]
    rollover: float
    balance: str
    claim_config: str
    claim_condition: str
    bonus: str
    bonus_random: str
    reset: str
    min_topup: float
    max_topup: float
    refer_link: str
# --- END OF src/models.py ---

# --- START OF src/config.py ---
@dataclass
class Credentials:
    mobile: str
    password: str

@dataclass
class Settings:
    url_file: str
    downline_enabled: bool

@dataclass
class LoggingConfig:
    log_file: str
    log_level: str
    console: bool
    detail: str

@dataclass
class AppConfig:
    credentials: Credentials
    settings: Settings
    logging: LoggingConfig

class ConfigLoader:
    """Loads and validates configuration from a .ini file."""
    def __init__(self, path: str = "config.ini"):
        if not os.path.exists(path):
            print(f"FATAL: Configuration file not found: {path}. Please create it.")
            sys.exit(1)
        self.config = configparser.ConfigParser()
        self.config.read(path)
        self._config_file_path = path # Store path for error messages

    def load(self) -> AppConfig:
        try:
            app_config = AppConfig(
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
                    log_level=self.config["logging"]["log_level"].upper(),
                    console=self.config["logging"].getboolean("console", fallback=True),
                    detail=self.config["logging"].get("detail", "LESS").upper()
                )
            )
            # Validate log_level
            if app_config.logging.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                print(f"Warning: Invalid log_level '{app_config.logging.log_level}' in '{self._config_file_path}'. Defaulting to INFO.")
                app_config.logging.log_level = "INFO"
            
            # Validate detail
            if app_config.logging.detail not in ["LESS", "MORE", "MAX"]:
                print(f"Warning: Invalid detail level '{app_config.logging.detail}' in '{self._config_file_path}'. Defaulting to LESS.")
                app_config.logging.detail = "LESS"

            return app_config

        except KeyError as e:
            print(f"FATAL: Configuration error in '{self._config_file_path}': Missing key {e}")
            sys.exit(1)
        except ValueError as e: 
            print(f"FATAL: Configuration error in '{self._config_file_path}': Invalid value for a boolean field. {e}")
            sys.exit(1)
# --- END OF src/config.py ---

# --- START OF src/logger.py ---
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = { # Renamed to avoid conflict with record
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.module,
            "method": record.funcName,
            "event": record.msg, 
            "details": record.details_data if hasattr(record, 'details_data') else {}
        }
        return json.dumps(log_entry)

class Logger:
    """Structured logger with verbosity control for CLI."""
    VERBOSITY_LEVELS = {"LESS": 0, "MORE": 1, "MAX": 2}
    EVENT_VERBOSITY = {
        "job_start": "LESS", "job_complete": "LESS", "login_success": "MORE",
        "login_failed": "MORE", "api_request": "MORE", "api_response": "MORE",
        "bonus_fetched": "MORE", "downline_fetched": "MORE", "csv_written": "MORE",
        "exception": "LESS", "website_unresponsive": "LESS", "down_sites_summary": "LESS",
        "bonus_api_error": "MORE", "progress_update": "LESS", 
        "cache_saved": "MORE", "cache_loaded_info": "MORE", "logger_internal_error": "WARNING",
        "file_error": "ERROR", "file_loaded": "INFO", "api_error": "WARNING", "api_data_warning": "WARNING",
        "csv_error": "ERROR", "csv_warning": "DEBUG", "data_processing_error": "WARNING",
        "login_summary": "INFO", "run_mode_info": "INFO", "progress_batch_update": "INFO",
        "job_interrupted": "WARNING", "job_error_critical": "CRITICAL", "shutdown": "INFO"
        # Add other event types here if needed
    }

    def __init__(self, log_file: str, log_level: str, console: bool, detail: str):
        self.logger = logging.getLogger("CliScraperLogger")
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        self.verbosity = self.VERBOSITY_LEVELS.get(detail.upper(), 0)

        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        try:
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setFormatter(JsonFormatter())
            self.logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not initialize file logger at {log_file}: {e}")

        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(event_display)s - Details: %(details_json)s",
                 datefmt="%H:%M:%S"
            )
            
            class DetailsInjectorFilter(logging.Filter):
                def filter(self, record_item): # Renamed to avoid conflict
                    details = record_item.details_data if hasattr(record_item, 'details_data') else {}
                    record_item.event_display = record_item.msg # Use msg as event for display
                    try:
                        details_str = json.dumps(details)
                        if len(details_str) > 150:
                             record_item.details_json = "{...too verbose for console...}"
                        else:
                            record_item.details_json = details_str
                    except TypeError:
                        record_item.details_json = str(details) # Fallback if not JSON serializable
                    return True
            
            console_handler.addFilter(DetailsInjectorFilter())
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)

    def emit(self, event: str, details: Dict[str, Any], level: str = "INFO") -> None:
        event_specific_verbosity_name = self.EVENT_VERBOSITY.get(event, "MORE")
        required_level_value = self.VERBOSITY_LEVELS.get(event_specific_verbosity_name, 0)

        if self.verbosity >= required_level_value:
            actual_details = details if isinstance(details, dict) else {"data": details}
            self.logger.log(getattr(logging, level.upper()), event, extra={'details_data': actual_details})

    def load_metrics(self, log_file: str) -> Dict[str, float]:
        metrics = {
            "bonuses": 0, "downlines": 0, "errors": 0, "runs": 0,
            "total_runtime": 0.0, "total_bonus_amount": 0.0,
            "successful_bonus_fetches": 0, "failed_bonus_api_calls": 0
        }
        if not os.path.exists(log_file):
            self.emit("cache_loaded_info", {"message": f"Log file '{log_file}' for metrics not found. Defaulting metrics."}, level="DEBUG")
            return metrics
        
        processed_lines = 0
        try:
            with open(log_file, "r", encoding='utf-8') as f:
                for line in f:
                    try:
                        log_data = json.loads(line) # Renamed
                        processed_lines += 1
                        event = log_data.get("event")
                        details = log_data.get("details", {})

                        if event == "bonus_fetched":
                            metrics["bonuses"] += details.get("count", 0)
                            metrics["total_bonus_amount"] += float(details.get("total_amount", 0.0))
                            metrics["successful_bonus_fetches"] += 1
                        elif event == "downline_fetched":
                            metrics["downlines"] += details.get("count", 0)
                        elif event == "exception" or event == "website_unresponsive":
                            metrics["errors"] += 1
                        elif event == "bonus_api_error":
                            metrics["failed_bonus_api_calls"] += 1
                            metrics["errors"] += 1
                        elif event == "job_complete":
                            metrics["runs"] += 1
                            metrics["total_runtime"] += float(details.get("duration", 0.0))
                    except json.JSONDecodeError:
                        self.emit("logger_internal_error", {"error": "JSONDecodeError parsing log line for metrics."}, level="WARNING")
                        continue 
            self.emit("cache_loaded_info", {"message": f"Loaded metrics from {processed_lines} lines in '{log_file}'."}, level="DEBUG")
        except Exception as e:
            self.emit("logger_internal_error", {"error": f"Error loading metrics from '{log_file}': {str(e)}"}, level="ERROR")
        return metrics
# --- END OF src/logger.py ---

# --- START OF src/utils.py ---
CACHE_FILE_PATH = "data/run_metrics_cache.json" 

def load_run_cache(logger_instance: Optional[Logger] = None) -> dict:
    default_cache = {"total_script_runs": 0, "sites": {}}
    # Ensure data directory exists for the cache file, even if it's the first run.
    # The CACHE_FILE_PATH already includes "data/"
    cache_dir = os.path.dirname(CACHE_FILE_PATH)
    if not os.path.exists(cache_dir):
        try:
            os.makedirs(cache_dir, exist_ok=True)
            if logger_instance:
                logger_instance.emit("cache_loaded_info", {"message": f"Created directory '{cache_dir}' for cache file."}, level="DEBUG")
        except OSError as e:
            if logger_instance:
                logger_instance.emit("cache_error", {"error": f"Could not create directory '{cache_dir}': {e}"}, level="ERROR")
            else:
                print(f"Error: Could not create directory '{cache_dir}': {e}")
            return default_cache # Return default if dir creation fails

    if not os.path.exists(CACHE_FILE_PATH):
        if logger_instance:
            logger_instance.emit("cache_loaded_info", {"message": f"Cache file '{CACHE_FILE_PATH}' not found. Initializing new cache."}, level="INFO")
        else:
            print(f"Info: Cache file '{CACHE_FILE_PATH}' not found. Initializing new cache.")
        return default_cache
    try:
        with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if "total_script_runs" not in data or "sites" not in data:
            if logger_instance:
                logger_instance.emit("cache_loaded_info", {"message": f"Cache file '{CACHE_FILE_PATH}' malformed. Re-initializing."}, level="WARNING")
            else:
                print(f"Warning: Cache file '{CACHE_FILE_PATH}' malformed. Re-initializing.")
            return default_cache
        if logger_instance:
             logger_instance.emit("cache_loaded_info", {"message": f"Successfully loaded run cache from '{CACHE_FILE_PATH}'."}, level="INFO")
        return data
    except (json.JSONDecodeError, FileNotFoundError) as e: # FileNotFoundError might happen in a race condition
        if logger_instance:
            logger_instance.emit("cache_loaded_info", {"message": f"Error loading cache '{CACHE_FILE_PATH}': {e}. Re-initializing."}, level="WARNING")
        else:
            print(f"Warning: Error loading cache '{CACHE_FILE_PATH}': {e}. Re-initializing.")
        return default_cache

def save_run_cache(data: dict, logger_instance: Optional[Logger] = None):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        if logger_instance:
            logger_instance.emit("cache_saved", {"path": CACHE_FILE_PATH, "total_script_runs": data.get("total_script_runs")}, level="INFO")
    except Exception as e:
        if logger_instance:
            logger_instance.emit("cache_error", {"error": f"Could not save cache to '{CACHE_FILE_PATH}': {e}"}, level="ERROR")
        else:
            print(f"Error: Could not save cache file '{CACHE_FILE_PATH}': {e}")

def progress(value, length=40, title=" ", vmin=0.0, vmax=1.0) -> str:
    blocks = ["", "▏","▎","▍","▌","▋","▊","▉","█"]
    vmin = vmin or 0.0
    vmax = vmax or 1.0
    lsep, rsep = "|", "|" 

    value = min(max(value, vmin), vmax) # Ensure value is within bounds
    # Avoid division by zero if vmin == vmax
    normalized_value = (value - vmin) / (vmax - vmin) if (vmax - vmin) != 0 else 0.0

    v = normalized_value * length
    x = math.floor(v)
    y = v - x
    base = 0.125
    # Ensure index i is within bounds of blocks list
    i = min(len(blocks) - 1, int(round(base * math.floor(float(y) / base), 3) / base if base != 0 else 0))

    bar_fill = "█" * x + blocks[i]
    # Calculate remaining space for the bar
    n = length - len(bar_fill)
    bar_str = lsep + bar_fill + " " * n + rsep
    return f"{title}{bar_str} {normalized_value*100:.1f}%"
# --- END OF src/utils.py ---

# --- START OF src/auth.py ---
import re 

class AuthService:
    """Manages authentication and URL processing."""
    API_PATH = "/api/v1/index.php"

    def __init__(self, logger: Logger):
        self.logger = logger

    @staticmethod
    def clean_url(url: str) -> str:
        url = re.sub(r"/(RF|GA|GX|JK|TB|AU|EWAY|MNP|LuckyNum|FORTUNE|BC|King|UU|EZ|AVG|YAYA|ACWIN|OW|CN|AUSSIE|BETJOHN|REF)[\w:]+$", "", url) # Added : to [\w:]
        url = re.sub(r"/\?r_c=[\w]+$", "", url) 
        return url.rstrip('/')

    @staticmethod
    def extract_merchant_info(html: str) -> tuple[Optional[str], Optional[str]]:
        match = re.search(r'var MERCHANTID = (\d+);\s*var MERCHANTNAME = ["\'](.*?)["\'];', html, re.IGNORECASE)
        return match.groups() if match else (None, None)

    def login(self, url: str, mobile: str, password: str, request_timeout: int = 30) -> Optional[AuthData]:
        try:
            response = requests.get(url, timeout=request_timeout)
            response.raise_for_status()
            html = response.text
        except requests.exceptions.Timeout:
            self.logger.emit("website_unresponsive", {"url": url, "error": "Timeout during initial page load"}, level="WARNING")
            return None
        except requests.exceptions.RequestException as e: 
            self.logger.emit("website_unresponsive", {"url": url, "error": f"Fetch URL failed: {str(e)}"}, level="WARNING")
            return None
        except Exception as e: 
            self.logger.emit("exception", {"url": url, "context": "Initial page load", "error": str(e)}, level="ERROR")
            return None

        merchant_id, merchant_name = self.extract_merchant_info(html)
        if not merchant_id or not merchant_name: 
            self.logger.emit("login_failed", {"url": url, "reason": "No merchant ID/name in HTML"}, level="WARNING")
            return None

        api_url_full = url.rstrip('/') + self.API_PATH
        payload = {
            "module": "/users/login", "mobile": mobile, "password": password,
            "merchantId": merchant_id, "domainId": "0", "accessId": "",
            "accessToken": "", "walletIsAdmin": ""
        }
        self.logger.emit("api_request", {
            "url": api_url_full, "action": "login",
            "module": payload.get("module"), "mobile": payload.get("mobile")
        }, level="DEBUG")

        try:
            response = requests.post(api_url_full, data=payload, timeout=request_timeout)
            response.raise_for_status()
            res_json = response.json()

            response_details = {"url": api_url_full, "action": "login", "status_code": response.status_code, "api_status": res_json.get("status")}
            if res_json.get("status") != "SUCCESS":
                response_details["error_message"] = res_json.get("message")
                data_field = res_json.get("data")
                if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                elif isinstance(data_field, str): response_details["error_data_string"] = data_field
            self.logger.emit("api_response", response_details, level="DEBUG")

            data = res_json.get("data", {})
            if not isinstance(data, dict) or not data.get("token"): # Check token and if data is a dict
                self.logger.emit("login_failed", {"url": url, "reason": res_json.get("message", "No token/data not dict")}, level="WARNING")
                return None

            self.logger.emit("login_success", {"url": url, "merchant_name": merchant_name}, level="INFO")
            return AuthData(
                merchant_id=merchant_id, merchant_name=merchant_name,
                access_id=data.get("id"), token=data.get("token"),
                api_url=api_url_full 
            )
        except requests.exceptions.Timeout:
            self.logger.emit("website_unresponsive", {"url": api_url_full, "action": "login", "error": "Timeout login API"}, level="WARNING")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.emit("website_unresponsive", {"url": api_url_full, "action": "login", "error": f"Login API error: {str(e)}"}, level="WARNING")
            return None
        except json.JSONDecodeError:
            self.logger.emit("exception", {"url": api_url_full, "action": "login", "error": "JSON decode error login API"}, level="ERROR")
            return None
        except Exception as e: 
            self.logger.emit("exception", {"url": url, "context": "Login process", "error": str(e)}, level="ERROR")
            return None
# --- END OF src/auth.py ---

# --- START OF Scraper class and load_urls (from src/main.py) ---
class Scraper:
    """Handles scraping of downlines and bonuses."""
    def __init__(self, logger: Logger, request_timeout: int):
        self.logger = logger
        self.request_timeout = request_timeout

    def fetch_downlines(self, base_url: str, auth: AuthData, csv_file: str) -> Union[int, str]:
        written_keys: Set[Tuple] = set()
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0 :
            try:
                with open(csv_file, mode='r', newline="", encoding="utf-8") as f_read:
                    reader = csv.DictReader(f_read)
                    key_fields = ['url', 'id', 'name', 'count', 'amount', 'register_date_time']
                    for row in reader:
                        if all(k in row for k in key_fields):
                            # Format amount consistently for key matching
                            formatted_amount = f"{float(row['amount'] or 0.0):.2f}"
                            written_keys.add((row['url'], row['id'], row['name'], str(row['count']), formatted_amount, row['register_date_time']))
                        else:
                            self.logger.emit("csv_warning", {"file": csv_file, "message": "Row with missing key fields skipped during pre-load."}, level="DEBUG")
            except Exception as e:
                 self.logger.emit("csv_error", {"file": csv_file, "operation": "read_existing_downlines", "error": str(e)}, level="WARNING")

        total_new_rows = 0
        page = 0
        while True:
            payload = {
                "level": "1", "pageIndex": str(page), "module": "/referrer/getDownline",
                "merchantId": auth.merchant_id, "domainId": "0", "accessId": auth.access_id,
                "accessToken": auth.token, "walletIsAdmin": True # API often needs non-empty string
            }
            self.logger.emit("api_request", {"url": auth.api_url, "module": payload.get("module"), "page": page}, level="DEBUG")
            try:
                response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
                response.raise_for_status()
                res = response.json()

                response_details = {"url": auth.api_url, "module": payload.get("module"), "api_status": res.get("status")}
                if res.get("status") != "SUCCESS":
                    response_details["error_message"] = res.get("message")
                    data_field = res.get("data")
                    if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                    elif isinstance(data_field, str): response_details["error_data_string"] = data_field
                self.logger.emit("api_response", response_details, level="DEBUG")

            except requests.exceptions.Timeout:
                self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/referrer/getDownline", "error": "Timeout"}, level="WARNING")
                return "UNRESPONSIVE"
            except requests.exceptions.RequestException as e:
                self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/referrer/getDownline", "error": str(e)}, level="WARNING")
                return "UNRESPONSIVE" 
            except json.JSONDecodeError:
                self.logger.emit("exception", {"url": auth.api_url, "module": "/referrer/getDownline", "error": "Failed to decode JSON response"}, level="ERROR")
                return "ERROR"
            except Exception as e:
                self.logger.emit("exception", {"error": f"Downline fetch for {auth.api_url} page {page} failed: {str(e)}"}, level="ERROR")
                return "ERROR"

            if res.get("status") != "SUCCESS":
                self.logger.emit("api_error", {"url": auth.api_url, "module":"/referrer/getDownline", "message": res.get("message", "API returned non-SUCCESS status.")}, level="WARNING")
                return "ERROR" 

            new_rows_this_page: List[Downline] = []
            downlines_data = res.get("data", {}).get("downlines", [])
            if not isinstance(downlines_data, list): 
                self.logger.emit("api_data_warning", {"url": auth.api_url, "module":"/referrer/getDownline", "message": "Downlines data is not a list as expected."}, level="WARNING")
                break 

            for d_data in downlines_data:
                if not isinstance(d_data, dict): continue 
                row_obj = Downline( # Renamed to avoid conflict
                    url=base_url, id=str(d_data.get("id","")), name=str(d_data.get("name","")),
                    count=int(d_data.get("count", 0) or 0),
                    amount=float(d_data.get("amount", 0.0) or 0.0),
                    register_date_time=str(d_data.get("registerDateTime",""))
                )
                current_key = (
                    row_obj.url, row_obj.id, row_obj.name, str(row_obj.count),
                    f"{row_obj.amount:.2f}", row_obj.register_date_time 
                )
                if current_key not in written_keys:
                    new_rows_this_page.append(row_obj)
                    written_keys.add(current_key)
            
            if not new_rows_this_page: 
                break

            file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as f_append:
                    fieldnames = [f_item.name for f_item in Downline.__dataclass_fields__.values()] # Renamed
                    writer = csv.DictWriter(f_append, fieldnames=fieldnames)
                    if not file_exists_and_not_empty:
                        writer.writeheader()
                    writer.writerows([r.__dict__ for r in new_rows_this_page])
                self.logger.emit("csv_written", {"file": csv_file, "new_rows_this_page": len(new_rows_this_page), "page": page}, level="DEBUG")
            except Exception as e:
                self.logger.emit("csv_error", {"file": csv_file, "operation":"append_downlines", "error": str(e)}, level="ERROR")
                return "ERROR" 

            total_new_rows += len(new_rows_this_page)
            page += 1
            time.sleep(0.5) 

        self.logger.emit("downline_fetched", {"url": base_url, "total_new_downlines": total_new_rows}, level="INFO")
        return total_new_rows

    def fetch_bonuses(self, base_url: str, auth: AuthData, csv_file: str) -> Union[Tuple[int, float, Dict[str, bool]], str]:
        C_KEYWORDS = ["commission", "affiliate"]
        D_KEYWORDS = ["downline first deposit"]
        S_KEYWORDS = ["share bonus", "referrer"]
        bonus_type_flags = {"C": False, "D": False, "S": False, "O": False}

        payload = {
            "module": "/users/syncData", "merchantId": auth.merchant_id, "domainId": "0",
            "accessId": auth.access_id, "accessToken": auth.token, "walletIsAdmin": ""
        }
        self.logger.emit("api_request", {"url": auth.api_url, "module": payload.get("module")}, level="DEBUG")
        try:
            response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
            response.raise_for_status()
            res = response.json()

            response_details = {"url": auth.api_url, "module": payload.get("module"), "api_status": res.get("status")}
            if res.get("status") != "SUCCESS":
                response_details["error_message"] = res.get("message")
                data_field = res.get("data")
                if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                elif isinstance(data_field, str): response_details["error_data_string"] = data_field
            self.logger.emit("api_response", response_details, level="DEBUG")

        except requests.exceptions.Timeout:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/users/syncData", "error": "Timeout"}, level="WARNING")
            return "UNRESPONSIVE"
        except requests.exceptions.RequestException as e:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/users/syncData", "error": str(e)}, level="WARNING")
            return "UNRESPONSIVE"
        except json.JSONDecodeError:
            self.logger.emit("exception", {"url": auth.api_url, "module": "/users/syncData", "error": "Failed to decode JSON response"}, level="ERROR")
            return "ERROR"
        except Exception as e:
            self.logger.emit("exception", {"error": f"Bonus fetch for {auth.api_url} failed: {str(e)}"}, level="ERROR")
            return "ERROR"

        if res.get("status") != "SUCCESS":
            self.logger.emit("bonus_api_error", {
                "url": auth.api_url, "status": res.get("status"), 
                "error_message": res.get("message", "N/A"), 
                "error_data": res.get("data", "N/A")
            }, level="WARNING")
            return "ERROR"

        bonus_list = res.get("data", {}).get("bonus", []) # Renamed
        promotion_list = res.get("data", {}).get("promotions", []) # Renamed
        bonuses_data_raw = (bonus_list if isinstance(bonus_list, list) else []) + \
                           (promotion_list if isinstance(promotion_list, list) else [])

        if not bonuses_data_raw:
            self.logger.emit("bonus_fetched", {"url": base_url, "count": 0, "total_amount": 0.0}, level="INFO")
            return 0, 0.0, bonus_type_flags

        rows_to_write_obj: List[Bonus] = []
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        
        for b_data in bonuses_data_raw:
            if not isinstance(b_data, dict): continue
            try:
                bonus_fixed_raw = b_data.get("bonusFixed") # Keep None if missing
                min_withdraw_raw = b_data.get("minWithdraw")

                bonus_f = float(bonus_fixed_raw) if bonus_fixed_raw is not None and bonus_fixed_raw != '' else 0.0
                min_w = float(min_withdraw_raw) if min_withdraw_raw is not None and min_withdraw_raw != '' else 0.0
                
                ratio = min_w / bonus_f if bonus_f != 0 else None
            except (ValueError, TypeError) as e:
                self.logger.emit("data_processing_error", {"context": "bonus_calculation", "data": b_data, "error": str(e)}, level="WARNING")
                continue

            bonus_instance = Bonus(
                url=base_url, merchant_name=str(auth.merchant_name or ""), id=str(b_data.get("id","")),
                name=str(b_data.get("name","")), transaction_type=str(b_data.get("transactionType","")),
                bonus_fixed=bonus_f, amount=float(b_data.get("amount", 0.0) or 0.0),
                min_withdraw=min_w, max_withdraw=float(b_data.get("maxWithdraw", 0.0) or 0.0),
                withdraw_to_bonus_ratio=ratio, rollover=float(b_data.get("rollover", 0.0) or 0.0),
                balance=str(b_data.get("balance", "")), claim_config=str(b_data.get("claimConfig", "")),
                claim_condition=str(b_data.get("claimCondition", "")), bonus=str(b_data.get("bonus", "")),
                bonus_random=str(b_data.get("bonusRandom", "")), reset=str(b_data.get("reset", "")),
                min_topup=float(b_data.get("minTopup", 0.0) or 0.0),
                max_topup=float(b_data.get("maxTopup", 0.0) or 0.0),
                refer_link=str(b_data.get("referLink", ""))
            )
            rows_to_write_obj.append(bonus_instance)

            name_lower = bonus_instance.name.lower() if bonus_instance.name else ""
            claim_config_lower = bonus_instance.claim_config.lower() if bonus_instance.claim_config else ""
            matched_specific_type = False
            if any(kw in name_lower or kw in claim_config_lower for kw in C_KEYWORDS): bonus_type_flags["C"] = True; matched_specific_type = True
            if any(kw in name_lower or kw in claim_config_lower for kw in D_KEYWORDS): bonus_type_flags["D"] = True; matched_specific_type = True
            if any(kw in name_lower or kw in claim_config_lower for kw in S_KEYWORDS): bonus_type_flags["S"] = True; matched_specific_type = True
            if not matched_specific_type: bonus_type_flags["O"] = True
        
        if rows_to_write_obj:
            file_exists = os.path.exists(csv_file)
            is_empty = not file_exists or os.path.getsize(csv_file) == 0

            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as f_append:
                    fieldnames = [f_item.name for f_item in Bonus.__dataclass_fields__.values()] # Renamed
                    writer = csv.DictWriter(f_append, fieldnames=fieldnames)
                    if is_empty: 
                        writer.writeheader()
                    writer.writerows([b.__dict__ for b in rows_to_write_obj])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write_obj)}, level="DEBUG")
            except Exception as e:
                self.logger.emit("csv_error", {"file": csv_file, "operation":"append_bonuses", "error": str(e)}, level="ERROR")
                return "ERROR" 

        current_fetch_total_amount = sum(b.amount for b in rows_to_write_obj)
        self.logger.emit("bonus_fetched", {"url": base_url, "count": len(rows_to_write_obj), "total_amount": current_fetch_total_amount}, level="INFO")
        return len(rows_to_write_obj), current_fetch_total_amount, bonus_type_flags

def load_urls(url_file_path: str, logger: Logger) -> List[str]:
    if not os.path.exists(url_file_path):
        logger.emit("file_error", {"message": f"URL file not found: {url_file_path}. No URLs to process."}, level="ERROR")
        return []
    try:
        with open(url_file_path, "r", encoding="utf-8") as f:
            urls = [url.strip() for url in f if url.strip() and not url.strip().startswith("#")] 
        logger.emit("file_loaded", {"path": url_file_path, "url_count": len(urls)}, level="INFO")
        return urls
    except Exception as e:
        logger.emit("file_error", {"message": f"Error reading URL file {url_file_path}: {str(e)}"}, level="ERROR")
        return []
# --- END OF Scraper class and load_urls ---

# --- MAIN EXECUTION ---
def run_scraper():
    # Define console display widths
    STAT_ITEM_WIDTH = 18  # Width for individual stat items like "10/5(+5)"
    CONSOLE_TARGET_WIDTH = 155 # Target total width for each console line, adjust if needed

    # Initialize Config
    config_loader = ConfigLoader(path="config.ini") 
    try:
        config = config_loader.load()
    except SystemExit: 
        return 

    # Initialize Logger
    log_file_path = config.logging.log_file
    if not os.path.isabs(log_file_path):
        # Assume script_dir is defined if this function is called from if __name__ == "__main__"
        # For robustness, calculate it here if needed or ensure it's passed.
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        log_file_path = os.path.join(current_script_dir, log_file_path)
    
    logger = Logger(
        log_file=log_file_path,
        log_level=config.logging.log_level,
        console=config.logging.console,
        detail=config.logging.detail
    )
    logger.emit("job_start", {"message": "CLI Scraper starting.", "config_loaded": True}, level="INFO")

    run_cache_data = load_run_cache(logger) 
    run_cache_data["total_script_runs"] = run_cache_data.get("total_script_runs", 0) + 1

    REQUEST_TIMEOUT = 30  
    unresponsive_sites_this_run: List[str] = []

    auth_service = AuthService(logger)
    scraper = Scraper(logger, REQUEST_TIMEOUT)
    
    url_file_input_path = config.settings.url_file
    if not os.path.isabs(url_file_input_path):
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        url_file_input_path = os.path.join(current_script_dir, url_file_input_path)
    urls_to_process = load_urls(url_file_input_path, logger)

    if not urls_to_process:
        logger.emit("job_complete", {"status": "No URLs to process. Exiting."}, level="INFO")
        save_run_cache(run_cache_data, logger) 
        return

    total_urls = len(urls_to_process)
    
    current_run_metrics = {
        "bonuses_new_total": 0, "downlines_new_total": 0, "errors_new_total": 0,
        "bonus_amount_new_total": 0.0
    }
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    base_data_dir = os.path.join(current_script_dir, "data")
    os.makedirs(base_data_dir, exist_ok=True)

    daily_bonus_csv_path = ""
    downline_csv_path = ""

    if config.settings.downline_enabled:
        downline_csv_path = os.path.join(base_data_dir, "downlines_master.csv")
        logger.emit("run_mode_info", {"mode": "Downline Scraping", "output_file": downline_csv_path}, level="INFO")
    else:
        today_date_str = datetime.now().strftime('%m-%d')
        daily_bonus_csv_path = os.path.join(base_data_dir, f"{today_date_str}_bonuses.csv")
        logger.emit("run_mode_info", {"mode": "Bonus Scraping", "output_file": daily_bonus_csv_path}, level="INFO")

    job_start_time = time.time()
    
    def format_stat_display(current_val, prev_val, width=STAT_ITEM_WIDTH): # Use the constant
        if current_val == 0 and prev_val == 0:
            return "-".ljust(width) 
        diff = current_val - prev_val
        s = f"{current_val}/{prev_val}({diff:+})"
        return s.ljust(width) 

    try:
        for idx, raw_url in enumerate(urls_to_process, 1):
            if idx > 1 and sys.stdout.isatty(): 
                sys.stdout.write('\x1b[3A') 
                sys.stdout.write('\x1b[J')  

            site_start_time = time.time()
            cleaned_url = auth_service.clean_url(raw_url) 
            site_key = cleaned_url 

            site_cache_entry = run_cache_data["sites"].get(site_key, {})
            pr_bonuses = site_cache_entry.get("last_run_new_bonuses", 0)
            prt_bonuses = site_cache_entry.get("cumulative_total_bonuses", 0)
            pr_downlines = site_cache_entry.get("last_run_new_downlines", 0)
            prt_downlines = site_cache_entry.get("cumulative_total_downlines", 0)
            pr_errors = site_cache_entry.get("last_run_new_errors", 0)
            prt_errors = site_cache_entry.get("cumulative_total_errors", 0)
            
            current_site_bonus_flags = {"C": False, "D": False, "S": False, "O": False} 
            cr_bonuses_site, cr_downlines_site, cr_errors_site = 0, 0, 0
            site_bonus_amount_new = 0.0

            auth_data = auth_service.login(cleaned_url, config.credentials.mobile, config.credentials.password, REQUEST_TIMEOUT)

            if not auth_data:
                cr_errors_site = 1
                logger.emit("login_summary", {"url": cleaned_url, "status": "failed"}, level="WARNING")
                unresponsive_sites_this_run.append(f"{cleaned_url} (Login Failed)")
            else:
                logger.emit("login_summary", {"url": cleaned_url, "merchant": auth_data.merchant_name, "status": "success"}, level="INFO")
                if config.settings.downline_enabled:
                    result_dl = scraper.fetch_downlines(cleaned_url, auth_data, downline_csv_path)
                    if isinstance(result_dl, str): 
                        cr_errors_site = 1
                        if result_dl == "UNRESPONSIVE": unresponsive_sites_this_run.append(f"{cleaned_url} (DL Fetch Unresponsive)")
                        else: unresponsive_sites_this_run.append(f"{cleaned_url} (DL Fetch Error)")
                    else: 
                        cr_downlines_site = result_dl
                else: 
                    result_bonuses = scraper.fetch_bonuses(cleaned_url, auth_data, daily_bonus_csv_path)
                    if isinstance(result_bonuses, str): 
                        cr_errors_site = 1
                        if result_bonuses == "UNRESPONSIVE": unresponsive_sites_this_run.append(f"{cleaned_url} (Bonus Fetch Unresponsive)")
                        else: unresponsive_sites_this_run.append(f"{cleaned_url} (Bonus Fetch Error)")
                    else: 
                        count, amount, flags = result_bonuses
                        cr_bonuses_site = count
                        site_bonus_amount_new = amount
                        current_site_bonus_flags = flags
            
            current_run_metrics["bonuses_new_total"] += cr_bonuses_site
            current_run_metrics["downlines_new_total"] += cr_downlines_site
            current_run_metrics["errors_new_total"] += cr_errors_site
            current_run_metrics["bonus_amount_new_total"] += site_bonus_amount_new

            run_cache_data["sites"].setdefault(site_key, {}) 
            run_cache_data["sites"][site_key].update({
                "last_run_new_bonuses": cr_bonuses_site,
                "cumulative_total_bonuses": prt_bonuses + cr_bonuses_site,
                "last_run_new_downlines": cr_downlines_site,
                "cumulative_total_downlines": prt_downlines + cr_downlines_site,
                "last_run_new_errors": cr_errors_site,
                "cumulative_total_errors": prt_errors + cr_errors_site,
                "bonus_flags": current_site_bonus_flags, 
                "last_processed_timestamp": datetime.now().isoformat()
            })
            
            site_processing_duration = time.time() - site_start_time
            percent_complete_normalized = (idx / total_urls) # Value between 0 and 1 for progress func
            progress_bar_str = progress(percent_complete_normalized, length=30, title="") 

            flags_str = f"[C]{'Y' if current_site_bonus_flags.get('C') else 'N'} " \
                        f"[D]{'Y' if current_site_bonus_flags.get('D') else 'N'} " \
                        f"[S]{'Y' if current_site_bonus_flags.get('S') else 'N'} " \
                        f"[O]{'Y' if current_site_bonus_flags.get('O') else 'N'}"
            
            line1 = f" {progress_bar_str} [{percent_complete_normalized*100:.2f}%] {idx}/{total_urls} "
            line2 = f" Site: {cleaned_url[:40].ljust(40)} | Time: {f'{site_processing_duration:.1f}s'.ljust(6)} | Run: #{str(run_cache_data['total_script_runs']).ljust(5)} | Flags: {flags_str} "
            
            sfs_display = run_cache_data["sites"][site_key]
            
            stat_b_curr, stat_b_prev = sfs_display['last_run_new_bonuses'], pr_bonuses
            stat_bt_curr, stat_bt_prev = sfs_display['cumulative_total_bonuses'], prt_bonuses
            stat_d_curr, stat_d_prev = sfs_display['last_run_new_downlines'], pr_downlines
            stat_dt_curr, stat_dt_prev = sfs_display['cumulative_total_downlines'], prt_downlines
            stat_e_curr, stat_e_prev = sfs_display['last_run_new_errors'], pr_errors
            stat_et_curr, stat_et_prev = sfs_display['cumulative_total_errors'], prt_errors

            stats_b_str = f"B: {format_stat_display(stat_b_curr, stat_b_prev)} (T: {format_stat_display(stat_bt_curr, stat_bt_prev)})"
            stats_d_str = f"D: {format_stat_display(stat_d_curr, stat_d_prev)} (T: {format_stat_display(stat_dt_curr, stat_dt_prev)})"
            stats_e_str = f"E: {format_stat_display(stat_e_curr, stat_e_prev)} (T: {format_stat_display(stat_et_curr, stat_et_prev)})"
            
            line3 = f" Stats: {stats_b_str} | {stats_d_str} | {stats_e_str} "
            
            display_line1 = line1.ljust(CONSOLE_TARGET_WIDTH)
            display_line2 = line2.ljust(CONSOLE_TARGET_WIDTH)
            display_line3 = line3.ljust(CONSOLE_TARGET_WIDTH)
            
            if sys.stdout.isatty():
                sys.stdout.write(f"{display_line1}\n{display_line2}\n{display_line3}\n")
                sys.stdout.flush()
            else: 
                if idx % 10 == 0 or idx == total_urls : 
                    logger.emit("progress_batch_update", {"processed": idx, "total": total_urls, "current_url": cleaned_url}, level="INFO")

        if sys.stdout.isatty(): sys.stdout.write("\n") 

        job_elapsed_time = time.time() - job_start_time
        avg_bonus_amount_this_run = (current_run_metrics["bonus_amount_new_total"] / current_run_metrics["bonuses_new_total"]) \
                                    if current_run_metrics["bonuses_new_total"] > 0 else 0.0
        
        job_summary_details = {
            "duration_seconds": round(job_elapsed_time, 2),
            "total_urls_processed": total_urls,
            "new_bonuses_fetched": current_run_metrics["bonuses_new_total"],
            "total_bonus_amount_new": round(current_run_metrics["bonus_amount_new_total"], 2),
            "avg_new_bonus_amount": round(avg_bonus_amount_this_run, 2),
            "new_downlines_fetched": current_run_metrics["downlines_new_total"],
            "new_errors_encountered": current_run_metrics["errors_new_total"],
            "unresponsive_or_failed_sites_count": len(unresponsive_sites_this_run)
        }
        logger.emit("job_complete", job_summary_details, level="INFO")

        if unresponsive_sites_this_run:
            logger.emit("down_sites_summary", {"sites": unresponsive_sites_this_run, "count": len(unresponsive_sites_this_run)}, level="WARNING")

    except KeyboardInterrupt:
        logger.emit("job_interrupted", {"message": "Scraping process interrupted by user (Ctrl+C)."}, level="WARNING")
        if sys.stdout.isatty(): sys.stdout.write("\nProcess interrupted.\n")
    except Exception as e:
        import traceback
        logger.emit("job_error_critical", {"error": str(e), "traceback": traceback.format_exc()}, level="CRITICAL")
        if sys.stdout.isatty(): print(f"\nCritical error occurred: {e}\nTraceback: {traceback.format_exc()}\n")
    finally:
        save_run_cache(run_cache_data, logger)
        logger.emit("shutdown", {"message": "CLI Scraper finished."}, level="INFO")
        logging.shutdown()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir) 
    
    run_scraper()


