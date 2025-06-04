import csv
import os
import sys
import time
import requests # Ensure this is imported
import logging
import json
import math
from dataclasses import dataclass, field # 'field' is not used but often imported with dataclass
from typing import List, Set, Tuple, Union, Optional, Dict, Any
from datetime import datetime, timedelta # 'timedelta' is not used but often imported
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
    def __init__(self, path: str = "config.ini"):
        if not os.path.exists(path):
            print(f"FATAL: Configuration file not found: {path}. Please create it.")
            sys.exit(1)
        self.config = configparser.ConfigParser()
        self.config.read(path)
        self._file_path = path # Store path for error messages

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
            if app_config.logging.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                print(f"Warning: Invalid log_level '{app_config.logging.log_level}' in '{self._file_path}'. Defaulting to INFO.")
                app_config.logging.log_level = "INFO"
            if app_config.logging.detail not in ["LESS", "MORE", "MAX"]:
                print(f"Warning: Invalid detail level '{app_config.logging.detail}' in '{self._file_path}'. Defaulting to LESS.")
                app_config.logging.detail = "LESS"
            return app_config
        except KeyError as e:
            print(f"FATAL: Configuration error in '{self._file_path}': Missing key {e}")
            sys.exit(1)
        except ValueError as e:
            print(f"FATAL: Configuration error in '{self._file_path}': Invalid value. {e}")
            sys.exit(1)
# --- END OF src/config.py ---

# --- START OF src/logger.py ---
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.module,
            "method": record.funcName,
            "event": record.msg,
            "details": record.details_data if hasattr(record, 'details_data') else {}
        }
        return json.dumps(log_record)

class Logger:
    VERBOSITY_LEVELS = {"LESS": 0, "MORE": 1, "MAX": 2}
    EVENT_VERBOSITY = {
        "job_start": "LESS", "job_complete": "LESS", "login_success": "MORE",
        "login_failed": "MORE", "api_request": "MORE", "api_response": "MORE",
        "bonus_fetched": "MORE", "downline_fetched": "MORE", "csv_written": "MORE",
        "exception": "LESS", "website_unresponsive": "LESS", "down_sites_summary": "LESS",
        "bonus_api_error": "MORE", "progress_update": "LESS", "cache_saved": "MORE",
        "cache_loaded_info": "MORE", "historical_data_written": "MORE",
        "historical_data_skipped": "MORE", "historical_data_error": "LESS",
        "comparison_report_generated": "MORE", "comparison_info": "MORE",
        "comparison_module_error": "LESS", "data_parsing_warning": "WARNING", # For new safe_get_float
        "data_parsing_error": "WARNING", # For new safe_get_float
        "logger_internal_error": "WARNING", "file_error": "ERROR", "file_loaded": "INFO",
        "run_mode_info": "INFO", "login_summary": "INFO", "api_error": "WARNING",
        "api_data_warning": "WARNING", "csv_warning": "DEBUG", "csv_error": "ERROR",
        "data_processing_error": "WARNING", "progress_batch_update": "INFO",
        "job_interrupted": "WARNING", "job_error_critical": "CRITICAL", "shutdown": "INFO"
    }

    def __init__(self, log_file: str, log_level: str, console: bool, detail: str):
        self.logger = logging.getLogger("CliScraperLogger")
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        self.verbosity = self.VERBOSITY_LEVELS.get(detail.upper(), 0)

        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setFormatter(JsonFormatter())
            self.logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not create file logger at {log_file}: {e}")

        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(event_msg)s", datefmt="%H:%M:%S"
            )
            class ConsoleEventFilter(logging.Filter):
                def filter(self, record):
                    details = record.details_data if hasattr(record, 'details_data') else {}
                    details_str = json.dumps(details)
                    if len(details_str) > 150 and record.levelname not in ('CRITICAL', 'ERROR'):
                        details_str = "{...too verbose...}"
                    elif len(details_str) > 500: # even for errors, cap it
                         details_str = details_str[:497] + "...}"
                    record.event_msg = f"{record.msg} - Details: {details_str}"
                    return True
            console_handler.addFilter(ConsoleEventFilter())
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
            self.emit("cache_loaded_info", {"message": f"Log file '{log_file}' for metrics not found."}, level="DEBUG")
            return metrics
        processed_lines = 0
        try:
            with open(log_file, "r", encoding='utf-8') as f:
                for line in f:
                    try:
                        log = json.loads(line)
                        processed_lines += 1
                        event = log.get("event")
                        details = log.get("details", {})
                        if event == "bonus_fetched":
                            metrics["bonuses"] += details.get("count", 0)
                            metrics["total_bonus_amount"] += float(details.get("total_amount", 0.0))
                            metrics["successful_bonus_fetches"] += 1
                        elif event == "downline_fetched": metrics["downlines"] += details.get("count", 0)
                        elif event in ["exception", "website_unresponsive"]: metrics["errors"] += 1
                        elif event == "bonus_api_error":
                            metrics["failed_bonus_api_calls"] += 1
                            metrics["errors"] += 1
                        elif event == "job_complete":
                            metrics["runs"] += 1
                            metrics["total_runtime"] += float(details.get("duration", 0.0))
                    except json.JSONDecodeError: continue
            self.emit("cache_loaded_info", {"message": f"Loaded metrics from {processed_lines} lines in '{log_file}'."}, level="DEBUG")
        except Exception as e:
            self.emit("logger_internal_error", {"error": f"Loading metrics from '{log_file}': {str(e)}"}, level="ERROR")
        return metrics
# --- END OF src/logger.py ---

# --- START OF src/utils.py ---
CACHE_FILE_PATH = "data/run_metrics_cache.json"
def load_run_cache(logger_instance: Optional[Logger] = None) -> dict:
    default_cache = {"total_script_runs": 0, "sites": {}}
    cache_dir = os.path.dirname(CACHE_FILE_PATH)
    if cache_dir and not os.path.exists(cache_dir): # Ensure 'data' directory exists
        try:
            os.makedirs(cache_dir, exist_ok=True)
        except OSError as e:
            if logger_instance: logger_instance.emit("cache_error", {"error": f"Could not create cache directory '{cache_dir}': {e}"}, level="ERROR")
            else: print(f"Error: Could not create cache directory '{cache_dir}': {e}")
            return default_cache # Return default if dir creation fails

    if not os.path.exists(CACHE_FILE_PATH):
        msg = f"Cache file '{CACHE_FILE_PATH}' not found. Initializing."
        if logger_instance: logger_instance.emit("cache_loaded_info", {"message": msg}, level="INFO")
        else: print(f"Info: {msg}")
        return default_cache
    try:
        with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f: data = json.load(f)
        if "total_script_runs" not in data or "sites" not in data:
            msg = f"Cache file '{CACHE_FILE_PATH}' malformed. Re-initializing."
            if logger_instance: logger_instance.emit("cache_loaded_info", {"message": msg}, level="WARNING")
            else: print(f"Warning: {msg}")
            return default_cache
        if logger_instance: logger_instance.emit("cache_loaded_info", {"message": f"Loaded run cache from '{CACHE_FILE_PATH}'."}, level="INFO")
        return data
    except (json.JSONDecodeError, FileNotFoundError) as e:
        msg = f"Error loading cache '{CACHE_FILE_PATH}': {e}. Re-initializing."
        if logger_instance: logger_instance.emit("cache_loaded_info", {"message": msg}, level="WARNING")
        else: print(f"Warning: {msg}")
        return default_cache

def save_run_cache(data: dict, logger_instance: Optional[Logger] = None):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)
        with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
        if logger_instance: logger_instance.emit("cache_saved", {"path": CACHE_FILE_PATH, "total_script_runs": data.get("total_script_runs")}, level="INFO")
    except Exception as e:
        msg = f"Could not save cache to '{CACHE_FILE_PATH}': {e}"
        if logger_instance: logger_instance.emit("cache_error", {"error": msg}, level="ERROR")
        else: print(f"Error: {msg}")

def progress(value, length=40, title=" ", vmin=0.0, vmax=1.0) -> str:
    blocks = ["", "▏","▎","▍","▌","▋","▊","▉","█"]
    lsep, rsep = "|", "|"
    value = min(max(value, vmin), vmax)
    value = (value - vmin) / float(vmax - vmin) if (vmax - vmin) != 0 else 0.0
    v = value * length
    x = math.floor(v)
    y = v - x
    base = 0.125
    i = min(len(blocks) - 1, int(round(base * math.floor(float(y) / base), 3) / base if base != 0 else 0))
    bar = "█" * x + blocks[i]
    n = length - len(bar)
    bar_str = lsep + bar + " " * n + rsep
    return f"{title}{bar_str} {value*100:.1f}%"
# --- END OF src/utils.py ---

# --- START OF src/auth.py ---
import re
class AuthService:
    API_PATH = "/api/v1/index.php"
    def __init__(self, logger: Logger): self.logger = logger

    @staticmethod
    def clean_url(url: str) -> str:
        url = re.sub(r"/(RF|GA|GX|JK|GA|TB|AU|EWAY|MNP|LuckyNum|FORTUNE|BC|King|UU|EZ|AVG|YAYA|ACWIN|OW|CN|AUSSIE|BETJOHN|REF)[\w]+$", "", url)
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
            self.logger.emit("website_unresponsive", {"url": url, "error": "Timeout initial load"}, level="WARNING")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.emit("website_unresponsive", {"url": url, "error": f"Fetch URL failed: {str(e)}"}, level="WARNING")
            return None
        except Exception as e:
            self.logger.emit("exception", {"url": url, "context": "Initial load", "error": str(e)}, level="ERROR")
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
        self.logger.emit("api_request", {"url": api_url_full, "action": "login", "module": "/users/login"}, level="DEBUG")
        try:
            response = requests.post(api_url_full, data=payload, timeout=request_timeout)
            response.raise_for_status()
            res_json = response.json()
            api_status = res_json.get("status", "UNKNOWN_STATUS")
            response_details = {"url": api_url_full, "action": "login", "status_code": response.status_code, "api_status": api_status}
            if api_status != "SUCCESS":
                response_details["error_message"] = res_json.get("message")
                data_field = res_json.get("data")
                if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                elif isinstance(data_field, str): response_details["error_data_string"] = data_field
            self.logger.emit("api_response", response_details, level="DEBUG")
            data = res_json.get("data", {})
            if not isinstance(data, dict) or not data.get("token"):
                self.logger.emit("login_failed", {"url": url, "reason": res_json.get("message", "No token/data not dict")}, level="WARNING")
                return None
            self.logger.emit("login_success", {"url": url, "merchant_name": merchant_name}, level="INFO")
            return AuthData(merchant_id, merchant_name, data.get("id"), data.get("token"), api_url_full)
        except requests.exceptions.Timeout:
            self.logger.emit("website_unresponsive", {"url": api_url_full, "action": "login", "error": "Timeout login API"}, level="WARNING")
        except requests.exceptions.RequestException as e:
            self.logger.emit("website_unresponsive", {"url": api_url_full, "action": "login", "error": f"Login API failed: {str(e)}"}, level="WARNING")
        except json.JSONDecodeError:
            self.logger.emit("exception", {"url": api_url_full, "action": "login", "error": "JSONDecodeError login API"}, level="ERROR")
        except Exception as e:
            self.logger.emit("exception", {"url": url, "context": "Login process", "error": str(e)}, level="ERROR")
        return None
# --- END OF src/auth.py ---

# --- START OF Scraper class and load_urls (from src/main.py) ---
class Scraper:
    def __init__(self, logger: Logger, request_timeout: int):
        self.logger = logger
        self.request_timeout = request_timeout

    def _safe_get_float(self, data_dict: dict, key: str, default_value: float = 0.0) -> float:
        raw_value = data_dict.get(key)
        if isinstance(raw_value, (int, float)):
            return float(raw_value)
        if isinstance(raw_value, str):
            if raw_value.strip() == "": return default_value
            try: return float(raw_value)
            except ValueError:
                self.logger.emit("data_parsing_warning", {"field": key, "raw_value": raw_value, "error": "ValueError: could not convert string to float."}, level="DEBUG")
                return default_value
        if raw_value is None:
            return default_value
        self.logger.emit("data_parsing_error", {"field": key, "raw_value_type": str(type(raw_value)), "raw_value_preview": str(raw_value)[:100], "error": "Unexpected type for float conversion, using default."}, level="WARNING")
        return default_value

    def fetch_downlines(self, base_url: str, auth: AuthData, csv_file: str) -> Union[int, str]:
        written_keys: Set[Tuple] = set()
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0 :
            try:
                with open(csv_file, mode='r', newline="", encoding="utf-8") as f_read:
                    reader = csv.DictReader(f_read)
                    key_fields = ['url', 'id', 'name', 'count', 'amount', 'register_date_time']
                    for row in reader:
                        if all(k in row for k in key_fields): written_keys.add(tuple(row[k] for k in key_fields))
                        else: self.logger.emit("csv_warning", {"file": csv_file, "message": "Row missing keys skipped."}, level="DEBUG")
            except Exception as e: self.logger.emit("csv_error", {"file": csv_file, "op": "read_downlines", "error": str(e)}, level="WARNING")
        total_new_rows, page = 0, 0
        while True:
            payload = {"level": "1", "pageIndex": str(page), "module": "/referrer/getDownline", "merchantId": auth.merchant_id, "domainId": "0", "accessId": auth.access_id, "accessToken": auth.token, "walletIsAdmin": True}
            self.logger.emit("api_request", {"url": auth.api_url, "module": "/referrer/getDownline", "page": page}, level="DEBUG")
            try:
                response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
                response.raise_for_status()
                res = response.json()
                api_status = res.get("status", "UNKNOWN_STATUS")
                response_details = {"url": auth.api_url, "module": "/referrer/getDownline", "api_status": api_status}
                if api_status != "SUCCESS":
                    response_details["error_message"] = res.get("message")
                    data_field = res.get("data")
                    if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                    elif isinstance(data_field, str): response_details["error_data_string"] = data_field
                self.logger.emit("api_response", response_details, level="DEBUG")
            except requests.exceptions.Timeout: self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/referrer/getDownline", "error": "Timeout"}, level="WARNING"); return "UNRESPONSIVE"
            except requests.exceptions.RequestException as e: self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/referrer/getDownline", "error": str(e)}, level="WARNING"); return "UNRESPONSIVE"
            except json.JSONDecodeError: self.logger.emit("exception", {"url": auth.api_url, "module": "/referrer/getDownline", "error": "JSONDecodeError"}, level="ERROR"); return "ERROR"
            except Exception as e: self.logger.emit("exception", {"error": f"Downline fetch {auth.api_url} page {page}: {str(e)}"}, level="ERROR"); return "ERROR"
            if api_status != "SUCCESS": self.logger.emit("api_error", {"url": auth.api_url, "module":"/referrer/getDownline", "message": res.get("message", "API non-SUCCESS")}, level="WARNING"); return "ERROR"
            new_rows_this_page: List[Downline] = []
            downlines_data = res.get("data", {}).get("downlines", [])
            if not isinstance(downlines_data, list): self.logger.emit("api_data_warning", {"url": auth.api_url, "module":"/referrer/getDownline", "message": "Downlines data not list."}, level="WARNING"); break
            for d_data in downlines_data:
                if not isinstance(d_data, dict): continue
                row = Downline(
                    url=base_url, id=str(d_data.get("id","")), name=str(d_data.get("name","")),
                    count=int(self._safe_get_float(d_data, "count")), # count often int
                    amount=self._safe_get_float(d_data, "amount"),
                    register_date_time=str(d_data.get("registerDateTime",""))
                )
                current_key = (row.url, row.id, row.name, str(row.count), f"{row.amount:.2f}", row.register_date_time)
                if current_key not in written_keys: new_rows_this_page.append(row); written_keys.add(current_key)
            if not new_rows_this_page: break
            file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as f_append:
                    fieldnames = [f.name for f in Downline.__dataclass_fields__.values()]
                    writer = csv.DictWriter(f_append, fieldnames=fieldnames)
                    if not file_exists_and_not_empty: writer.writeheader()
                    writer.writerows([r.__dict__ for r in new_rows_this_page])
                self.logger.emit("csv_written", {"file": csv_file, "new_rows": len(new_rows_this_page), "page": page}, level="DEBUG")
            except Exception as e: self.logger.emit("csv_error", {"file": csv_file, "op":"append_downlines", "error": str(e)}, level="ERROR"); return "ERROR"
            total_new_rows += len(new_rows_this_page); page += 1; time.sleep(0.5)
        self.logger.emit("downline_fetched", {"url": base_url, "total_new_downlines": total_new_rows}, level="INFO")
        return total_new_rows

    def fetch_bonuses(self, base_url: str, auth: AuthData, csv_file: str) -> Union[Tuple[int, float, Dict[str, bool]], str]:
        C_KEYWORDS, D_KEYWORDS, S_KEYWORDS = ["commission", "affiliate"], ["downline first deposit"], ["share bonus", "referrer"]
        bonus_type_flags = {"C": False, "D": False, "S": False, "O": False}
        payload = {"module": "/users/syncData", "merchantId": auth.merchant_id, "domainId": "0", "accessId": auth.access_id, "accessToken": auth.token, "walletIsAdmin": ""}
        self.logger.emit("api_request", {"url": auth.api_url, "module": "/users/syncData"}, level="DEBUG")
        try:
            response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
            response.raise_for_status()
            res = response.json()
            api_status = res.get("status", "UNKNOWN_STATUS")
            response_details = {"url": auth.api_url, "module": "/users/syncData", "api_status": api_status}
            if api_status != "SUCCESS":
                response_details["error_message"] = res.get("message")
                data_field = res.get("data")
                if isinstance(data_field, dict): response_details["error_description"] = data_field.get("description")
                elif isinstance(data_field, str): response_details["error_data_string"] = data_field
            self.logger.emit("api_response", response_details, level="DEBUG")
        except requests.exceptions.Timeout: self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/users/syncData", "error": "Timeout"}, level="WARNING"); return "UNRESPONSIVE"
        except requests.exceptions.RequestException as e: self.logger.emit("website_unresponsive", {"url": auth.api_url, "module": "/users/syncData", "error": str(e)}, level="WARNING"); return "UNRESPONSIVE"
        except json.JSONDecodeError: self.logger.emit("exception", {"url": auth.api_url, "module": "/users/syncData", "error": "JSONDecodeError"}, level="ERROR"); return "ERROR"
        except Exception as e: self.logger.emit("exception", {"error": f"Bonus fetch {auth.api_url}: {str(e)}"}, level="ERROR"); return "ERROR"
        if api_status != "SUCCESS": self.logger.emit("bonus_api_error", {"url": auth.api_url, "status": api_status, "error_message": res.get("message", "N/A"), "error_data": res.get("data", "N/A")}, level="WARNING"); return "ERROR"
        
        bonus_data_list = res.get("data", {}).get("bonus", [])
        promo_data_list = res.get("data", {}).get("promotions", [])
        # Ensure both are lists before concatenating
        bonuses_data_raw = (bonus_data_list if isinstance(bonus_data_list, list) else []) + \
                           (promo_data_list if isinstance(promo_data_list, list) else [])

        if not bonuses_data_raw:
            self.logger.emit("bonus_fetched", {"url": base_url, "count": 0, "total_amount": 0.0}, level="INFO")
            return 0, 0.0, bonus_type_flags
        
        rows_to_write_obj: List[Bonus] = []
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        for b_data in bonuses_data_raw:
            if not isinstance(b_data, dict): continue
            
            bonus_f = self._safe_get_float(b_data, "bonusFixed")
            min_w = self._safe_get_float(b_data, "minWithdraw")
            ratio = min_w / bonus_f if bonus_f != 0 else None

            bonus_instance = Bonus(
                url=base_url, merchant_name=str(auth.merchant_name or ""), id=str(b_data.get("id","")),
                name=str(b_data.get("name","")), transaction_type=str(b_data.get("transactionType","")),
                bonus_fixed=bonus_f, amount=self._safe_get_float(b_data, "amount"),
                min_withdraw=min_w, max_withdraw=self._safe_get_float(b_data, "maxWithdraw"),
                withdraw_to_bonus_ratio=ratio, rollover=self._safe_get_float(b_data, "rollover"),
                balance=str(b_data.get("balance", "")), claim_config=str(b_data.get("claimConfig", "")),
                claim_condition=str(b_data.get("claimCondition", "")), bonus=str(b_data.get("bonus", "")),
                bonus_random=str(b_data.get("bonusRandom", "")), reset=str(b_data.get("reset", "")),
                min_topup=self._safe_get_float(b_data, "minTopup"),
                max_topup=self._safe_get_float(b_data, "maxTopup"),
                refer_link=str(b_data.get("referLink", ""))
            )
            rows_to_write_obj.append(bonus_instance)
            name_lower, claim_config_lower = bonus_instance.name.lower(), bonus_instance.claim_config.lower()
            matched_specific_type = False
            if any(kw in name_lower or kw in claim_config_lower for kw in C_KEYWORDS): bonus_type_flags["C"] = True; matched_specific_type = True
            if any(kw in name_lower or kw in claim_config_lower for kw in D_KEYWORDS): bonus_type_flags["D"] = True; matched_specific_type = True
            if any(kw in name_lower or kw in claim_config_lower for kw in S_KEYWORDS): bonus_type_flags["S"] = True; matched_specific_type = True
            if not matched_specific_type: bonus_type_flags["O"] = True
        
        if rows_to_write_obj:
            file_exists, is_empty = os.path.exists(csv_file), (not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0)
            try:
                with open(csv_file, "a", newline="", encoding="utf-8") as f_append:
                    fieldnames = [f.name for f in Bonus.__dataclass_fields__.values()]
                    writer = csv.DictWriter(f_append, fieldnames=fieldnames)
                    if is_empty: writer.writeheader()
                    writer.writerows([b.__dict__ for b in rows_to_write_obj])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write_obj)}, level="DEBUG")
            except Exception as e: self.logger.emit("csv_error", {"file": csv_file, "op":"append_bonuses", "error": str(e)}, level="ERROR"); return "ERROR"
        current_fetch_total_amount = sum(b.amount for b in rows_to_write_obj)
        self.logger.emit("bonus_fetched", {"url": base_url, "count": len(rows_to_write_obj), "total_amount": current_fetch_total_amount}, level="INFO")
        return len(rows_to_write_obj), current_fetch_total_amount, bonus_type_flags

def load_urls(url_file_path: str, logger: Logger) -> List[str]:
    if not os.path.exists(url_file_path):
        logger.emit("file_error", {"message": f"URL file not found: {url_file_path}"}, level="ERROR")
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
    config_loader = ConfigLoader(path="config.ini")
    try: config = config_loader.load()
    except SystemExit: return

    log_file_path = config.logging.log_file
    if not os.path.isabs(log_file_path) and hasattr(sys, '_MEIPASS'): # PyInstaller bundle
        log_file_path = os.path.join(os.path.dirname(sys.executable), log_file_path)
    elif not os.path.isabs(log_file_path): # Relative path from script dir
        log_file_path = os.path.join(os.path.dirname(__file__), log_file_path)
    
    logger = Logger(log_file_path, config.logging.log_level, config.logging.console, config.logging.detail)
    logger.emit("job_start", {"message": "CLI Scraper starting.", "config_loaded": True}, level="INFO")

    run_cache_data = load_run_cache(logger)
    run_cache_data["total_script_runs"] = run_cache_data.get("total_script_runs", 0) + 1
    REQUEST_TIMEOUT, unresponsive_sites_this_run = 30, []
    auth_service, scraper = AuthService(logger), Scraper(logger, REQUEST_TIMEOUT)
    
    url_file_input_path = config.settings.url_file
    if not os.path.isabs(url_file_input_path) and hasattr(sys, '_MEIPASS'):
         url_file_input_path = os.path.join(os.path.dirname(sys.executable), url_file_input_path)
    elif not os.path.isabs(url_file_input_path):
        url_file_input_path = os.path.join(os.path.dirname(__file__), url_file_input_path)
    urls_to_process = load_urls(url_file_input_path, logger)

    if not urls_to_process:
        logger.emit("job_complete", {"status": "No URLs. Exiting."}, level="INFO")
        save_run_cache(run_cache_data, logger); return

    total_urls = len(urls_to_process)
    current_run_metrics = {"bonuses_new_total": 0, "downlines_new_total": 0, "errors_new_total": 0, "bonus_amount_new_total": 0.0}
    
    base_data_dir = "data" # Relative to script dir (due to os.chdir)
    os.makedirs(base_data_dir, exist_ok=True)
    output_csv_path = ""
    if config.settings.downline_enabled:
        output_csv_path = os.path.join(base_data_dir, "downlines_master.csv")
        logger.emit("run_mode_info", {"mode": "Downline Scraping", "output_file": output_csv_path}, level="INFO")
    else:
        output_csv_path = os.path.join(base_data_dir, f"{datetime.now().strftime('%m-%d')}_bonuses.csv")
        logger.emit("run_mode_info", {"mode": "Bonus Scraping", "output_file": output_csv_path}, level="INFO")

    job_start_time = time.time()
    def format_stat_display(curr, prev): return f"{curr}/{prev}({curr-prev:+})" if curr != 0 or prev != 0 else "-"

    try:
        for idx, raw_url in enumerate(urls_to_process, 1):
            if idx > 1 and sys.stdout.isatty(): sys.stdout.write('\x1b[3A\x1b[J')
            site_start_time = time.time()
            cleaned_url = auth_service.clean_url(raw_url)
            site_key = cleaned_url
            site_cache = run_cache_data["sites"].get(site_key, {})
            pr_b, prt_b = site_cache.get("last_run_new_bonuses",0), site_cache.get("cumulative_total_bonuses",0)
            pr_d, prt_d = site_cache.get("last_run_new_downlines",0), site_cache.get("cumulative_total_downlines",0)
            pr_e, prt_e = site_cache.get("last_run_new_errors",0), site_cache.get("cumulative_total_errors",0)
            cr_b_site, cr_d_site, cr_e_site, site_b_amt_new = 0,0,0,0.0
            site_bonus_flags = {"C":False,"D":False,"S":False,"O":False}
            auth_data = auth_service.login(cleaned_url, config.credentials.mobile, config.credentials.password, REQUEST_TIMEOUT)
            if not auth_data:
                cr_e_site = 1; logger.emit("login_summary", {"url":cleaned_url, "status":"failed"},level="WARNING"); unresponsive_sites_this_run.append(f"{cleaned_url} (Login Fail)")
            else:
                logger.emit("login_summary", {"url":cleaned_url, "merchant":auth_data.merchant_name, "status":"success"},level="INFO")
                if config.settings.downline_enabled:
                    res_dl = scraper.fetch_downlines(cleaned_url, auth_data, output_csv_path)
                    if isinstance(res_dl, str): cr_e_site=1; unresponsive_sites_this_run.append(f"{cleaned_url} (DL {res_dl})")
                    else: cr_d_site = res_dl
                else:
                    res_b = scraper.fetch_bonuses(cleaned_url, auth_data, output_csv_path)
                    if isinstance(res_b, str): cr_e_site=1; unresponsive_sites_this_run.append(f"{cleaned_url} (Bonus {res_b})")
                    else: cr_b_site, site_b_amt_new, site_bonus_flags = res_b
            current_run_metrics["bonuses_new_total"] += cr_b_site; current_run_metrics["downlines_new_total"] += cr_d_site
            current_run_metrics["errors_new_total"] += cr_e_site; current_run_metrics["bonus_amount_new_total"] += site_b_amt_new
            run_cache_data["sites"].setdefault(site_key, {}).update({
                "last_run_new_bonuses":cr_b_site, "cumulative_total_bonuses":prt_b+cr_b_site,
                "last_run_new_downlines":cr_d_site, "cumulative_total_downlines":prt_d+cr_d_site,
                "last_run_new_errors":cr_e_site, "cumulative_total_errors":prt_e+cr_e_site,
                "bonus_flags":site_bonus_flags, "last_processed_timestamp":datetime.now().isoformat()})
            site_dur, pc = time.time()-site_start_time, idx/total_urls
            prog_bar = progress(pc, length=30, title="")
            flags_s = f"[C]{'Y' if site_bonus_flags.get('C') else 'N'} [D]{'Y'if site_bonus_flags.get('D') else 'N'} [S]{'Y'if site_bonus_flags.get('S') else 'N'} [O]{'Y'if site_bonus_flags.get('O') else 'N'}"
            l1,l2 = f" {prog_bar} [{pc*100:.2f}%] {idx}/{total_urls} ", f" Site: {cleaned_url[:40].ljust(40)} | T: {site_dur:.1f}s | Run: #{run_cache_data['total_script_runs']} | Flags: {flags_s} "
            sfs_d = run_cache_data["sites"][site_key]
            sb,sd,se = f"B:{format_stat_display(sfs_d['last_run_new_bonuses'],pr_b)}(T:{format_stat_display(sfs_d['cumulative_total_bonuses'],prt_b)})", \
                        f"D:{format_stat_display(sfs_d['last_run_new_downlines'],pr_d)}(T:{format_stat_display(sfs_d['cumulative_total_downlines'],prt_d)})", \
                        f"E:{format_stat_display(sfs_d['last_run_new_errors'],pr_e)}(T:{format_stat_display(sfs_d['cumulative_total_errors'],prt_e)})"
            l3 = f" Stats: {sb} | {sd} | {se} "
            if sys.stdout.isatty(): sys.stdout.write(f"{l1}\n{l2}\n{l3}\n"); sys.stdout.flush()
            else:
                if idx % 10 == 0 or idx == total_urls: logger.emit("progress_batch_update", {"proc":idx,"total":total_urls,"curr":cleaned_url},level="INFO")
        if sys.stdout.isatty(): sys.stdout.write("\n")
        job_elapsed = time.time()-job_start_time
        avg_b_amt = (current_run_metrics["bonus_amount_new_total"]/current_run_metrics["bonuses_new_total"]) if current_run_metrics["bonuses_new_total"] > 0 else 0.0
        summary = {"dur_s":round(job_elapsed,2),"urls":total_urls,"new_b":current_run_metrics["bonuses_new_total"],
                   "b_amt_new":round(current_run_metrics["bonus_amount_new_total"],2),"avg_b_amt":round(avg_b_amt,2),
                   "new_d":current_run_metrics["downlines_new_total"],"new_e":current_run_metrics["errors_new_total"],
                   "failed_sites_count":len(unresponsive_sites_this_run)}
        logger.emit("job_complete", summary, level="INFO")
        if unresponsive_sites_this_run: logger.emit("down_sites_summary", {"sites":unresponsive_sites_this_run,"count":len(unresponsive_sites_this_run)},level="WARNING")
    except KeyboardInterrupt: logger.emit("job_interrupted", {"message":"User interrupted (Ctrl+C)."},level="WARNING"); print("\nProcess interrupted.")
    except Exception as e: import traceback; logger.emit("job_error_critical", {"error":str(e),"traceback":traceback.format_exc()},level="CRITICAL"); print(f"\nCritical error: {e}\n")
    finally: save_run_cache(run_cache_data,logger); logger.emit("shutdown",{"message":"CLI Scraper finished."},level="INFO"); logging.shutdown()

if __name__ == "__main__":
    # Determine script directory, handling PyInstaller's _MEIPASS
    if hasattr(sys, '_MEIPASS'):
        # In a PyInstaller bundle, __file__ is not reliable for the original script location
        # sys.executable is the path to the bundled .exe
        # We want files relative to the .exe, so no os.chdir needed if files are bundled with it
        # or if paths are configured to be absolute or found relative to sys.executable
        # For this script, assuming config.ini, urls.txt are in same dir as .exe
        # And data/logs are subdirs there.
        pass # Handled in run_scraper for individual file paths
    else:
        # Standard Python execution, chdir to script's own directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
    run_scraper()

