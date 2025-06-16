import csv
import os
import requests
import time
import json
from dataclasses import dataclass
# Ensure all needed types are here for standalone clarity if ever needed, though main script passes them
from typing import Set, Tuple, Union, List, Optional, Dict, Any


# --- ABBREVIATIONS USED ---
# (Refer to Abbreviation Dictionary.md in the main project directory)

# --- DATACLASSES NEEDED BY THIS MODULE ---
@dataclass
class AuthData:
    merchant_id: str
    merchant_name: str # Included as part of AuthData standard structure
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

# --- LOGGER TYPE HINTING ---
# This placeholder is for type hinting within this module.
# The actual logger instance (from bonus.py) will be passed in.
class LoggerPlaceholder:
    def emit(self, event_msg: str, details: Optional[Dict[str, Any]] = None, severity_str: str = "INFO") -> None: pass
    # Add convenience methods if they were to be called directly within this module,
    # though it's more likely the main script calls logger.info, logger.debug etc.
    # For this module, relying on emit is sufficient as the real logger has it.
    def debug(self, event_msg: str, details: Optional[Dict[str, Any]] = None): self.emit(event_msg, details, "DEBUG")
    def info(self, event_msg: str, details: Optional[Dict[str, Any]] = None): self.emit(event_msg, details, "INFO")
    def warning(self, event_msg: str, details: Optional[Dict[str, Any]] = None): self.emit(event_msg, details, "WARNING")
    def error(self, event_msg: str, details: Optional[Dict[str, Any]] = None): self.emit(event_msg, details, "ERROR")


# --- DOWNLINE FETCHING LOGIC ---
def process_site_for_downlines(
    base_url: str, # This base_url is the one cleaned by bonus.py's AuthService
    auth: AuthData, # Contains the correctly formed auth.api_url
    csv_file_path: str,
    logger: LoggerPlaceholder, # Receives the actual Logger instance from bonus.py
    req_timeout: int
) -> Union[int, str]:
    """
    Fetches downline data for a single site and appends to CSV.
    Returns number of new downlines found or an error string ("UNRESPONSIVE", "ERROR").
    """
    written_keys: Set[Tuple] = set()
    # Ensure parent directory for CSV exists
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    # Pre-load keys from existing CSV to avoid duplicates
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0 :
        try:
            with open(csv_file_path, mode='r', newline="", encoding="utf-8") as f_read:
                reader = csv.DictReader(f_read)
                # Define key fields for a downline to check for existence
                key_fields = ['url', 'id', 'name', 'count', 'amount', 'register_date_time']
                for row in reader:
                    if all(k in row for k in key_fields):
                        # Ensure consistent formatting for amount when creating the key for lookup
                        try:
                            amount_float = float(row['amount'] or 0.0)
                            formatted_amount = f"{amount_float:.2f}"
                        except ValueError:
                            formatted_amount = "0.00" # Default if conversion fails
                        
                        written_keys.add((
                            row['url'], row['id'], row['name'],
                            str(row['count']), formatted_amount, row['register_date_time']
                        ))
                    else:
                        # This log will use the passed-in logger from bonus.py
                        logger.debug("csv_dnln_read_warn", {"file": csv_file_path, "msg": "Row missing keys in pre-load."})
        except Exception as e:
             logger.warning("csv_dnln_read_err", {"file": csv_file_path, "op": "read_downlines", "err": str(e)})


    total_new_rows = 0
    page_idx = 0
    while True:
        payload = {
            "level": "1", "pageIndex": str(page_idx), "module": "/referrer/getDownline",
            "merchantId": auth.merchant_id, "domainId": "0", "accessId": auth.access_id,
            "accessToken": auth.token, "walletIsAdmin": True
        }
        # Use auth.api_url for the request, which is already correctly formed by bonus.py
        logger.debug("api_dnln_req", {"url": auth.api_url, "mod": payload.get("module"), "page": page_idx})
        try:
            response = requests.post(auth.api_url, data=payload, timeout=req_timeout)
            response.raise_for_status()
            res = response.json()
            api_status = res.get("status")
            resp_details = {"url": auth.api_url, "mod": payload.get("module"), "api_stat": api_status}
            if api_status != "SUCCESS":
                resp_details["err_msg"] = res.get("message")
                data_f = res.get("data")
                if isinstance(data_f, dict): resp_details["err_desc"] = data_f.get("description")
                elif isinstance(data_f, str): resp_details["err_str"] = data_f
            logger.debug("api_dnln_resp", resp_details)
        except requests.exceptions.Timeout:
            logger.warning("api_dnln_timeout", {"url": auth.api_url, "mod": "/referrer/getDownline", "err": "Timeout"}); return "UNRESPONSIVE"
        except requests.exceptions.RequestException as e:
            logger.warning("api_dnln_req_exception", {"url": auth.api_url, "mod": "/referrer/getDownline", "err": str(e)}); return "UNRESPONSIVE"
        except json.JSONDecodeError:
            logger.error("api_dnln_json_err", {"url": auth.api_url, "mod": "/referrer/getDownline", "err": "JSON decode"}); return "ERROR"
        except Exception as e: # Catch-all for other unexpected errors
            logger.error("dnln_fetch_exception", {"err": f"DL fetch {auth.api_url} pg {page_idx} fail: {str(e)}"}); return "ERROR"

        if res.get("status") != "SUCCESS":
            logger.warning("api_dnln_status_err", {"url": auth.api_url, "mod":"/referrer/getDownline", "msg": res.get("message", "API non-SUCCESS")})
            return "ERROR"

        new_rows_page: List[Downline] = []
        dl_data_list = res.get("data", {}).get("downlines", [])
        if not isinstance(dl_data_list, list):
            logger.warning("api_dnln_data_warn", {"url": auth.api_url, "mod":"/referrer/getDownline", "msg": "DL data not list."})
            break # Stop if data format is unexpected

        for d_item in dl_data_list:
            if not isinstance(d_item, dict): continue
            try:
                # Ensure amount is float, handle None or empty string gracefully
                amount_val = float(d_item.get("amount", 0.0) or 0.0)
            except ValueError:
                amount_val = 0.0 # Default if conversion fails
            
            # base_url here is the cleaned scheme://netloc used for the 'url' field in CSV
            row_obj = Downline(
                url=base_url, id=str(d_item.get("id","")), name=str(d_item.get("name","")),
                count=int(d_item.get("count", 0) or 0), # Ensure int, handle None or empty string
                amount=amount_val,
                register_date_time=str(d_item.get("registerDateTime",""))
            )
            # Use a consistent key for checking if already written
            current_key = (row_obj.url, row_obj.id, row_obj.name, str(row_obj.count), f"{row_obj.amount:.2f}", row_obj.register_date_time)
            if current_key not in written_keys:
                new_rows_page.append(row_obj)
                written_keys.add(current_key)

        if not new_rows_page: # No new, unique downlines on this page
            break

        file_not_empty = os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0
        try:
            with open(csv_file_path, "a", newline="", encoding="utf-8") as f_app:
                fieldnames = [f.name for f in Downline.__dataclass_fields__.values()]
                writer = csv.DictWriter(f_app, fieldnames=fieldnames)
                if not file_not_empty: # Write header only if file is new or empty
                    writer.writeheader()
                writer.writerows([r.__dict__ for r in new_rows_page])
            logger.debug("csv_dnln_write", {"file": csv_file_path, "new_rows": len(new_rows_page), "page": page_idx})
        except Exception as e:
            logger.error("csv_dnln_write_err", {"file": csv_file_path, "op":"append_dl", "err": str(e)})
            return "ERROR" # Critical error if CSV can't be written

        total_new_rows += len(new_rows_page)
        page_idx += 1
        time.sleep(0.5) # Be respectful to the API

    logger.info("dnln_data_summary", {"url": base_url, "new_dl_count": total_new_rows})
    return total_new_rows

if __name__ == '__main__':
    # This module is intended to be imported by bonus.py, not run directly.
    print("Downline Processor Module - Not for direct execution.")
