# config.py 
import configparser
import sys
import os

def get_config(path: str = "config.ini") -> configparser.ConfigParser:
    if not os.path.exists(path):
        print(f"FATAL ERROR: Configuration file not found at '{path}'")
        sys.exit(1)
    
    config = configparser.ConfigParser(inline_comment_prefixes=(';', '#'))
    config.read(path)
    
    required = {
        "auth": ["username", "password"],
        "scraper": ["url_list_path"],
        "output": ["db_connection_string"],
        "logging": ["log_file_path"],
    }
    for section, keys in required.items():
        if not config.has_section(section):
            print(f"FATAL ERROR: Missing required section '[{section}]' in '{path}'")
            sys.exit(1)
        for key in keys:
            if not config.has_option(section, key):
                print(f"FATAL ERROR: Missing required key '{key}' in section '[{section}]'")
                sys.exit(1)
    return config