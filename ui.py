import sys
import time
from typing import Deque

class UIHandler:
    def __init__(self):
        self.total = 0
        self.processed = 0
        self.errors = 0
        self.bonuses = 0

    def set_total_urls(self, total: int):
        self.total = total
        if total > 0:
            print(f"Starting scrape of {total} URLs...")

    def update(self, url: str, success: bool, count: int, tracker: Deque[float]):
        self.processed += 1
        self.bonuses += count
        if not success:
            self.errors += 1

        if not sys.stdout.isatty() or self.total == 0:
            return

        # Print a simple, single line for each update
        status = "SUCCESS" if success else "FAIL"
        progress = f"[{self.processed}/{self.total}]"
        print(f"{progress:<12} {status:<8} | Bonuses: {count:<4} | URL: {url}")


    def final(self, found: int, failed: int):
        print(f"\n{'='*40}\nScraping Complete")
        print(f"Total Bonuses Found: {found}")
        print(f"Failed URLs: {failed}")
        print("="*40)