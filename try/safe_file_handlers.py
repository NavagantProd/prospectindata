# safe_file_handler.py
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import logging

logger = logging.getLogger("file_handler")

class SafeFileHandler:
    """Handles file operations safely with locking and backup."""
    
    def __init__(self,
                 output_dir: Path = Path("output"),
                 backup_dir: Path = Path("backup")):
        self.output_dir = output_dir
        self.backup_dir = backup_dir
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.backup_dir.mkdir(exist_ok=True, parents=True)
    
    def is_file_locked(self, file_path: Path) -> bool:
        if not file_path.exists():
            return False
        try:
            with open(file_path, 'a'):
                pass
            return False
        except (PermissionError, IOError):
            return True
    
    def wait_for_file_unlock(self, file_path: Path, max_wait: int = 30) -> bool:
        start = time.time()
        while self.is_file_locked(file_path):
            if time.time() - start > max_wait:
                return False
            logger.info(f"Waiting for unlock: {file_path}")
            time.sleep(2)
        return True
    
    def create_backup(self, file_path: Path) -> Optional[Path]:
        if not file_path.exists():
            return None
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        backup_path = self.backup_dir / backup_name
        try:
            shutil.copy2(file_path, backup_path)
            logger.info(f"Backup created: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None
    
    def save_dataframe_safely(self,
                              df: pd.DataFrame,
                              file_path: Path,
                              create_backup: bool = True) -> bool:
        try:
            file_path.parent.mkdir(exist_ok=True, parents=True)
            if create_backup and file_path.exists():
                self.create_backup(file_path)
            if not self.wait_for_file_unlock(file_path):
                logger.error(f"Locked: {file_path}")
                # try alternate name
                for i in range(1, 11):
                    alt = file_path.parent / f"{file_path.stem}_{i}{file_path.suffix}"
                    if not self.is_file_locked(alt):
                        file_path = alt
                        break
                else:
                    logger.error("No alternate filename unlocked")
                    return False
            df.to_csv(file_path, index=False)
            logger.info(f"Saved: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Save failed: {e}")
            return False
    
    def generate_output_filename(self, prefix: str = "enriched_leads") -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"{prefix}_{timestamp}.csv"
