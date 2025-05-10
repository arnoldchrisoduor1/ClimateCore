import os
import json
import shutil
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import hashlib

from ..utils.logger import logger
from ..config import DATA_VERSION_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from ..database.connection import get_db_session
from ..database.operations import DatabaseOperations

class DataVersioner:
    """Class for managing data versions"""
    
    def __init__(self):
        self.version_dir = DATA_VERSION_DIR
        self.processed_dir = PROCESSED_DATA_DIR
        self.raw_dir = RAW_DATA_DIR
        
        # Ensure the version directory exists
        os.makedirs(self.version_dir, exist_ok=True)
        
        self.version_registry_file = os.path.join(self.version_dir, "version_registry.json")
        self._initialize_registry()
    
    def _initialize_registry(self):
        """Initialize the version registry if it doesn't exist"""
        if not os.path.exists(self.version_registry_file):
            registry = {
                "versions": [],
                "latest_version": None,
                "created_at": datetime.utcnow().isoformat()
            }
            
            with open(self.version_registry_file, 'w') as f:
                json.dump(registry, f, indent=2)
            
            logger.info("Initialized empty version registry")
    
    def _load_registry(self) -> Dict[str, Any]:
        """Load the version registry"""
        with open(self.version_registry_file, 'r') as f:
            return json.load(f)
    
    def _save_registry(self, registry: Dict[str, Any]):
        """Save the version registry"""
        with open(self.version_registry_file, 'w') as f:
            json.dump(registry, f, indent=2)
    
    def create_version(self, name: str, description: str = None, 
                      parent_version_id: str = None) -> str:
        """Create a new data version"""
        registry = self._load_registry()
        
        # Generate a new version ID
        version_id = str(uuid.uuid4())
        
        # Create a directory for this version
        version_dir = os.path.join(self.version_dir, version_id)
        os.makedirs(version_dir, exist_ok=True)
        
        # If no parent is specified but versions exist, use the latest as parent
        if parent_version_id is None and registry["versions"]:
            parent_version_id = registry["latest_version"]
        
        # Create the version record
        version_record = {
            "id": version_id,
            "name": name,
            "description": description,
            "parent_id": parent_version_id,
            "created_at": datetime.utcnow().isoformat(),
            "data_files": [],
            "metadata": {
                "raw_files_count": len(os.listdir(self.raw_dir)),
                "processed_files_count": len(os.listdir(self.processed_dir))
            }
        }
        
        # Add to registry and update latest
        registry["versions"].append(version_record)
        registry["latest_version"] = version_id
        
        # Save the registry
        self._save_registry(registry)
        
        # Save version info to its directory
        with open(os.path.join(version_dir, "version_info.json"), 'w') as f:
            json.dump(version_record, f, indent=2)
        
        logger.info(f"Created new data version: {version_id} - {name}")
        
        # Create in database as well
        with get_db_session() as session:
            DatabaseOperations.create_data_version(
                session, name, description, parent_version_id
            )
        
        return version_id
    
    def add_data_to_version(self, version_id: str, 
                           dataframe: pd.DataFrame, 
                           data_type: str) -> str:
        """Add data to a version"""
        registry = self._load_registry()
        
        # Find the version
        version = None
        for v in registry["versions"]:
            if v["id"] == version_id:
                version = v
                break
        
        if not version:
            logger.error(f"Version not found: {version_id}")
            return None
        
        # Create a filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{timestamp}.parquet"
        filepath = os.path.join(self.version_dir, version_id, filename)
        
        # Save the data
        dataframe.to_parquet(filepath, index=False)
        
        # Calculate hash for data integrity
        hash_value = self._calculate_data_hash(dataframe)
        
        # Add to version's data files
        file_info = {
            "filename": filename,
            "data_type": data_type,
            "created_at": datetime.utcnow().isoformat(),
            "rows": len(dataframe),
            "columns": list(dataframe.columns),
            "hash": hash_value
        }
        
        version["data_files"].append(file_info)
        
        # Update registry
        self._save_registry(registry)
        
        # Update version info file
        version_info_path = os.path.join(self.version_dir, version_id, "version_info.json")
        with open(version_info_path, 'w') as f:
            json.dump(version, f, indent=2)
        
        logger.info(f"Added {filename} to version {version_id}")
        return filepath
    
    def _calculate_data_hash(self, dataframe: pd.DataFrame) -> str:
        """Calculate a hash of the dataframe for data integrity checking"""
        # Convert to string for hashing
        data_str = dataframe.to_json(orient='records')
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def get_version_data(self, version_id: str, data_type: str = None) -> pd.DataFrame:
        """Get data from a specific version"""
        version_dir = os.path.join(self.version_dir, version_id)
        
        if not os.path.exists(version_dir):
            logger.error(f"Version directory not found: {version_id}")
            return pd.DataFrame()
        
        # Load version info
        with open(os.path.join(version_dir, "version_info.json"), 'r') as f:
            version_info = json.load(f)
        
        # Filter files by data type if specified
        data_files = version_info["data_files"]
        if data_type:
            data_files = [f for f in data_files if f["data_type"] == data_type]
        
        if not data_files:
            logger.warning(f"No data files found for version {version_id} and type {data_type}")
            return pd.DataFrame()
        
        # Load all data files
        dfs = []
        for file_info in data_files:
            filepath = os.path.join(version_dir, file_info["filename"])
            if os.path.exists(filepath) and filepath.endswith('.parquet'):
                df = pd.read_parquet(filepath)
                dfs.append(df)
            else:
                logger.warning(f"Data file not found or not parquet: {filepath}")
        
        if not dfs:
            return pd.DataFrame()
        
        # Combine all dataframes
        return pd.concat(dfs, ignore_index=True)
    
    def get_version_history(self) -> List[Dict[str, Any]]:
        """Get the version history"""
        registry = self._load_registry()
        return registry["versions"]
    
    def get_latest_version(self) -> Optional[str]:
        """Get the latest version ID"""
        registry = self._load_registry()
        return registry["latest_version"]
    
    def verify_version_integrity(self, version_id: str) -> Dict[str, Any]:
        """Verify the integrity of a version's data"""
        version_dir = os.path.join(self.version_dir, version_id)
        
        if not os.path.exists(version_dir):
            logger.error(f"Version directory not found: {version_id}")
            return {"valid": False, "error": "Version not found"}
        
        # Load version info
        with open(os.path.join(version_dir, "version_info.json"), 'r') as f:
            version_info = json.load(f)
        
        results = {
            "version_id": version_id,
            "version_name": version_info["name"],
            "files_checked": 0,
            "files_valid": 0,
            "files_invalid": 0,
            "missing_files": 0,
            "details": []
        }
        
        # Check each data file
        for file_info in version_info["data_files"]:
            filepath = os.path.join(version_dir, file_info["filename"])
            results["files_checked"] += 1
            
            if not os.path.exists(filepath):
                results["missing_files"] += 1
                results["details"].append({
                    "filename": file_info["filename"],
                    "status": "missing",
                    "error": "File not found"
                })
                continue
            
            # Load and check hash
            try:
                df = pd.read_parquet(filepath)
                current_hash = self._calculate_data_hash(df)
                
                if current_hash == file_info["hash"]:
                    results["files_valid"] += 1
                    results["details"].append({
                        "filename": file_info["filename"],
                        "status": "valid",
                        "rows": len(df)
                    })
                else:
                    results["files_invalid"] += 1
                    results["details"].append({
                        "filename": file_info["filename"],
                        "status": "invalid",
                        "error": "Hash mismatch",
                        "expected_hash": file_info["hash"],
                        "actual_hash": current_hash
                    })
            except Exception as e:
                results["files_invalid"] += 1
                results["details"].append({
                    "filename": file_info["filename"],
                    "status": "error",
                    "error": str(e)
                })
        
        results["valid"] = (results["files_invalid"] == 0 and results["missing_files"] == 0)
        
        return results