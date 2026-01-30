from .types import StorageType
from .parquet_store import ParquetStore

def get_storage(storage_type: StorageType, app_id: str, **kwargs):
    if storage_type == StorageType.PARQUET:
        return ParquetStore(app_id=app_id, **kwargs)
    raise ValueError(f"Unknown storage type: {storage_type}")