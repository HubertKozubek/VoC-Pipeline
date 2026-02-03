from .types import StorageType
from .parquet_store import ParquetStore
from .postgres_store import PostgresStore
from voc.storage.storage import VocStorage

def get_storage(storage_type: StorageType, config: dict = None, **kwargs) -> VocStorage:

    final_config = config.copy() if config else {}
    final_config.update(kwargs)
    
    if storage_type == StorageType.PARQUET:
        return ParquetStore(**final_config)
    elif storage_type == StorageType.POSTGRES:
        return PostgresStore(**final_config)
    raise ValueError(f"Unknown storage type: {storage_type}")