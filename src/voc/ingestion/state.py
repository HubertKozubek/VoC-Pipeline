import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

# controls the state of the ingestion process
class StateManager:
    def get_last_sync(self, app_id: str) -> Optional[datetime]:
        pass
    
    def update_last_sync(self, app_id: str, last_sync: datetime):
        pass
