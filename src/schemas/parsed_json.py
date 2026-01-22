from typing import Optional
from pydantic import BaseModel

class ParsedJson(BaseModel):
    event_id: str
    timestamp: str
    event_type: str
    user_id: str
    document_id: str
    comment_text: Optional[str] = None
    shared_with: Optional[str] = None
    edit_length: Optional[int] = None