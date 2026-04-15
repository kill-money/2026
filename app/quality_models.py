from dataclasses import dataclass
from typing import Optional


@dataclass
class RawMessage:
    ts: int
    chat_id: int
    msg_id: int
    text: str
    sender: str
    live: int


@dataclass
class NormalizedMessage:
    ts: int
    chat_id: int
    msg_id: int
    text: str
    phone: Optional[str]
    tag: Optional[str]
    confidence: float
    sender: str
    live: int
