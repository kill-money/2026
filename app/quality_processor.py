import re
from typing import Optional

from .quality_models import RawMessage, NormalizedMessage

_PHONE_RE = re.compile(r"(?<!\d)(?:86)?1\d{10}(?!\d)")


class QualityProcessor:
    def clean_text(self, text: str) -> str:
        return text.replace("\n", " ").strip()

    def extract_phone(self, text: str) -> Optional[str]:
        m = _PHONE_RE.search(text)
        if not m:
            return None
        v = m.group(0)
        return v[2:] if v.startswith("86") else v

    def extract_tag(self, text: str) -> Optional[str]:
        t = text.lower()
        if any(k in t for k in ("贷款", "网贷", "借")):
            return "loan"
        if any(k in t for k in ("低保", "扶贫", "救助")):
            return "poverty"
        if any(k in t for k in ("资金盘", "拉人头", "mlm")):
            return "mlm"
        return None

    def process(self, raw: RawMessage) -> Optional[NormalizedMessage]:
        text = self.clean_text(raw.text)
        if not text:
            return None
        phone = self.extract_phone(text)
        tag = self.extract_tag(text)
        if not phone or not tag:
            return None
        confidence = 0.85 if any(w in text[:40] for w in ("我", "本人", "我的")) else 0.7
        return NormalizedMessage(
            ts=raw.ts,
            chat_id=raw.chat_id,
            msg_id=raw.msg_id,
            text=text[:1000],
            phone=phone,
            tag=tag,
            confidence=confidence,
            sender=raw.sender,
            live=raw.live,
        )
