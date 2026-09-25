"""Validated deployment preferences, separate from protocol and security invariants."""
from dataclasses import dataclass
import os


# Integer preferences share one environment-name and validation table.
_INTEGER_SETTINGS = (
    ('timezone_minutes', 'DISPLAY_TIMEZONE_OFFSET_MINUTES', -720, 840),
    ('notification_seconds', 'NOTIFICATION_INTERVAL_SECONDS', 5, 3600),
    ('menu_seconds', 'MENU_TTL_SECONDS', 60, 86400),
    ('filter_page_size', 'FILTER_PAGE_SIZE', 1, 5),
    ('address_page_size', 'ADDRESS_PAGE_SIZE', 1, 25),
    ('pending_limit', 'ADDRESS_PENDING_LIMIT', 1, 1000),
    ('cache_mib', 'RECENT_CACHE_MIB', 1, 65536),
    ('cache_entries', 'RECENT_CACHE_MAX_ENTRIES', 100, 1000000),
    ('notification_workers', 'NOTIFICATION_WORKERS', 1, 32),
    ('token_workers', 'TOKEN_LOOKUP_WORKERS', 1, 8),
    ('token_pending', 'TOKEN_LOOKUP_MAX_PENDING', 1, 128),
    ('global_api_rps', 'TELEGRAM_GLOBAL_RPS', 1, 20),
    ('housekeeping_seconds', 'HOUSEKEEPING_INTERVAL_SECONDS', 1, 60),
    ('obsolete_days', 'OBSOLETE_RECORD_DAYS', 1, 3650),
    ('confirmed_days', 'CONFIRMED_RECORD_DAYS', 1, 3650),
    ('keep_records', 'MAX_DISPOSABLE_RECORDS', 100, 1000000),
)


@dataclass(frozen=True)
class BotSettings:
    timezone_minutes: int = 0
    notification_seconds: int = 5
    menu_seconds: int = 600
    filter_page_size: int = 5
    address_page_size: int = 10
    pending_limit: int = 20
    cache_mib: int = 256
    cache_entries: int = 50000
    notification_workers: int = 32
    token_workers: int = 4
    token_pending: int = 32
    global_api_rps: int = 20
    housekeeping_seconds: int = 1
    obsolete_days: int = 7
    confirmed_days: int = 30
    keep_records: int = 25000
    title: str = '多鏈地址交易監控'
    status_title: str = '全鏈監控'
    usage_notice: str = ''

    def __post_init__(self):
        for field, _, low, high in _INTEGER_SETTINGS:
            value=getattr(self,field)
            if type(value) is not int or not low<=value<=high:
                raise ValueError(f'{field} must be an integer between {low} and {high}')
        if self.token_pending < self.token_workers:
            raise ValueError('token_pending must not be smaller than token_workers')
        if not isinstance(self.title,str) or not self.title.strip() or len(self.title)>64 or any(ord(c)<32 for c in self.title):
            raise ValueError('BOT_TITLE must contain 1 to 64 printable characters')
        if not isinstance(self.status_title,str) or not self.status_title.strip() or len(self.status_title)>64 or any(ord(c)<32 for c in self.status_title):
            raise ValueError('BOT_STATUS_TITLE must contain 1 to 64 printable characters')
        if not isinstance(self.usage_notice,str) or len(self.usage_notice)>300 or any(ord(c)<32 and c!='\n' for c in self.usage_notice):
            raise ValueError('BOT_USAGE_NOTICE must contain at most 300 printable characters')

    @classmethod
    def from_env(cls, env=None):
        env=os.environ if env is None else env
        values={}
        for field, key, _, _ in _INTEGER_SETTINGS:
            if key in env:
                try:values[field]=int(env[key])
                except (TypeError,ValueError):raise ValueError(f'{key} must be an integer') from None
        values['usage_notice']=env.get('BOT_USAGE_NOTICE','')
        values['title']=env.get('BOT_TITLE',cls.title)
        values['status_title']=env.get('BOT_STATUS_TITLE',cls.status_title)
        return cls(**values)
