"""Telegram UI runs on the coordinator; chain ingestion runs separately."""
import html
import json
import logging
import threading
import time
import unicodedata
from dataclasses import replace
from types import SimpleNamespace

from scanner_adapters import build_adapter
from block_validation import header
from .common import normalize_evm, tron_to_hex
from .decoder import decode
from .feed_store import FeedStore
from .recent_cache import RecentCache
from .finality import Finalizer
from .filters import ScamFilter
from .notifications import NotificationDispatcher, NotificationPump
from .store import Store
from .telegram import Telegram
from .telegram_controller import TelegramControllerMixin
from .security import TokenSecurity
from .enrichment import TokenEnrichment
from .settings import BotSettings

LOG = logging.getLogger(__name__)

STATUS_NAMES = {'ethereum':'Ethereum', 'tron':'TRON', 'polygon':'Polygon', 'bnb':'BNB',
                'avalanche':'Avalanche', 'optimism':'OP', 'arbitrum':'Arbitrum',
                'base':'Base', 'hyperliquid':'HyperEVM'}


def status_cell(value, width, right=False):
    text, used = '', 0
    for char in str(value).replace('\n', ' ').replace('\r', ' '):
        size = 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
        if used + size > width:
            break
        text += char
        used += size
    padding = ' ' * (width-used)
    return padding+text if right else text+padding


class App(TelegramControllerMixin):
    def __init__(self, token, owner, config, state_path, leases):
        self.settings = BotSettings.from_env()
        self.owner, self.config, self.state_path, self.leases = owner, config, state_path, leases
        self.store = Store(state_path, owner)
        self.store.configure_owner(owner)
        self.telegram = Telegram(token, state_path, settings=self.settings)
        self.feed = FeedStore(state_path)
        self.started_at = int(time.time())
        self.active_user_id = owner
        self.pending_inputs, self.status_refreshes, self.latest_menu_message_ids = {}, {}, {}
        def normalize_tron(address):
            tron_to_hex(address)
            return address.strip()
        self.adapters = {name:SimpleNamespace(config=cfg,normalize=normalize_evm if cfg['type']=='evm' else normalize_tron)
                         for name,cfg in config['chains'].items()}
        self.evm_group = [name for name,cfg in config['chains'].items() if cfg['type']=='evm']
        self.notification_dispatcher = NotificationDispatcher(
            self.telegram,lambda text,user_id,store,event_id=None:self.telegram.send(text=text,chat_id=user_id,
                guard=lambda:store.delivery_allowed(event_id,user_id))['message_id'],self._notification_text)
        self.stop = threading.Event()
        self.inflight = {}
        self.recent_cache = RecentCache(max_bytes=self.settings.cache_mib*1024*1024,max_entries=self.settings.cache_entries)
        self.token_security = TokenSecurity()
        self.enrichment = TokenEnrichment(state_path, owner, self.token_security, self.inflight,settings=self.settings)

    def status_text(self, user_id=None):
        user = self.active_user_id if user_id is None else user_id
        rows = self.store.addresses(user_id=user)
        notifications = self.store.notification_stats(user)
        lines = [f'<b>{html.escape(self.settings.status_title)}</b>',f'你的地址 {sum(bool(r["enabled"]) for r in rows)}/{len(rows)} · 待通知 {notifications["pending"] or 0}']
        status = self.leases.status()
        nodes = {n['node_id']:n for n in status['nodes']}
        order = {name: index for index, name in enumerate(self._ordered_chain_names())}
        table = ['   '+status_cell('鏈',9)+' '+status_cell('節點',7)+' '+status_cell('落後',4,True)+' '+status_cell('RPC',6,True)]
        for lease in sorted(status['leases'], key=lambda row: order.get(row['chain_name'], len(order))):
            if lease.get('disabled') or lease['chain_name'] not in self.adapters:
                continue
            node = nodes.get(lease['owner_node'],{})
            metrics = node.get('metrics',{}).get(lease['chain_name'],{})
            fresh = time.time()-node.get('last_seen',0)<45
            rate = metrics.get('chain_blocks_per_second') or 0
            lag_blocks = metrics.get('lag')
            delayed = (not isinstance(lag_blocks,(int,float)) or lag_blocks > max(3,rate*30)
                       or (metrics.get('state_age_seconds') or 0)>30)
            icon = '✅' if fresh and metrics.get('status')=='running' and not delayed else '⚠️'
            lag_value = metrics.get('lag','?')
            lag = '999+' if isinstance(lag_value,(int,float)) and lag_value>9999 else str(lag_value)
            qualified,candidates = metrics.get('qualified'),metrics.get('candidates')
            rpc_text = f'{qualified}/{candidates}' if type(qualified) is int and type(candidates) is int and 0 <= qualified <= candidates and candidates > 0 else '檢測中'
            name = lease['chain_name']
            label = self.config['chains'].get(name,{}).get('status_name',STATUS_NAMES.get(name,self._chain_label(name)))
            table.append(f'{icon} '+status_cell(label,9)+' '+status_cell(lease['owner_node'],7)+' '+status_cell(lag,4,True)+' '+status_cell(rpc_text,6,True))
        lines.append('<pre>'+html.escape('\n'.join(table))+'</pre>')
        return '\n'.join(lines)

    def info_text(self):
        thresholds = {tuple(str(cfg.get(key,default)) for key,default in [('market_min_liquidity_usd','10000'),('market_min_transfer_usd','1')]) for cfg in self.config['chains'].values()}
        shared = next(iter(thresholds)) if len(thresholds)==1 else None
        lines = ['<b>技術設定</b>','全鏈下載 → 地址比對 → 協調節點核對 → 用戶通知',
                 '租約 20s · 心跳 5s · 卡死 120s · 只向主節點故障轉移',
                 '啟動從當時最新區塊開始，不補停機舊交易',
                 '交易未確認 → 確認／重組時原地更新；EVM 主幣直接轉帳與 ERC-20、TRON TRX／TRC-20',
                 '不含合約內部主幣轉帳、NFT、尚未上鏈的 mempool 交易',
                 f'命中資料記憶體快取上限 {self.recent_cache.max_bytes//(1024*1024)} MiB；確認按區塊合併核對',
                 'GoPlus 免費風險檢查＋市場過濾，結果快取 24h；不代表真幣保證',
                 (f'流動性門檻 US${html.escape(shared[0])} · 轉帳門檻 US${html.escape(shared[1])}' if shared else '各鏈過濾門檻見下方'),
                 f'通知間隔 {self.settings.notification_seconds}s · API 全域上限 {self.settings.global_api_rps}/s',
                 f'非通知消息保留 {self.settings.menu_seconds}s；備份僅含自己的地址及設定']
        for name in self._ordered_chain_names():
            cfg = self.config['chains'][name]
            if not shared:
                lines.append(f'{html.escape(self._chain_label(name))} · 流動性 US${html.escape(str(cfg.get("market_min_liquidity_usd","10000")))} · 轉帳 US${html.escape(str(cfg.get("market_min_transfer_usd","1")))}')
            lines.append(f'{html.escape(self._chain_label(name))} · 安全距離 {cfg["finality_blocks"]} 區塊 · 候選 RPC {len(cfg["rpc_urls"])}')
        return '\n'.join(lines)

    def consume(self, name):
        store = Store(self.state_path,self.owner)
        adapter = build_adapter(name,self.config['chains'][name])
        finalizer = Finalizer(adapter,store)
        try:
            while not self.stop.is_set():
                try:
                    self.inflight[name]=time.monotonic()
                    finalizer.settle()
                except Exception as exc:
                    LOG.warning('confirmation check deferred chain=%s error=%s',name,type(exc).__name__)
                finally:
                    self.inflight.pop(name,None)
                hit = self.feed.due(name)
                if hit is None:
                    self.stop.wait(1)
                    continue
                try:
                    self.inflight[name] = time.monotonic()
                    self.feed.claim(hit)  # Count each attempt exactly once, before external work.
                    rows = store.addresses(name,include_evm=name in self.evm_group,user_id=-1)
                    rows = [r for r in rows if r['created_at'] <= hit['seen']]
                    if not rows:
                        self.feed.finish(hit,2)  # Explicitly canceled, not counted as delivered.
                        continue
                    safe_height=finalizer.safe_height() if rows else -1
                    events = decode(adapter,store,hit,{r['address'] for r in rows},self.recent_cache, metadata_lookup=self.enrichment.metadata) if rows else []
                    confirmed=hit['height']<=safe_height
                    if events and confirmed and header(adapter,hit['height'])[0]!=hit['hash']:
                        raise ValueError('canonical block changed before confirmation')
                    incomplete = False
                    for event in events:
                        if not event.metadata_complete:
                            incomplete = True
                            continue  # Retry this hit; never send an unscaled token amount.
                        assessment = None
                        if event.asset_id != 'native':
                            enriched = self.enrichment.request(adapter, store, event.asset_id)
                            if enriched is None:
                                incomplete = True
                                continue
                            event = replace(event, source_risk=enriched['risk'])
                            assessment = self.enrichment.assessment(store, name, event.asset_id)
                        filtered,reason = ScamFilter(adapter.config).evaluate(event,assessment)
                        if not filtered and event.asset_id != 'native':
                            reason = '|'.join(filter(None,[reason,*self.enrichment.warnings(store,adapter.config,name,event.asset_id)]))
                        # Recheck authorization and address ownership after slow RPC/filter calls.
                        current = store.addresses(name,include_evm=name in self.evm_group,user_id=-1)
                        original_ids = {r['id'] for r in rows}
                        recipients = {r['user_id'] for r in current if r['id'] in original_ids and
                                      r['address']==event.address and r['watch_direction'] in ('both',event.direction)}
                        if recipients:
                            store.save_event(event,filtered,reason,'cluster-v2',confirmed=confirmed,user_ids=recipients,address_ids=original_ids)
                    self.feed.finish(hit,not incomplete,delay=5 if incomplete else None)
                except Exception as exc:
                    LOG.warning('transaction verification deferred chain=%s error=%s',name,type(exc).__name__)
                    self.feed.finish(hit,False)
                finally:
                    self.inflight.pop(name,None)
        finally:
            store.db.close()

    def notify_loop(self):
        store = Store(self.state_path,self.owner)
        pump = NotificationPump(self.notification_dispatcher, self.state_path, self.owner, self.inflight)
        try:
            while not self.stop.is_set():
                delay = 1
                try:
                    self.inflight['notification-scheduler'] = time.monotonic()
                    delay = pump.tick(store, paused=bool(self.telegram.rate.remaining()))
                except Exception as exc:
                    LOG.warning('notification flush deferred error=%s',type(exc).__name__)
                finally:
                    self.inflight.pop('notification-scheduler', None)
                self.stop.wait(delay)
        finally:
            pump.close()
            store.db.close()

    def run(self):
        workers = [threading.Thread(target=self.consume,args=(name,),daemon=True,name='events-'+name) for name in self.adapters]
        workers.append(threading.Thread(target=self.notify_loop,daemon=True,name="notifications"))
        for worker in workers:
            worker.start()
        registered, next_cleanup, next_stats = False,0,0
        try:
            while not self.stop.is_set():
                if any(time.monotonic()-started > 120 for started in list(self.inflight.values())):
                    raise RuntimeError('transaction verification stalled')
                if any(not w.is_alive() for w in workers):
                    raise RuntimeError('event consumer stopped')
                try:
                    if not registered and not self.telegram.rate.remaining():
                        if self.store.meta('v2_telegram_initialized','') != '1':
                            self.telegram.call('deleteWebhook', {'drop_pending_updates':True})
                            self.store.set_meta('v2_telegram_initialized','1')
                        self.register_commands()
                        registered=True
                    self.poll_telegram()
                    self.store.set_meta('v2_telegram_poll_at',int(time.time()))
                    if not self.telegram.rate.remaining():
                        with self.telegram.rate.background():
                            self._flush_message_deletions()
                            self._refresh_status_message()
                    else:
                        self.stop.wait(2)
                    if time.monotonic() >= next_stats:
                        self.store.set_meta('v2_cache_stats',json.dumps(self.recent_cache.stats()))
                        next_stats=time.monotonic()+30
                    if time.monotonic() >= next_cleanup:
                        self.store.cleanup(keep_disposable=self.settings.keep_records,
                                           obsolete_days=self.settings.obsolete_days,
                                           confirmed_days=self.settings.confirmed_days)
                        self.feed.cleanup()
                        next_cleanup=time.monotonic()+86400
                except Exception as exc:
                    LOG.warning('Telegram operation deferred error=%s',type(exc).__name__)
                    self.stop.wait(min(60,max(2,getattr(exc,'retry_after',0))))
        finally:
            self.stop.set()
            self.enrichment.close()
            self.store.db.close()
