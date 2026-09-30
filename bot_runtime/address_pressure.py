"""Remove only the owner's overloaded subscription; retain an auditable alert."""
import html
import json
import time


def measure(store, row, now, limit=20):
    scope = '''FROM events e JOIN event_deliveries d ON d.event_id=e.event_id
        WHERE d.user_id=? AND e.address=? AND (e.chain=? OR (?='evm' AND e.chain<>'tron'))
        AND (?='both' OR e.direction=?) AND e.filtered=0 AND e.orphaned=0 AND e.created_at>=?'''
    chat_deadline=json.loads(store.meta('telegram_chat_cooldowns','{}')).get(str(row['user_id']),0)
    cutoff=max(int(row['created_at']), int(max(float(store.meta('telegram_cooldown_until','0')),chat_deadline))+1)
    args=(row['user_id'],row['address'],row['chain'],row['chain'],row['watch_direction'],row['watch_direction'],cutoff)
    count=store.db.execute('SELECT COUNT(*) '+scope+' AND d.notified=0 AND d.notify_dead=0',args).fetchone()[0]
    return count if count > limit else None



def remove_overloaded(store, now=None, limit=20, usage_notice=''):
    now=int(time.time()) if now is None else now
    # Server-wide delivery outages are not evidence of an abusive subscription.
    if now < float(store.meta('telegram_cooldown_until','0'))+120:
        return []
    removed=[]
    # Start at the partial pending-delivery index, not the complete address book.
    candidates = store.db.execute('''SELECT a.* FROM event_deliveries d
        JOIN events e ON e.event_id=d.event_id
        JOIN addresses a ON a.user_id=d.user_id AND a.address=e.address
          AND (a.chain=e.chain OR (a.chain='evm' AND e.chain<>'tron'))
        JOIN authorized_users u ON u.user_id=a.user_id
        WHERE d.notified=0 AND d.notify_dead=0 AND e.filtered=0 AND e.orphaned=0 AND a.enabled=1
        GROUP BY a.id HAVING COUNT(*)>?''',(limit,)).fetchall()
    cooldowns = json.loads(store.meta('telegram_chat_cooldowns','{}'))
    for row in candidates:
        if now < cooldowns.get(str(row['user_id']),0)+120:
            continue
        if not row['enabled'] or measure(store,row,now,limit) is None:
            continue
        with store.db:
            store.db.execute('BEGIN IMMEDIATE')
            current=store.address(row['id'],user_id=row['user_id'])
            count=measure(store,current,now,limit) if current and current['enabled'] else None
            if count is None:
                continue
            store.db.execute('DELETE FROM addresses WHERE id=? AND user_id=?',(row['id'],row['user_id']))
            # A second subscription owned by this same user may still cover an event.
            store.db.execute('''UPDATE event_deliveries SET notify_dead=1,notify_last_error='address_overload'
                WHERE user_id=? AND notified=0 AND event_id IN (
                  SELECT e.event_id FROM events e WHERE e.address=? AND (e.chain=? OR (?='evm' AND e.chain<>'tron'))
                  AND NOT EXISTS (SELECT 1 FROM addresses a WHERE a.user_id=? AND a.enabled=1
                    AND a.address=e.address AND (a.chain=e.chain OR (a.chain='evm' AND e.chain<>'tron'))
                    AND (a.watch_direction='both' OR a.watch_direction=e.direction)))''',
                (row['user_id'],row['address'],row['chain'],row['chain'],row['user_id']))
            text=(f'⚠️ <b>高頻地址已自動移除</b>\n{html.escape(row["label"] or row["chain"])}\n'
                  f'<code>{html.escape(row["address"])}</code>\n'
                  f'這個地址有 {count} 筆待發通知，超過 {limit} 筆上限。\n'
                  '為恢復通知隊列，已移除你的這個地址並取消其尚未發送的通知；其他地址繼續運作。\n'
                  + html.escape(usage_notice))
            store.db.execute('INSERT OR IGNORE INTO pressure_alerts(address_id,user_id,text,created_at) VALUES(?,?,?,?)',
                (row['id'],row['user_id'],text,now))
            removed.append(row['id'])
    return removed


def send_alert(dispatcher, store, user):
    row=store.db.execute('SELECT * FROM pressure_alerts WHERE user_id=? AND sent=0 AND next_try<=? ORDER BY created_at LIMIT 1',
        (user,int(time.time()))).fetchone()
    if row is None or not store.is_authorized(user):
        return False
    from .telegram_rate import TelegramThrottle
    from .telegram import TelegramError
    try:
        dispatcher.send_to_user(row['text'],user,store,None)
        with store.db:
            store.db.execute('UPDATE pressure_alerts SET sent=1 WHERE address_id=?',(row['address_id'],))
    except TelegramThrottle:
        pass
    except Exception as exc:
        delay=getattr(exc,'retry_after',0) or min(3600,15*2**min(row['attempts'],8))
        permanent=isinstance(exc,TelegramError) and exc.status in {400,403,404} and row['attempts']>=2
        with store.db:
            store.db.execute('UPDATE pressure_alerts SET attempts=attempts+1,next_try=?,sent=? WHERE address_id=?',
                (int(time.time())+delay,2 if permanent else 0,row['address_id']))
    return True
