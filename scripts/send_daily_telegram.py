"""게시된 데일리 리포트(report.json) → 텔레그램 요약 + 사이트 전문 링크.

data/daily/config.json의 "telegram": true일 때만 보낸다 (시험 기간엔 false — 사이트에만 게시).
report.json의 telegram_sent로 중복 발송 방지. 운영 경고는 이 채널에 보내지 않는다(GitHub 이슈 사용).
"""
import glob
import html
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = 'data/daily'
SITE = 'https://macromonitor.zbbg.kr/report.html'
cfg = json.load(open(os.path.join(BASE, 'config.json'), encoding='utf-8')) if os.path.exists(os.path.join(BASE, 'config.json')) else {}
if not cfg.get('telegram'):
    print('telegram 꺼짐 (data/daily/config.json) — 발송 생략')
    sys.exit(0)
token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
if not token or not chat:
    print('텔레그램 시크릿 없음 — 발송 생략')
    sys.exit(0)

REGIME = {'risk_on': '🟢 Risk-On', 'risk_off': '🔴 Risk-Off', 'transition': '🟡 전환 국면'}
DOW = '월화수목금토일'
e = html.escape

for p in sorted(glob.glob(os.path.join(BASE, '*', 'report.json')))[-3:]:
    r = json.load(open(p, encoding='utf-8'))
    if r.get('telegram_sent'):
        continue
    sess = r['session']
    import datetime as dt
    d = dt.date.fromisoformat(sess)
    lines = [f"📊 <b>MacroMonitor 데일리</b> · {d.month}/{d.day}({DOW[d.weekday()]}) 미국장",
             '', f"<b>{e(r['headline'])}</b>",
             f"{REGIME.get(r['regime']['label'], '')} — {e(r['regime'].get('one_liner', ''))}", '']
    rows = {x['key']: x for x in r.get('dashboard', {}).get('rows', [])}
    snap = []
    for k, lab in (('SPY', 'S&P'), ('QQQ', '나스닥'), ('US10Y', '10Y'), ('DXY', 'DXY'), ('GOLD', '금'), ('BTC', 'BTC')):
        x = rows.get(k)
        if x and x.get('chg_1d') is not None:
            snap.append(f"{lab} {x['chg_1d']:+}{'bp' if x['unit'] == 'bp' else '%'}")
    if snap:
        lines += [' · '.join(snap), '']
    lines.append('<b>핵심</b>')
    lines += [f"• {e(dv['title'])}" for dv in r.get('drivers', [])]
    if r.get('analogs'):
        lines += ['', '<b>과거 유사 국면</b>'] + [f"• {e(a['body'][:140])}{'…' if len(a['body']) > 140 else ''}" for a in r['analogs'][:2]]
    lines += ['', '<b>관전 포인트</b>'] + [f"• {e(w['condition'])} → {e(w['implication'])}" for w in r.get('watchpoints', [])]
    lines += ['', f"📝 전문: {SITE}?d={sess}"]
    text = '\n'.join(lines)[:4000]
    body = urllib.parse.urlencode({'chat_id': chat, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': 'true'}).encode()
    with urllib.request.urlopen(f'https://api.telegram.org/bot{token}/sendMessage', data=body, timeout=30) as resp:
        ok = json.loads(resp.read()).get('ok')
    if not ok:
        sys.exit(f'{sess} 텔레그램 발송 실패')
    r['telegram_sent'] = True
    json.dump(r, open(p, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print(f'{sess} 텔레그램 발송')
