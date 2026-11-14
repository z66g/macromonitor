"""Worker 이벤트 캘린더 → 결과 조사 대상만 골라 data/econ_calendar.json.

클라우드 에이전트 세션은 Worker 도메인에 접근할 수 없으므로 Actions가 캘린더를 레포에 저장하고,
에이전트(ops/routines/econ.md)는 이 파일로 '발표가 끝났는데 결과가 없는 이벤트'를 고른다.
대상 판정은 data/econ_spec.json의 indicators[].match (이벤트 이름에 포함되면 대상).
조회 실패·빈 결과면 예외로 종료 → 기존 파일 유지 (운영 원칙 2).
"""
import datetime as dt
import json
import sys
import urllib.request

WORKER = 'https://macrolens-worker.macrolens.workers.dev'
OUT = 'data/econ_calendar.json'
spec = json.load(open('data/econ_spec.json', encoding='utf-8'))


def match_indicator(name):
    for ind in spec['indicators']:
        if ind['match'] in (name or ''):
            return ind['id']
    return None


req = urllib.request.Request(f'{WORKER}/calendar', headers={'User-Agent': 'MacroMonitor-Actions/1.0', 'Accept': 'application/json'})
with urllib.request.urlopen(req, timeout=60) as r:
    cal = json.loads(r.read().decode('utf-8'))
if cal.get('error') or not cal.get('events'):
    sys.exit(f"캘린더 조회 실패: {cal.get('error') or '이벤트 없음'} — 기존 파일 유지")

today = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=9)).date()   # KST
lo, hi = today - dt.timedelta(days=10), today + dt.timedelta(days=21)
events = []
for e in cal['events']:
    ind = match_indicator(e.get('name'))
    if not ind:
        continue
    try:
        d = dt.date.fromisoformat(e['date'])
    except (KeyError, ValueError):
        continue
    if lo <= d <= hi:
        events.append({'key': f"{e['date']}|{e['name']}", 'date': e['date'], 'name': e['name'],
                       'indicator': ind, 'imp': e.get('imp'), 'estimated': bool(e.get('estimated'))})
if not events:
    sys.exit('조사 대상 이벤트가 0건 — 캘린더 형식 변경 의심, 기존 파일 유지')

events.sort(key=lambda x: (x['date'], x['name']))
out = {'updated': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
       'calendar_saved_at': cal.get('_savedAt'), 'today_kst': today.isoformat(), 'events': events}
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, indent=1)
print(f'{len(events)}건 저장 ({lo} ~ {hi})')
