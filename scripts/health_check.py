"""MacroMonitor 일일 헬스체크 (결정적 단계 — LLM 없음).

'워크플로는 성공인데 실제로는 아무것도 안 하는' 고장을 잡는 데 초점:
  빈 캐시, 오래된 데이터, 스키마 변경으로 비어버린 필드, 요약이 안 붙는 경매, 멈춘 다이제스트 등.
클라우드 헬스체크 에이전트가 이 스크립트를 먼저 실행하고, 이상 항목만 원인을 조사한다.

사용: python3 scripts/health_check.py [--out data/health.json]
"""
import datetime as dt
import json
import re
import sys
import urllib.request

WORKER = 'https://macrolens-worker.macrolens.workers.dev'
REPO = 'z66g/macromonitor'
NOW = dt.datetime.now(dt.timezone.utc)
TODAY = NOW.date()
checks = []


def add(name, status, detail):
    checks.append({'name': name, 'status': status, 'detail': detail})


def get(path, timeout=60):
    url = path if path.startswith('http') else WORKER + path
    req = urllib.request.Request(url, headers={'User-Agent': 'MacroMonitor-HealthCheck/1.0',
                                               'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def age_hours(iso):
    if not iso:
        return None
    t = dt.datetime.fromisoformat(str(iso).replace('Z', '+00:00'))
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return (NOW - t).total_seconds() / 3600


def age_days(date_str):
    if not date_str:
        return None
    return (TODAY - dt.date.fromisoformat(str(date_str)[:10])).days


def check(name, fn):
    try:
        fn()
    except Exception as e:  # noqa: BLE001 — 점검 자체 실패도 결과로 남긴다
        add(name, 'fail', f'점검 실패: {type(e).__name__}: {e}')


def series_lengths(o, path=''):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == 'series' and isinstance(v, list):
                yield path or '(root)', len(v)
            else:
                yield from series_lengths(v, f'{path}.{k}' if path else k)


# ── Worker 캐시 / 데이터 신선도 ─────────────────────────────
def c_liq():
    d = get('/liq')
    nl = d.get('nlMomentum') or {}
    if nl.get('currentNL') is None:
        add('T1 순유동성', 'fail', f"currentNL 없음 (sources={nl.get('sources')})")
    else:
        a = age_days(nl.get('asOf'))
        add('T1 순유동성', 'ok' if a is not None and a <= 6 else 'warn',
            f"NL ${nl['currentNL']}B score {nl.get('score')}% asOf {nl.get('asOf')} · {nl.get('sources')}")
    y = (d.get('yields') or {}).get('dgs10') or {}
    a = age_days(y.get('asOf'))
    add('국채 수익률', 'ok' if a is not None and a <= 5 else 'warn', f"DGS10 {y.get('current')} asOf {y.get('asOf')}")


def c_t2():
    d = get('/t2')
    h = age_hours(d.get('_savedAt'))
    lab = (d.get('labor') or {}).get('unrate') or {}
    rel = age_days(lab.get('releasedAt'))
    st = 'ok' if h is not None and h <= 13 and rel is not None and rel <= 45 else 'warn'
    add('T2 실물경제', st, f"캐시 {h and round(h, 1)}h 전 · 실업률 {lab.get('asOf')} (발표 {lab.get('releasedAt')})")


def c_t3():
    d = get('/t3')
    empty = [p for p, n in series_lengths(d) if n == 0]
    h = age_hours(d.get('_savedAt'))
    st = 'fail' if empty else ('ok' if h is not None and h <= 13 else 'warn')
    add('T3 글로벌', st, f"빈 시리즈 {empty or '없음'} · 캐시 {h and round(h, 1)}h 전")


def c_calendar():
    d = get('/calendar')
    ev = d.get('events') or []
    st = 'fail' if d.get('error') or not ev else 'ok'
    add('이벤트 캘린더', st, f"{len(ev)}건 · error={d.get('error')}")


def c_fomc_horizon():
    src = open('worker.js', encoding='utf-8').read()
    m = re.search(r'function fetchFomcDates\(\) \{(.*?)\n\}', src, re.S)
    dates = sorted(re.findall(r"'(\d{4}-\d{2}-\d{2})'", m.group(1))) if m else []
    left = (dt.date.fromisoformat(dates[-1]) - TODAY).days if dates else None
    st = 'ok' if left is not None and left > 60 else 'warn'
    add('FOMC 일정 하드코딩', st, f"마지막 등록일 {dates[-1] if dates else '?'} ({left}일 남음) — 60일 이내면 다음 해 일정 추가 필요")


def c_tower():
    d = get('/liq-tower')
    q = d.get('qraActive') or {}
    weeks = (d.get('vampire') or {}).get('weeks') or []
    dts = ((d.get('fed') or {}).get('tgaDts') or {}).get('latest') or {}
    a = age_days(dts.get('date'))
    add('유동성 예측모델', 'ok' if len(weeks) == 4 else 'fail', f"4주 예측 {len(weeks)}주")
    add('DTS TGA', 'ok' if a is not None and a <= 5 else 'warn', f"최신 {dts.get('date')} ${dts.get('close')}B")
    ann_age = age_days(q.get('announcement_date'))
    stale = ann_age is None or ann_age > 95          # QRA는 2·5·8·11월 분기 발표 → 95일 넘으면 새 발표 누락
    add('QRA', 'ok' if q.get('quarter_match') and not stale else 'warn',
        f"{q.get('target_quarter')} · 현재 분기 일치={q.get('quarter_match')} · 발표 {q.get('announcement_date')} ({ann_age}일 전)")


def c_news():
    d = get('/news')
    items = d.get('items') or []
    tr = sum(1 for i in items if i.get('titleKo'))
    h = age_hours(d.get('_translatedAt'))
    ratio = tr / len(items) if items else 0
    st = 'ok' if items and h is not None and h <= 2 and ratio >= 0.8 else 'warn' if items else 'fail'
    add('뉴스 수집·번역', st, f"{len(items)}건 · 번역 {tr}건 ({ratio:.0%}) · 수집 {h and round(h, 1)}h 전")


def c_digest():
    d = get('/news-digest')
    h = age_hours(d.get('generatedAt'))
    st = 'ok' if h is not None and h <= 8 else 'warn'
    add('뉴스 다이제스트', st, f"생성 {h and round(h, 1)}h 전 · {len(d.get('items') or [])}건")


def c_h41():
    d = get('/h41-history?weeks=4')
    labels = (d.get('series') or {}).get('labels') or []
    latest = dt.datetime.strptime(labels[0], '%B %d, %Y').date() if labels else None
    a = (TODAY - latest).days if latest else None
    st = 'ok' if len(labels) >= 3 and a is not None and a <= 9 else 'warn'
    add('H.4.1 이력', st, f"{len(labels)}주 · 최신 {labels[0] if labels else '?'}")


def c_ici():
    d = get('/ici')
    a = age_days(d.get('weekEnded'))
    add('ICI MMF', 'ok' if a is not None and a <= 12 else 'warn', f"주차 {d.get('weekEnded')} · 이력 {d.get('kvCount')}주")


# ── 레포 데이터 파일 ─────────────────────────────────────
def c_auctions():
    d = json.load(open('data/auction_results.json', encoding='utf-8'))
    rs = d.get('results') or []
    empty = sum(1 for r in rs if not r.get('auction_date'))
    if empty:
        add('경매 데이터 스키마', 'fail', f'경매일 빈 레코드 {empty}건 — TreasuryDirect 스키마 변경 의심')
    else:
        add('경매 데이터 스키마', 'ok', f"{len(rs)}건 · 갱신 {d.get('updated', '')[:16]}")
    late = [f"{r['auction_date']} {r['security_term']} {r['security_type']}" for r in rs
            if r.get('security_type') in ('Note', 'Bond', 'TIPS') and not r.get('summary')
            and 2 <= age_days(r['auction_date']) <= 14]
    add('경매 요약', 'warn' if late else 'ok', f"요약 지연(2~14일): {late or '없음'}")


def c_actions():
    since = (NOW - dt.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
    d = get(f'https://api.github.com/repos/{REPO}/actions/runs?status=failure&per_page=50&created=%3E{since}')
    fails = [f"{r['name']} ({r['created_at'][11:16]}Z)" for r in d.get('workflow_runs', [])]
    add('GitHub Actions 실패(24h)', 'warn' if fails else 'ok', fails or '없음')


for name, fn in [('T1 순유동성', c_liq), ('T2 실물경제', c_t2), ('T3 글로벌', c_t3), ('이벤트 캘린더', c_calendar),
                 ('FOMC 일정 하드코딩', c_fomc_horizon), ('유동성 예측모델', c_tower), ('뉴스 수집·번역', c_news),
                 ('뉴스 다이제스트', c_digest), ('H.4.1 이력', c_h41), ('ICI MMF', c_ici),
                 ('경매 데이터', c_auctions), ('GitHub Actions', c_actions)]:
    check(name, fn)

order = {'ok': 0, 'warn': 1, 'fail': 2}
overall = max((c['status'] for c in checks), key=lambda s: order[s], default='ok')
report = {'checked_at': NOW.isoformat(timespec='seconds'), 'status': overall, 'checks': checks}

for c in checks:
    print(f"{ {'ok': '✅', 'warn': '⚠️', 'fail': '❌'}[c['status']] } {c['name']}: {c['detail']}")
print(f'\n종합: {overall}')
if '--out' in sys.argv:
    with open(sys.argv[sys.argv.index('--out') + 1], 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
