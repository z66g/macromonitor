"""이벤트 결과 에이전트 출력(data/econ_results_ai.json) → 검증 → data/econ_results.json (사이트가 읽는 파일).

econ_results.json을 쓰는 주체는 GitHub Actions뿐이다. 에이전트는 econ_results_ai.json만 쓴다.
- 형식·범위·출처 검사, 지표 정의는 data/econ_spec.json
- FRED에 있는 지표는 실제치를 FRED 공식 값(Worker /fred 읽기 전용 프록시)과 대조:
    일치 → 게시(verified='fred') · 불일치 → 거부(rejects) · FRED 미반영 → 보류(pending, 다음 실행 때 재검증)
- FRED에 없는 지표(ISM·미시간·FOMC)는 출처 URL 기반 게시(verified='source')
- 예상 대비 판정(상회/부합/하회)은 스크립트가 계산 (LLM 판단 아님)

econ_results_ai.json 형식:
{"updated": "...", "entries": {"<YYYY-MM-DD>|<캘린더 이벤트 이름>": {
   "period": "2026-10",                       # monthly YYYY-MM · quarterly YYYY-Qn · weekly/meeting YYYY-MM-DD
   "metrics": {"<metric id>": {"actual": 3.0, "forecast": 3.1, "previous": 3.0, "previous_revised": null}},
   "interpretation": "한 문장 해석 (200자 이하)",
   "sources": ["https://..."], "researched_at": "<UTC ISO>", "model": "<모델 ID>"}}}

사용: python3 scripts/merge_econ_results.py            # 검증·병합 (FRED 대조 포함)
      python3 scripts/merge_econ_results.py --check    # 에이전트 자체 점검: 형식만 (FRED 대조 생략, 파일 안 씀)
"""
import datetime as dt
import json
import math
import re
import sys
import urllib.request

WORKER = 'https://macrolens-worker.macrolens.workers.dev'
SPEC = json.load(open('data/econ_spec.json', encoding='utf-8'))
AI_PATH, OUT_PATH = 'data/econ_results_ai.json', 'data/econ_results.json'
CHECK = '--check' in sys.argv
NOW = dt.datetime.now(dt.timezone.utc)
TODAY_KST = (NOW + dt.timedelta(hours=9)).date()
KEEP_DAYS = 400


def load(path, default):
    try:
        return json.load(open(path, encoding='utf-8'))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def indicator_for(name):
    return next((i for i in SPEC['indicators'] if i['match'] in name), None)


def is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and abs(v) < 1e7


def period_date(ind, period, release):
    """period 문자열 → FRED 관측일(YYYY-MM-DD). 형식·발표일 대비 범위가 틀리면 ValueError."""
    kind = ind['period']
    if kind == 'monthly':
        if not re.fullmatch(r'\d{4}-\d{2}', period or ''):
            raise ValueError(f'period는 YYYY-MM이어야 함: {period!r}')
        d = dt.date.fromisoformat(period + '-01')
        lag = (release.year - d.year) * 12 + release.month - d.month
        if not 0 <= lag <= 4:
            raise ValueError(f'기준월 {period}이 발표일 {release}과 맞지 않음')
    elif kind == 'quarterly':
        m = re.fullmatch(r'(\d{4})-Q([1-4])', period or '')
        if not m:
            raise ValueError(f'period는 YYYY-Qn이어야 함: {period!r}')
        d = dt.date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
        if not 0 <= (release - d).days <= 200:
            raise ValueError(f'기준분기 {period}이 발표일 {release}과 맞지 않음')
    else:   # weekly · meeting
        try:
            d = dt.date.fromisoformat(period or '')
        except ValueError:
            raise ValueError(f'period는 YYYY-MM-DD이어야 함: {period!r}')
        if not 0 <= (release - d).days <= (0 if kind == 'meeting' else 14):
            raise ValueError(f'기준일 {period}이 발표일 {release}과 맞지 않음')
    return d.isoformat()


_fred_cache = {}


def fred_value(series, units, obs_date):
    """Worker /fred 프록시로 해당 관측일 값. 없으면 None (아직 FRED 미반영)."""
    k = (series, units)
    if k not in _fred_cache:
        url = f'{WORKER}/fred?series_id={series}&units={units}&limit=40&sort_order=desc'
        req = urllib.request.Request(url, headers={'User-Agent': 'MacroMonitor-Actions/1.0'})
        with urllib.request.urlopen(req, timeout=40) as r:
            d = json.loads(r.read().decode('utf-8'))
        if d.get('error'):
            raise RuntimeError(f'FRED 프록시 오류 {series}: {d["error"]}')
        _fred_cache[k] = {o['date']: float(o['value']) for o in d.get('observations', []) if o.get('value') not in ('.', None)}
    return _fred_cache[k].get(obs_date)


def validate(key, e):
    """(정리된 항목, 상태, 사유) — 상태: ok / pending / reject"""
    m = re.fullmatch(r'(\d{4}-\d{2}-\d{2})\|(.+)', key)
    if not m:
        return None, 'reject', '키 형식은 YYYY-MM-DD|이벤트 이름'
    release = dt.date.fromisoformat(m.group(1))
    ind = indicator_for(m.group(2))
    if not ind:
        return None, 'reject', '조사 대상 지표가 아님 (econ_spec.json match 없음)'
    if release > TODAY_KST:
        return None, 'reject', '아직 발표 전인 이벤트'
    if (TODAY_KST - release).days > 60:
        return None, 'reject', '60일 넘은 이벤트'
    try:
        obs_date = period_date(ind, e.get('period'), release)
    except ValueError as ex:
        return None, 'reject', str(ex)

    spec_m = {x['id']: x for x in ind['metrics']}
    metrics = e.get('metrics') or {}
    if not isinstance(metrics, dict) or not metrics:
        return None, 'reject', 'metrics 없음'
    unknown = set(metrics) - set(spec_m)
    if unknown:
        return None, 'reject', f'정의에 없는 지표 {sorted(unknown)}'
    primary = next(x['id'] for x in ind['metrics'] if x.get('primary'))
    if not is_num((metrics.get(primary) or {}).get('actual')):
        return None, 'reject', f'대표 지표 {primary}의 실제치(actual) 없음'
    clean = {}
    for mid, v in metrics.items():
        if not isinstance(v, dict):
            return None, 'reject', f'{mid} 형식 오류'
        row = {}
        for f in ('actual', 'forecast', 'previous', 'previous_revised'):
            x = v.get(f)
            if x is not None and not is_num(x):
                return None, 'reject', f'{mid}.{f}이 숫자가 아님: {x!r}'
            row[f] = x
        row['label'], row['unit'] = spec_m[mid]['label'], spec_m[mid]['unit']   # 사이트 표시용
        clean[mid] = row

    srcs = e.get('sources') or []
    if not (isinstance(srcs, list) and 1 <= len(srcs) <= 6 and all(isinstance(u, str) and u.startswith('https://') for u in srcs)):
        return None, 'reject', 'sources는 https URL 1~6개'
    interp = e.get('interpretation') or ''
    if not isinstance(interp, str) or len(interp) > 200 or '<' in interp:
        return None, 'reject', 'interpretation은 200자 이하 문장 (HTML 금지)'

    # FRED 대조 — 실제치가 있는 지표만
    checked, pending = [], []
    if not CHECK:
        for mid, row in clean.items():
            fr = spec_m[mid].get('fred')
            if not fr or row['actual'] is None:
                continue
            val = fred_value(fr['series'], fr['units'], obs_date)
            if val is None:
                pending.append(f"{mid}: FRED {fr['series']} {obs_date} 미반영")
                continue
            val *= fr.get('scale', 1)
            tol = fr.get('tol') if 'tol' in fr else abs(val) * fr['tol_pct'] / 100
            if abs(row['actual'] - val) > tol + 1e-9:
                return None, 'reject', f"{mid} 실제치 {row['actual']} ≠ FRED {fr['series']} {obs_date} {val:.4g} (허용 ±{tol:.3g})"
            checked.append(mid)
    if pending:
        return None, 'pending', '; '.join(pending)

    p = clean[primary]
    surprise = None
    if p['forecast'] is not None:
        diff = p['actual'] - p['forecast']
        surprise = 'inline' if abs(diff) < 1e-9 else ('above' if diff > 0 else 'below')
    has_fred = any(x.get('fred') for x in ind['metrics'])
    item = {
        'indicator': ind['id'], 'date': release.isoformat(), 'name': m.group(2), 'period': e['period'],
        'primary': primary, 'metrics': clean, 'surprise': surprise,
        'interpretation': interp.strip(), 'sources': srcs,
        'verified': 'fred' if checked else ('fred_skipped' if CHECK and has_fred else 'source'),
        'researched_at': e.get('researched_at'), 'model': e.get('model'),
    }
    return item, 'ok', ''


ai = load(AI_PATH, {'entries': {}})
out = load(OUT_PATH, {'results': {}})
results = out.get('results') or {}
pending, rejects = {}, {}
n_new = 0
for key, e in (ai.get('entries') or {}).items():
    if key[:10] < (TODAY_KST - dt.timedelta(days=60)).isoformat():
        continue   # 오래된 에이전트 항목은 무시 (이미 게시된 결과는 results에 그대로 남음)
    try:
        item, status, why = validate(key, e if isinstance(e, dict) else {})
    except Exception as ex:   # 네트워크 등 — 이번 실행만 보류, 다음 실행 때 재시도
        status, why, item = 'pending', f'검증 중 오류: {type(ex).__name__}: {ex}', None
    if status == 'ok':
        prev = results.get(key)
        same = prev and {k: v for k, v in prev.items() if k != 'published_at'} == item
        if not same:
            item['published_at'] = NOW.isoformat(timespec='seconds')
            results[key] = item
            n_new += 1
        elif prev:
            item['published_at'] = prev.get('published_at')
    elif status == 'pending':
        pending[key] = why
    else:
        rejects[key] = why
    print(f'{status:7} {key} {why}')

if CHECK:
    print(f'\n점검: 거부 {len(rejects)}건 · 보류 {len(pending)}건')
    sys.exit(1 if rejects else 0)

cutoff = (TODAY_KST - dt.timedelta(days=KEEP_DAYS)).isoformat()
results = {k: v for k, v in results.items() if v.get('date', '') >= cutoff}
new = {'results': dict(sorted(results.items())), 'pending': pending, 'rejects': rejects}
old = load(OUT_PATH, {})
if {k: old.get(k) for k in new} == new:
    print('\n변경 없음 — 파일 그대로 (커밋 생략)')
    sys.exit(0)
out = {'updated': NOW.isoformat(timespec='seconds'), **new}
with open(OUT_PATH, 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, indent=1)
print(f'\n게시 {len(results)}건 (이번에 갱신 {n_new}) · 보류 {len(pending)} · 거부 {len(rejects)}')
