"""데일리 리포트 1단계 — 전일 시황 팩트시트 + 이상 신호 + 과거 유사 국면 (결정적 계산, LLM 없음).

출력: data/daily/<세션일>/facts.json, data/daily/index.json
  - market:    자산별 종가·1/5/20일 변화·z-score(60일)·이동평균 위치·52주 위치 (Yahoo 20년 일봉)
  - rates:     2·10·30년, 2s10s (FRED via Worker /yields-hist) + Yahoo 금리지수
  - plumbing:  순유동성·TGA·역레포·SOFR·SRF·신용 스프레드·금융스트레스 (Worker + NY Fed)
  - events:    전일 지표 결과(econ_results.json)·국채경매(auction_results.json)·오늘~이번 주 일정
  - anomalies: '평소와 다른 것' 점수순 — 리포트 에이전트가 반드시 다뤄야 할 목록
  - analogs:   시장 이상 신호마다 과거 비슷한 날(같은 방향·비슷한 z·같은 추세 국면)과 이후 경로,
               대응별 결과(보유 / 다음날 매도 / 다음날 추가 / 5일 기다린 뒤 진입) 통계

클라우드 에이전트는 Worker·일부 사이트에 접근할 수 없으므로 Actions가 이 파일을 만들어 레포에 둔다.
조회 실패한 항목은 null + data_status에 사유 — 지어내지 않는다. 핵심(시장 데이터)이 비면 예외로 종료.

사용: python3 scripts/build_daily_facts.py [--out-dir DIR]
"""
import datetime as dt
import json
import math
import os
import statistics as st
import sys
import time
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

WORKER = 'https://macrolens-worker.macrolens.workers.dev'
UA = {'User-Agent': 'Mozilla/5.0 (MacroMonitor-Actions/1.0)', 'Accept': 'application/json'}
OUT_DIR = sys.argv[sys.argv.index('--out-dir') + 1] if '--out-dir' in sys.argv else 'data/daily'
NOW = dt.datetime.now(dt.timezone.utc)
ET = ZoneInfo('America/New_York')
status = {}

# key: (Yahoo 심볼, 이름, 그룹, 변화 단위)  — 'bp'는 금리 지수(값이 % 수준), 나머지는 가격 % 변화
UNIVERSE = {
    'SPY':  ('SPY', 'S&P 500 (SPY)', 'equity', 'pct'),   'QQQ':  ('QQQ', '나스닥100 (QQQ)', 'equity', 'pct'),
    'IWM':  ('IWM', '러셀2000 (IWM)', 'equity', 'pct'),  'RSP':  ('RSP', 'S&P 동일가중 (RSP)', 'equity', 'pct'),
    'VIX':  ('^VIX', 'VIX', 'vol', 'pct'),                'MOVE': ('^MOVE', 'MOVE', 'vol', 'pct'),
    'US3M': ('^IRX', '미 3개월 금리', 'rates', 'bp'),     'US5Y': ('^FVX', '미 5년 금리', 'rates', 'bp'),
    'US10Y': ('^TNX', '미 10년 금리', 'rates', 'bp'),     'US30Y': ('^TYX', '미 30년 금리', 'rates', 'bp'),
    'TLT':  ('TLT', '장기국채 (TLT)', 'bonds', 'pct'),    'HYG':  ('HYG', '하이일드 (HYG)', 'credit', 'pct'),
    'LQD':  ('LQD', '투자등급 (LQD)', 'credit', 'pct'),
    'DXY':  ('DX-Y.NYB', '달러인덱스', 'fx', 'pct'),      'USDJPY': ('JPY=X', '달러/엔', 'fx', 'pct'),
    'GOLD': ('GC=F', '금', 'commodity', 'pct'),           'SILVER': ('SI=F', '은', 'commodity', 'pct'),
    'WTI':  ('CL=F', 'WTI 원유', 'commodity', 'pct'),     'COPPER': ('HG=F', '구리', 'commodity', 'pct'),
    'NATGAS': ('NG=F', '천연가스', 'commodity', 'pct'),   'BTC':  ('BTC-USD', '비트코인', 'crypto', 'pct'),
    'XLK': ('XLK', '기술', 'sector', 'pct'), 'XLF': ('XLF', '금융', 'sector', 'pct'), 'XLE': ('XLE', '에너지', 'sector', 'pct'),
    'XLV': ('XLV', '헬스케어', 'sector', 'pct'), 'XLY': ('XLY', '임의소비재', 'sector', 'pct'), 'XLP': ('XLP', '필수소비재', 'sector', 'pct'),
    'XLI': ('XLI', '산업재', 'sector', 'pct'), 'XLB': ('XLB', '소재', 'sector', 'pct'), 'XLU': ('XLU', '유틸리티', 'sector', 'pct'),
    'XLRE': ('XLRE', '부동산', 'sector', 'pct'), 'XLC': ('XLC', '통신', 'sector', 'pct'),
}
# 과거 유사 국면을 계산할 자산 → 대응 결과를 볼 대상 자산 (금리·변동성 충격은 주식/채권 대응으로 본다)
ANALOG_ASSETS = {'SPY': ['SPY'], 'QQQ': ['QQQ', 'SPY'], 'IWM': ['IWM', 'SPY'], 'VIX': ['SPY'], 'MOVE': ['SPY', 'TLT'],
                 'US10Y': ['TLT', 'SPY'], 'US30Y': ['TLT', 'SPY'], 'HYG': ['HYG', 'SPY'], 'DXY': ['DXY', 'SPY', 'GOLD'],
                 'USDJPY': ['USDJPY', 'SPY'], 'GOLD': ['GOLD'], 'SILVER': ['SILVER', 'GOLD'], 'WTI': ['WTI', 'SPY'], 'NATGAS': ['NATGAS'],
                 'BTC': ['BTC'], 'COPPER': ['COPPER', 'SPY'], 'US5Y': ['TLT', 'SPY'], 'TLT': ['TLT', 'SPY'], 'LQD': ['LQD', 'SPY']}


def get_json(url, timeout=40, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                return json.loads(r.read().decode('utf-8'))
        except Exception as e:   # noqa: BLE001
            err = e
            time.sleep(2 * (i + 1))
    raise err


def r2(x, n=2):
    return None if x is None or (isinstance(x, float) and not math.isfinite(x)) else round(x, n)


# ── 1. 시장 (Yahoo 20년 일봉) ───────────────────────────────
def yahoo_daily(symbol):
    d = get_json(f'https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}?range=20y&interval=1d')
    res = d['chart']['result'][0]
    ts, closes = res.get('timestamp') or [], res['indicators']['quote'][0].get('close') or []
    by = {}
    for t, c in zip(ts, closes):
        if c is not None:
            by[dt.datetime.fromtimestamp(t, ET).date().isoformat()] = c
    return sorted(by.items())


series = {}
for key, (sym, *_r) in UNIVERSE.items():
    try:
        series[key] = yahoo_daily(sym)
        status[f'market.{key}'] = 'ok'
    except Exception as e:   # noqa: BLE001
        status[f'market.{key}'] = f'실패: {type(e).__name__}'
    time.sleep(0.4)
if 'SPY' not in series or len(series['SPY']) < 300:
    sys.exit('SPY 일봉 수신 실패 — 팩트시트 생성 중단')

session = series['SPY'][-1][0]   # 전일(미국) 세션 날짜
now_et = NOW.astimezone(ET)
session_complete = session < now_et.date().isoformat() or now_et.time() >= dt.time(16, 15)


def changes(vals, unit):
    """vals 오름차순 종가 → 일간 변화 리스트 (pct: %, bp: (금리지수 차) ×100)"""
    if unit == 'bp':
        return [(b - a) * 100 for a, b in zip(vals, vals[1:])]
    return [(b / a - 1) * 100 for a, b in zip(vals, vals[1:]) if a]


def zscores(ch, win=60):
    """각 시점의 1일 변화 z (직전 win일 표준편차 기준)"""
    out = [None] * len(ch)
    for i in range(win, len(ch)):
        sd = st.pstdev(ch[i - win:i])
        out[i] = ch[i] / sd if sd > 0 else None
    return out


def sma(vals, n, i):
    return sum(vals[i - n + 1:i + 1]) / n if i >= n - 1 else None


derived = {}
market = {}
for key, s in series.items():
    _, name, group, unit = UNIVERSE[key]
    dates, vals = [d for d, _ in s], [v for _, v in s]
    ch = changes(vals, unit) if unit == 'bp' else [(vals[i] / vals[i - 1] - 1) * 100 for i in range(1, len(vals))]
    z = zscores(ch)
    # 5일 누적 변화와 그 z (직전 250개 5일 변화의 표준편차 기준)
    ch5 = [((vals[i] - vals[i - 5]) * 100 if unit == 'bp' else (vals[i] / vals[i - 5] - 1) * 100) for i in range(5, len(vals))]
    z5 = [None] * len(ch5)
    for i in range(250, len(ch5)):
        sd = st.pstdev(ch5[i - 250:i - 5])   # 마지막 5개는 현재 구간과 겹치므로 제외
        z5[i] = ch5[i] / sd if sd > 0 else None
    derived[key] = {'dates': dates, 'vals': vals, 'ch': ch, 'z': z, 'ch5': ch5, 'z5': z5, 'unit': unit}
    n = len(vals) - 1

    def back(k):
        if n - k < 0:
            return None
        return (vals[n] - vals[n - k]) * 100 if unit == 'bp' else (vals[n] / vals[n - k] - 1) * 100

    yr = vals[-252:]
    market[key] = {
        'name': name, 'group': group, 'unit': '%p→bp' if unit == 'bp' else '%', 'date': dates[n],
        'close': r2(vals[n], 4), 'chg_1d': r2(back(1)), 'chg_5d': r2(back(5)), 'chg_20d': r2(back(20)),
        'z_1d': r2(z[-1]), 'z_5d': r2(z5[-1]) if z5 else None, 'vs_50dma_pct': r2((vals[n] / sma(vals, 50, n) - 1) * 100) if sma(vals, 50, n) else None,
        'vs_200dma_pct': r2((vals[n] / sma(vals, 200, n) - 1) * 100) if sma(vals, 200, n) else None,
        'pctile_1y': r2(sum(v <= vals[n] for v in yr) / len(yr) * 100, 0),
        'high_52w': r2(max(yr), 4), 'low_52w': r2(min(yr), 4), 'stale': dates[n] != session and group != 'crypto',
    }

sectors = sorted((k for k in market if market[k]['group'] == 'sector'), key=lambda k: market[k]['chg_1d'] or 0, reverse=True)

# ── 2. 금리·배관·신용 (Worker / NY Fed) ─────────────────────
def worker(path):
    try:
        d = get_json(WORKER + path, timeout=60)
        if isinstance(d, dict) and d.get('error'):
            raise RuntimeError(d['error'])
        status[f'worker{path}'] = 'ok'
        return d
    except Exception as e:   # noqa: BLE001
        status[f'worker{path}'] = f'실패: {e}'
        return None


liq, yh, tower, pipe3, fsi, cal = (worker(p) for p in ['/liq', '/yields-hist', '/liq-tower', '/pipe3', '/ofr-fsi', '/calendar'])
iorb = worker('/fred?series_id=IORB&limit=5&sort_order=desc')

rates = {}
if yh and yh.get('labels'):
    L = yh['labels']

    def last2(arr):
        pts = [(d, v) for d, v in zip(L, arr) if v is not None]
        return pts[-1], (pts[-2] if len(pts) > 1 else None), (pts[-6] if len(pts) > 5 else None)

    for k in ('dgs2', 'dgs10', 'dgs30'):
        (d0, v0), p1, p5 = last2(yh[k])
        rates[k] = {'date': d0, 'value': v0, 'chg_1d_bp': r2((v0 - p1[1]) * 100, 1) if p1 else None,
                    'chg_5d_bp': r2((v0 - p5[1]) * 100, 1) if p5 else None}
    if rates.get('dgs2') and rates.get('dgs10'):
        rates['s2s10_bp'] = r2((rates['dgs10']['value'] - rates['dgs2']['value']) * 100, 1)

sofr = srf = None
try:
    sd = get_json('https://markets.newyorkfed.org/api/rates/secured/sofr/last/6.json')
    rr = sd.get('refRates') or []
    if rr:
        sofr = {'date': rr[0]['effectiveDate'], 'rate': rr[0]['percentRate'],
                'chg_bp': r2((rr[0]['percentRate'] - rr[1]['percentRate']) * 100, 1) if len(rr) > 1 else None,
                'volume_b': rr[0].get('volumeInBillions')}
    status['nyfed.sofr'] = 'ok'
except Exception as e:   # noqa: BLE001
    status['nyfed.sofr'] = f'실패: {e}'
try:
    rd = get_json('https://markets.newyorkfed.org/api/rp/repo/all/results/last/10.json')
    by = {}
    for op in (rd.get('repo') or {}).get('operations', []):
        by[op['operationDate']] = by.get(op['operationDate'], 0) + (op.get('totalAmtAccepted') or 0)
    if by:
        last = max(by)
        srf = {'date': last, 'accepted_b': r2(by[last] / 1e9, 2)}
    status['nyfed.srf'] = 'ok'
except Exception as e:   # noqa: BLE001
    status['nyfed.srf'] = f'실패: {e}'

iorb_v = None
if iorb:
    obs = [o for o in iorb.get('observations', []) if o.get('value') not in ('.', None)]
    iorb_v = float(obs[0]['value']) if obs else None

nlm = (liq or {}).get('nlMomentum') or {}
rrp = (liq or {}).get('repoRegime') or {}
plumbing = {
    'net_liquidity_b': nlm.get('currentNL'), 'nl_4w_avg_b': nlm.get('avg4wNL'), 'nl_regime': nlm.get('regime'),
    'nl_asof': nlm.get('asOf'), 'tga_b': nlm.get('tga'), 'reserves_credit_b': nlm.get('walcl'),
    'on_rrp_b': rrp.get('rrpCurrent'), 'on_rrp_5d_chg_b': rrp.get('rrpDelta5d'), 'repo_regime': rrp.get('regime'),
    'sofr': sofr, 'iorb': iorb_v,
    'sofr_minus_iorb_bp': r2((sofr['rate'] - iorb_v) * 100, 1) if sofr and iorb_v is not None else None,
    'srf': srf,
    'ig_oas': (pipe3 or {}).get('igOas'), 'ccc_oas': (pipe3 or {}).get('cccHy'),
    'stlfsi': ((fsi or {}).get('total') or {}).get('current'), 'nfci': ((fsi or {}).get('funding') or {}).get('current'),
}
dts = sorted(((((tower or {}).get('fed') or {}).get('tgaDts') or {}).get('series') or []), key=lambda x: x.get('date', ''))
if len(dts) >= 2:
    plumbing['tga_daily'] = {'date': dts[-1]['date'], 'value_b': r2(dts[-1]['close'], 1),
                             'chg_1d_b': r2(dts[-1]['close'] - dts[-2]['close'], 1),
                             'chg_5d_b': r2(dts[-1]['close'] - dts[-6]['close'], 1) if len(dts) >= 6 else None}
else:
    plumbing['tga_daily'] = None

# ── 3. 이벤트 ───────────────────────────────────────────────
def repo_json(path):
    try:
        return json.load(open(path, encoding='utf-8'))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


econ = repo_json('data/econ_results.json').get('results') or {}
econ_recent = [v for k, v in econ.items() if session <= k[:10] <= (dt.date.fromisoformat(session) + dt.timedelta(days=1)).isoformat()
               or k[:10] == session]
auc = [r for r in (repo_json('data/auction_results.json').get('results') or [])
       if r.get('auction_date') == session and r.get('security_type') in ('Note', 'Bond', 'TIPS')]
auctions = [{'term': r.get('security_term'), 'type': r.get('security_type'), 'high_yield': (r.get('_analysis') or {}).get('high_yield'),
             'btc': (r.get('_analysis') or {}).get('btc'), 'btc_avg6': (r.get('_analysis') or {}).get('btc_avg6'),
             'tail_bps': r.get('tail_bps'), 'grade': r.get('grade'), 'summary': r.get('summary')} for r in auc]
today_kst = (NOW + dt.timedelta(hours=9)).date()
upcoming = [{'date': e['date'], 'name': e['name'], 'imp': e.get('imp'), 'estimated': bool(e.get('estimated'))}
            for e in ((cal or {}).get('events') or [])
            if today_kst.isoformat() <= e['date'] <= (today_kst + dt.timedelta(days=7)).isoformat() and e.get('imp') in ('high', 'medium')]

# ── 4. 이상 신호 ────────────────────────────────────────────
anoms = []


def add(kind, key, score, text, **kw):
    anoms.append({'kind': kind, 'key': key, 'score': r2(score, 2), 'text': text, **kw})


for key, m in market.items():
    if m['stale'] or m['z_1d'] is None:
        continue
    unit = 'bp' if UNIVERSE[key][3] == 'bp' else '%'
    if abs(m['z_1d']) >= 2:
        add('move', key, abs(m['z_1d']), f"{m['name']} {m['chg_1d']:+}{unit} (60일 변동성 대비 {m['z_1d']:+.1f}σ)", z=m['z_1d'])
    elif m.get('z_5d') is not None and abs(m['z_5d']) >= 2 and m['group'] != 'sector':
        add('move5', key, abs(m['z_5d']) * 0.9, f"{m['name']} 5일 누적 {m['chg_5d']:+}{unit} (5일 변동 분포 대비 {m['z_5d']:+.1f}σ)", z=m['z_5d'])
    d = derived[key]
    for lv_key, lv in (('vs_50dma_pct', 50), ('vs_200dma_pct', 200)):
        i = len(d['vals']) - 1
        a, b = sma(d['vals'], lv, i - 1), sma(d['vals'], lv, i)
        if a and b and (d['vals'][i - 1] - a) * (d['vals'][i] - b) < 0 and m['group'] in ('equity', 'fx', 'commodity', 'crypto', 'rates'):
            up = d['vals'][i] > b
            add('cross', key, 1.5 if lv == 200 else 1.0, f"{m['name']} {lv}일 이동평균 {'상향' if up else '하향'} 돌파",
                ma=lv, dir='up' if up else 'down')
    yr = d['vals'][-252:]
    if m['group'] in ('equity', 'rates', 'commodity', 'crypto', 'fx') and len(yr) >= 252:
        cur, hi, lo = d['vals'][-1], max(yr), min(yr)
        near = (lambda a, b: abs(a - b) * 100 <= 3) if UNIVERSE[key][3] == 'bp' else (lambda a, b: abs(a / b - 1) <= 0.005)
        if cur >= hi:
            add('extreme', key, 1.2, f"{m['name']} 52주 신고가", side='high')
        elif cur <= lo:
            add('extreme', key, 1.2, f"{m['name']} 52주 신저가", side='low')
        elif near(cur, hi):
            add('extreme', key, 1.0, f"{m['name']} 52주 고점 근접 (고점 {hi:.4g})", side='high')
        elif near(cur, lo):
            add('extreme', key, 1.0, f"{m['name']} 52주 저점 근접 (저점 {lo:.4g})", side='low')

for key, levels in (('VIX', (20, 30, 40)), ('MOVE', (100, 120, 140)), ('US10Y', (4.0, 4.5, 5.0)), ('DXY', (100, 105, 110))):
    d = derived.get(key)
    if not d or len(d['vals']) < 2:
        continue
    a, b = d['vals'][-2], d['vals'][-1]
    for lv in levels:
        if (a - lv) * (b - lv) < 0:
            add('level', key, 1.8, f"{UNIVERSE[key][1]} {lv} {'상향' if b > lv else '하향'} 돌파 ({a:.2f}→{b:.2f})",
                level=lv, dir='up' if b > lv else 'down')


# 섹터 격차: 그날 최고·최저 섹터 차이를 직전 250일 분포와 비교
sec_keys = [k for k in derived if UNIVERSE[k][2] == 'sector']
if len(sec_keys) >= 8:
    common = sorted(set.intersection(*(set(derived[k]['dates'][1:]) for k in sec_keys)))[-251:]
    pos = {k: {dd: i for i, dd in enumerate(derived[k]['dates'])} for k in sec_keys}
    spreads = []
    for dd in common:
        rets = [derived[k]['ch'][pos[k][dd] - 1] for k in sec_keys]
        spreads.append(max(rets) - min(rets))
    if len(spreads) > 60 and common[-1] == session:
        sd, mu = st.pstdev(spreads[:-1]), st.mean(spreads[:-1])
        zz = (spreads[-1] - mu) / sd if sd > 0 else 0
        if zz >= 2:
            top, bot = market[sectors[0]], market[sectors[-1]]
            add('rotation', 'SECTORS', 1.4 + zz / 3,
                f"섹터 격차 {spreads[-1]:.1f}%p (평소 {mu:.1f}%p, {zz:+.1f}σ) — {top['name']} {top['chg_1d']:+}% vs {bot['name']} {bot['chg_1d']:+}%")


def zof(k):
    return (market.get(k) or {}).get('z_1d')


if zof('SPY') is not None and zof('VIX') is not None and zof('SPY') > 1 and zof('VIX') > 1:
    add('divergence', 'SPY/VIX', 2.0, '주가와 VIX 동반 상승 — 상승 중 헤지 수요 증가')
if zof('SPY') is not None and zof('HYG') is not None and zof('SPY') * zof('HYG') < 0 and abs(zof('SPY')) > 1 and abs(zof('HYG')) > 1:
    add('divergence', 'SPY/HYG', 2.0, '주식과 하이일드 채권 반대 방향 — 신용이 주식을 확인하지 않음')
if zof('GOLD') is not None and zof('DXY') is not None and zof('GOLD') > 1 and zof('DXY') > 1:
    add('divergence', 'GOLD/DXY', 1.5, '금과 달러 동반 강세 — 안전자산 동시 수요')
gaps = [(abs((market[b]['chg_1d'] or 0) - (market['RSP']['chg_1d'] or 0)) - (1.0 if b == 'QQQ' else 0.7), b)
        for b in ('SPY', 'QQQ') if zof(b) is not None and zof('RSP') is not None]
if gaps and max(gaps)[0] > 0:   # 둘 중 임계를 더 많이 넘은 쪽 하나만
    big = max(gaps)[1]
    gap = (market[big]['chg_1d'] or 0) - (market['RSP']['chg_1d'] or 0)
    add('divergence', f'{big}/RSP', 1.3 + abs(gap) / 3,
        f"{market[big]['name']} vs 동일가중(RSP) 격차 {gap:+.2f}%p — {'대형 기술주 주도' if gap > 0 else '대형 기술주 이탈, 폭은 유지'}")

if plumbing['sofr_minus_iorb_bp'] is not None and plumbing['sofr_minus_iorb_bp'] > 0:
    add('plumbing', 'SOFR-IORB', 2.5, f"SOFR가 IORB 위 {plumbing['sofr_minus_iorb_bp']}bp — 단기자금 압박")
if srf and srf['date'] >= (dt.date.fromisoformat(session) - dt.timedelta(days=1)).isoformat() and (srf['accepted_b'] or 0) > 1:
    add('plumbing', 'SRF', 2.5, f"상설레포(SRF) {srf['accepted_b']}B 사용 — 담보 유동성 수요")
ccc = plumbing.get('ccc_oas') or {}
if ccc.get('delta') is not None and abs(ccc['delta']) >= 0.25:
    add('credit', 'CCC OAS', 2.0, f"CCC 스프레드 {ccc['delta'] * 100:+.0f}bp")
ig = plumbing.get('ig_oas') or {}
if ig.get('delta') is not None and abs(ig['delta']) >= 0.05:
    add('credit', 'IG OAS', 1.6, f"IG 스프레드 {ig['delta'] * 100:+.0f}bp")
if plumbing.get('on_rrp_5d_chg_b') is not None and abs(plumbing['on_rrp_5d_chg_b']) >= 50:
    add('plumbing', 'ON RRP', 1.4, f"역레포 5일 {plumbing['on_rrp_5d_chg_b']:+.0f}B")
for v in econ_recent:
    if v.get('surprise') in ('above', 'below'):
        p = v['metrics'][v['primary']]
        add('event', v['name'], 1.8, f"{v['name']} {p['label']} 실제 {p['actual']} vs 예상 {p['forecast']} ({'상회' if v['surprise'] == 'above' else '하회'})")
for a in auctions:
    if a.get('tail_bps') is not None and abs(a['tail_bps']) >= 1.5:
        add('event', f"{a['term']} 경매", 1.6 + abs(a['tail_bps']) / 4,
            f"{a['term']} {a['type']} 경매 {'tail' if a['tail_bps'] > 0 else 'stop-through'} {a['tail_bps']:+.1f}bp")

anoms.sort(key=lambda a: -a['score'])
anoms = anoms[:10]

# ── 5. 과거 유사 국면 ───────────────────────────────────────
HORIZONS = (1, 5, 20, 60)


def idx_map(key):
    return {d: i for i, d in enumerate(derived[key]['dates'])}


def fwd(key, date, h, start_offset=0):
    """date 종가(또는 date+start_offset 거래일 종가) 기준 h 거래일 뒤 변화"""
    d = derived[key]
    i = idx_map(key).get(date)
    if i is None:
        return None
    i0, i1 = i + start_offset, i + start_offset + h
    if i1 >= len(d['vals']):
        return None
    return (d['vals'][i1] - d['vals'][i0]) * 100 if d['unit'] == 'bp' else (d['vals'][i1] / d['vals'][i0] - 1) * 100


def mae(key, date, h=20):
    """date 종가 이후 h일 동안 최대 역행 (보유 시 최악 평가손, %)"""
    d = derived[key]
    i = idx_map(key).get(date)
    if i is None or i + h >= len(d['vals']) or d['unit'] == 'bp':
        return None
    return (min(d['vals'][i + 1:i + h + 1]) / d['vals'][i] - 1) * 100


def summ(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 3:
        return None
    xs.sort()
    return {'n': len(xs), 'median': r2(st.median(xs)), 'mean': r2(st.mean(xs)), 'pct_positive': r2(sum(x > 0 for x in xs) / len(xs) * 100, 0),
            'p10': r2(xs[max(0, int(len(xs) * 0.1) - 1)]), 'worst': r2(xs[0]), 'best': r2(xs[-1])}


spy_d = derived['SPY']
spy_trend = {spy_d['dates'][i]: (spy_d['vals'][i] > sma(spy_d['vals'], 200, i)) for i in range(199, len(spy_d['vals']))}


def trigger_dates(key, kind, an):
    """과거에 같은 종류의 신호가 난 날짜들 (최근 20거래일 제외 — 현재 국면과 겹침). → ([(date, z|None)], 설명)"""
    d = derived[key]
    v, dates, last = d['vals'], d['dates'], len(d['vals']) - 21
    out = []
    if kind == 'move':
        z0 = d['z'][-1]
        sign, thr = (1 if z0 > 0 else -1), max(2.0, abs(z0) * 0.8)
        for i in range(61, len(d['ch']) - 20):
            z = d['z'][i]
            if z is not None and z * sign >= thr:
                out.append((dates[i + 1], z))   # ch[i] = vals[i]→vals[i+1] → 그 날짜는 dates[i+1]
        return out, f"같은 방향 1일 변동 {sign * thr:+.1f}σ 이상"
    if kind == 'move5':
        z0 = d['z5'][-1]
        sign, thr = (1 if z0 > 0 else -1), max(2.0, abs(z0) * 0.8)
        for i in range(250, len(d['ch5']) - 20):
            z = d['z5'][i]
            if z is not None and z * sign >= thr:
                out.append((dates[i + 5], z))   # ch5[i] = vals[i]→vals[i+5] → 그 날짜는 dates[i+5]
        return out, f"같은 방향 5일 누적 변동 {sign * thr:+.1f}σ 이상"
    if kind == 'cross':
        n, up = an['ma'], an['dir'] == 'up'
        for i in range(n + 1, last):
            p, c = sma(v, n, i - 1), sma(v, n, i)
            if (v[i - 1] - p) * (v[i] - c) < 0 and (v[i] > c) == up:
                out.append((dates[i], None))
        return out, f"{n}일 이동평균 {'상향' if up else '하향'} 돌파"
    if kind == 'extreme':
        hi = an['side'] == 'high'
        for i in range(252, last):
            w = v[i - 251:i + 1]
            if (v[i] >= max(w)) if hi else (v[i] <= min(w)):
                out.append((dates[i], None))
        return out, f"52주 {'신고가' if hi else '신저가'}"
    if kind == 'level':
        lv, up = an['level'], an['dir'] == 'up'
        for i in range(1, last):
            if (v[i - 1] - lv) * (v[i] - lv) < 0 and (v[i] > lv) == up:
                out.append((dates[i], None))
        return out, f"{lv} {'상향' if up else '하향'} 돌파"
    return [], ''


def analogs_for(an):
    key, kind = an['key'], an['kind']
    d = derived[key]
    cand, desc = trigger_dates(key, kind, an)
    trend_now = spy_trend.get(session)
    filtered = [(dd, z) for dd, z in cand if spy_trend.get(dd) == trend_now] if trend_now is not None else cand
    regime_note = f"S&P 500 {'200일선 위' if trend_now else '200일선 아래'} 국면만" if trend_now is not None else '국면 조건 없음'
    if len(filtered) < 8:
        filtered, regime_note = cand, '국면 조건 없음 (표본 부족)'
    # 군집 제거: 이어진 신호는 첫 날만 (1일 충격 10거래일, 돌파·신고가류 20거래일)
    gap = 10 if kind in ('move', 'move5') else 20
    events, last_i = [], -99
    imap = idx_map(key)
    for dd, z in filtered:
        i = imap[dd]
        if i - last_i > gap:
            events.append((dd, z))
            last_i = i
    base = {'asset': key, 'trigger': an['text'], 'condition': desc, 'regime_filter': regime_note, 'n': len(events)}
    if len(events) < 5:
        return {**base, 'note': '유사 사례 5건 미만 — 통계 생략'}
    out = {**base, 'period': f"{events[0][0]} ~ {events[-1][0]}", 'forward': {}, 'responses': {}}
    for t in ANALOG_ASSETS.get(key, [key]):
        if t not in derived:
            continue
        out['forward'][t] = {f'{h}d': summ([fwd(t, dd, h) for dd, _ in events]) for h in HORIZONS}
        if derived[t]['unit'] == 'bp':
            continue
        nxt = [fwd(t, dd, 19, 1) for dd, _ in events]                 # 다음날 종가 → 20거래일째
        resp = {
            'hold_20d': summ([fwd(t, dd, 20) for dd, _ in events]),                          # 그대로 보유
            'sell_next_day_avoided': summ([-x if x is not None else None for x in nxt]),     # 다음날 매도 시 피한 손익(+면 매도가 유리)
            'add_next_day_20d': summ(nxt),                                                   # 다음날 추가 매수
            'wait5_then_add_20d': summ([fwd(t, dd, 20, 5) for dd, _ in events]),             # 5일 기다린 뒤 진입
            'max_adverse_20d': summ([mae(t, dd) for dd, _ in events]),                       # 보유 시 20일 내 최대 평가손
        }
        cands = {k: v['median'] for k, v in resp.items() if v and k != 'max_adverse_20d'}
        out['responses'][t] = {**resp, 'best_by_median': max(cands, key=cands.get) if cands else None}
    # 에이전트가 당시 맥락을 검색할 대표 사례 — 1일 충격은 |z|가 가장 비슷한 순, 나머지는 최근 순
    if kind in ('move', 'move5'):
        z0 = d['z'][-1] if kind == 'move' else d['z5'][-1]
        closest = sorted(events, key=lambda e: abs(abs(e[1]) - abs(z0)))[:6]
    else:
        closest = events[-6:]
    vix = dict(series.get('VIX', []))
    out['closest'] = [{'date': dd, 'z': r2(z), 'move_1d': r2(d['ch'][imap[dd] - 1]) if imap[dd] > 0 else None,
                       'spy_5d': r2(fwd('SPY', dd, 5)), 'spy_20d': r2(fwd('SPY', dd, 20)), 'vix_level': r2(vix.get(dd))}
                      for dd, z in sorted(closest)]
    return out


analogs = []
for an in anoms:
    if an['kind'] in ('move', 'move5', 'cross', 'extreme', 'level') and an['key'] in ANALOG_ASSETS and an['key'] in derived:
        res = analogs_for(an)
        if res:
            analogs.append(res)
analogs = analogs[:4]

# ── 6. 저장 ─────────────────────────────────────────────────
facts = {
    'session': session, 'session_complete': session_complete, 'generated_at': NOW.isoformat(timespec='seconds'),
    'market': market, 'sector_rank': [{'key': k, 'name': market[k]['name'], 'chg_1d': market[k]['chg_1d']} for k in sectors],
    'rates': rates, 'plumbing': plumbing,
    'events': {'econ_results': econ_recent, 'auctions': auctions, 'upcoming_7d': upcoming},
    'anomalies': anoms, 'analogs': analogs, 'data_status': status,
}
os.makedirs(os.path.join(OUT_DIR, session), exist_ok=True)
path = os.path.join(OUT_DIR, session, 'facts.json')
with open(path, 'w', encoding='utf-8') as f:
    json.dump(facts, f, ensure_ascii=False, indent=1)
idx_path = os.path.join(OUT_DIR, 'index.json')
try:
    idx = json.load(open(idx_path, encoding='utf-8'))
except (FileNotFoundError, json.JSONDecodeError):
    idx = {'sessions': []}
if session not in idx['sessions']:
    idx['sessions'] = sorted(idx['sessions'] + [session])[-400:]
idx['latest'] = session
with open(idx_path, 'w', encoding='utf-8') as f:
    json.dump(idx, f, ensure_ascii=False, indent=1)
fails = [k for k, v in status.items() if v != 'ok']
print(f'{path} — 세션 {session} (완료={session_complete}) · 이상 신호 {len(anoms)} · 유사 국면 {len(analogs)} · 실패 {fails or "없음"}')
for a in anoms:
    print(f"  [{a['score']}] {a['text']}")
