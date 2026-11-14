"""데일리 리포트 초안(draft.json) 검증 → 게시본(report.json) 생성.

리포트 에이전트(ops/routines/daily_report.md)는 draft.json만 쓴다. 이 스크립트를 통과해야 사이트·텔레그램에 나간다.
검사:
  1) 형식 — 필수 항목, 개수·길이, HTML 금지, 출처 https
  2) 숫자 대조 — 본문의 모든 숫자가 팩트시트(facts.json) 값(반올림 허용)이거나 cited_numbers(출처 포함)에 있어야 함
  3) 이상 신호 반영 — 팩트시트 anomalies 상위 3개가 drivers.anomaly_keys 또는 skipped_anomalies에 있어야 함
  4) 금지 표현 — 매수·매도 권유, 확정 표현, 은어 (이분법적 해석 차단)
  5) 유사 국면 — 해석 문단에 사례 수(n)가 있어야 함
게시본에는 스크립트가 만든 시장 대시보드(표)를 붙인다 — 표의 숫자는 LLM이 아니라 팩트시트에서 온다.

사용: python3 scripts/verify_daily_report.py                      # 게시 안 된 초안 전부 검증·게시 (Actions)
      python3 scripts/verify_daily_report.py --check <draft.json>  # 에이전트 자체 점검 (파일 안 씀)
실패 시 종료 코드 1 + data/daily/<세션>/verify_errors.json (워크플로가 GitHub 이슈로 알림)
"""
import datetime as dt
import glob
import hashlib
import json
import os
import re
import sys

BASE = 'data/daily'
NOW = dt.datetime.now(dt.timezone.utc)
BANNED = ['매수하세요', '매도하세요', '사세요', '파세요', '사야 한다', '팔아야 한다', '사야한다', '팔아야한다', '무조건', '반드시 오', '반드시 내',
          '확실히 오', '확실히 내', '폭락 확정', '폭등 확정', '떡상', '떡락', '줍줍', '존버', '몰빵', '풀매수', '물타기 하', '지금이 기회',
          '호재니까', '악재니까', '호재라서 매수', '악재라서 매도']
NAME_TOKENS = ['S&P 500', 'S&P500', '나스닥100', '나스닥 100', '러셀2000', '러셀 2000', '다우30', 'MOVE', 'VIX', 'Q1', 'Q2', 'Q3', 'Q4',
               '1분기', '2분기', '3분기', '4분기', '2s10s', '3m10y']
COUNT_UNITS = r'(?:번|건|회|개|가지|차례|명|곳|종|위)'
PERIOD_UNITS = r'(?:거래일|일선|일|주선|주|개월|달|년|시간|분)'
NUM_RE = re.compile(r'(?<![A-Za-z0-9_.])([+\-−]?\d[\d,]*(?:\.\d+)?)')


def load(p, default=None):
    try:
        return json.load(open(p, encoding='utf-8'))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def number_pool(facts):
    pool = set()

    def walk(o):
        if isinstance(o, bool) or o is None:
            return
        if isinstance(o, (int, float)):
            pool.add(float(o))
        elif isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(facts)
    extra = set()
    for v in pool:
        extra.update({abs(v), v * 100, abs(v) * 100})   # 부호를 말로 쓰는 경우, %p→bp (OAS 0.06 → 6bp)
    return pool | extra


def matches(x, pool):
    for p in pool:
        d = abs(x - p)
        if d <= 0.051 or (x == int(x) and abs(p) >= 1 and d < 0.5 + 1e-9) or (abs(p) >= 100 and d / abs(p) <= 0.006):
            return True
    return False


def scan_numbers(text):
    """본문에서 대조 대상 숫자 추출 — 날짜·연도·이름·세는 수·기간은 제외"""
    t = text
    for tok in NAME_TOKENS:
        t = t.replace(tok, ' ')
    t = re.sub(r'\d{4}-\d{2}(?:-\d{2})?', ' ', t)                 # 날짜
    t = re.sub(r'\d{1,2}/\d{1,2}', ' ', t)                          # 11/13
    t = re.sub(r'(?:19|20)\d{2}\s*년', ' ', t)                       # 2010년
    t = re.sub(r'\d{1,2}\s*월\s*\d{1,2}\s*일', ' ', t)
    t = re.sub(r'\d{1,2}\s*월', ' ', t)
    out = []
    for m in NUM_RE.finditer(t):
        raw = m.group(1)
        tail = t[m.end():m.end() + 4]
        val = float(raw.replace(',', '').replace('−', '-'))
        if 1990 <= abs(val) <= 2035 and '.' not in raw:              # 연도
            continue
        if re.match(r'\s*' + COUNT_UNITS, tail) and abs(val) <= 100 and val == int(val):
            continue
        if re.match(r'\s*' + PERIOD_UNITS, tail) and abs(val) <= 260 and val == int(val):
            continue
        if abs(val) <= 10 and val == int(val) and '.' not in raw and not re.match(r'\s*(?:%|bp|σ|\$|B|T)', tail):
            continue
        out.append((raw, val))
    return out


def verify(draft, facts):
    errs = []
    need = ['session', 'headline', 'regime', 'drivers', 'watchpoints']
    for k in need:
        if not draft.get(k):
            errs.append(f'필수 항목 없음: {k}')
    if errs:
        return errs
    if draft['session'] != facts.get('session'):
        errs.append(f"세션 불일치: draft {draft['session']} vs facts {facts.get('session')}")
    if len(draft['headline']) > 70:
        errs.append('headline 70자 초과')
    if (draft.get('regime') or {}).get('label') not in ('risk_on', 'risk_off', 'transition'):
        errs.append('regime.label은 risk_on / risk_off / transition')
    drivers = draft.get('drivers') or []
    if not 2 <= len(drivers) <= 3:
        errs.append(f'drivers는 2~3개 (현재 {len(drivers)})')
    wps = draft.get('watchpoints') or []
    if not 3 <= len(wps) <= 5:
        errs.append(f'watchpoints는 3~5개 (현재 {len(wps)})')
    for w in wps:
        if not re.search(r'\d', w.get('condition', '')):
            errs.append(f"watchpoint 조건에 수치 없음: {w.get('condition')!r}")

    # 블록 단위 텍스트 + cited_numbers
    blocks = [('headline', draft['headline'], []), ('regime', draft['regime'].get('one_liner', ''), [])]
    for i, d in enumerate(drivers):
        blocks.append((f'drivers[{i}]', f"{d.get('title', '')}\n{d.get('body', '')}", d.get('cited_numbers') or []))
        if len(d.get('body', '')) > 600:
            errs.append(f'drivers[{i}].body 600자 초과')
    for k in ('plumbing', 'sectors'):
        if draft.get(k):
            blocks.append((k, draft[k].get('body', ''), draft[k].get('cited_numbers') or []))
    an_facts = {a['asset']: a for a in facts.get('analogs', [])}
    for i, a in enumerate(draft.get('analogs') or []):
        blocks.append((f'analogs[{i}]', a.get('body', ''), a.get('cited_numbers') or []))
        fa = an_facts.get(a.get('asset'))
        if not fa:
            errs.append(f"analogs[{i}] asset {a.get('asset')!r}이 팩트시트 유사 국면에 없음")
        elif str(fa.get('n')) not in a.get('body', ''):
            errs.append(f"analogs[{i}] 본문에 사례 수 n={fa.get('n')}가 없음")
    for i, y in enumerate(draft.get('yesterday_check') or []):
        blocks.append((f'yesterday_check[{i}]', f"{y.get('point', '')}\n{y.get('outcome', '')}", y.get('cited_numbers') or []))
    for i, w in enumerate(wps):
        blocks.append((f'watchpoints[{i}]', f"{w.get('condition', '')}\n{w.get('implication', '')}", w.get('cited_numbers') or []))
    for i, r in enumerate(draft.get('risks') or []):
        blocks.append((f'risks[{i}]', r if isinstance(r, str) else '', []))

    total = sum(len(t) for _, t, _ in blocks)
    if total > 4200:
        errs.append(f'본문 합계 {total}자 — 3,500자 안팎으로 줄일 것')
    pool = number_pool(facts)
    for name, text, cited in blocks:
        if '<' in text:
            errs.append(f'{name}: HTML/꺾쇠 금지')
        for b in BANNED:
            if b in text:
                errs.append(f'{name}: 금지 표현 "{b}" — 매수·매도 권유·확정·은어 대신 조건과 확률로')
        cited_vals = []
        for c in cited:
            if not (isinstance(c, dict) and isinstance(c.get('value'), (int, float)) and str(c.get('source', '')).startswith('https://')):
                errs.append(f'{name}: cited_numbers 항목은 {{value: 숫자, source: https URL}}')
            else:
                cited_vals.append(float(c['value']))
        for raw, val in scan_numbers(text):
            if matches(val, pool) or any(abs(abs(val) - abs(c)) <= 0.051 for c in cited_vals):
                continue
            errs.append(f'{name}: 숫자 {raw}가 팩트시트·cited_numbers에 없음')

    # 출처
    for i, d in enumerate(drivers):
        if not all(isinstance(u, str) and u.startswith('https://') for u in d.get('sources') or []):
            errs.append(f'drivers[{i}].sources는 https URL')
    # 이상 신호 상위 3개 반영
    covered = {k for d in drivers for k in (d.get('anomaly_keys') or [])} | {s.get('key') for s in draft.get('skipped_anomalies') or []}
    for a in (facts.get('anomalies') or [])[:3]:
        if a['key'] not in covered:
            errs.append(f"이상 신호 상위 항목 '{a['text']}'(key {a['key']})를 drivers에서 다루거나 skipped_anomalies에 사유를 적을 것")
    return errs


def dashboard(facts):
    m = facts.get('market', {})
    rows = []
    for k in ['SPY', 'QQQ', 'IWM', 'RSP', 'VIX', 'MOVE', 'US3M', 'US5Y', 'US10Y', 'US30Y', 'TLT', 'HYG', 'DXY', 'USDJPY', 'GOLD', 'WTI', 'COPPER', 'BTC']:
        x = m.get(k)
        if x:
            rows.append({'key': k, 'name': x['name'], 'close': x['close'], 'chg_1d': x['chg_1d'], 'chg_5d': x['chg_5d'],
                         'z_1d': x.get('z_1d'), 'unit': 'bp' if 'bp' in x['unit'] else '%', 'stale': x.get('stale')})
    sr = facts.get('sector_rank') or []
    pl = facts.get('plumbing') or {}
    return {'rows': rows, 'sectors_top': sr[:3], 'sectors_bottom': sr[-3:][::-1],
            'plumbing': {k: pl.get(k) for k in ('net_liquidity_b', 'nl_4w_avg_b', 'tga_b', 'on_rrp_b', 'sofr_minus_iorb_bp', 'stlfsi')},
            'upcoming': (facts.get('events') or {}).get('upcoming_7d', []),
            'econ_results': (facts.get('events') or {}).get('econ_results', []),
            'data_failures': [k for k, v in (facts.get('data_status') or {}).items() if v != 'ok']}


def publish(sess):
    d = os.path.join(BASE, sess)
    draft, facts = load(os.path.join(d, 'draft.json')), load(os.path.join(d, 'facts.json'))
    if not facts:
        return [f'{sess}: facts.json 없음']
    errs = verify(draft or {}, facts)
    if errs:
        json.dump({'session': sess, 'checked_at': NOW.isoformat(timespec='seconds'), 'errors': errs},
                  open(os.path.join(d, 'verify_errors.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        return errs
    report = {**draft, 'dashboard': dashboard(facts), 'anomalies': facts.get('anomalies', [])[:6],
              'analog_stats': facts.get('analogs', []), 'verified_at': NOW.isoformat(timespec='seconds'),
              'draft_hash': hashlib.sha256(open(os.path.join(d, 'draft.json'), 'rb').read()).hexdigest(),
              'telegram_sent': (load(os.path.join(d, 'report.json')) or {}).get('telegram_sent', False)}
    json.dump(report, open(os.path.join(d, 'report.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    if os.path.exists(os.path.join(d, 'verify_errors.json')):
        os.remove(os.path.join(d, 'verify_errors.json'))
    idx = load(os.path.join(BASE, 'index.json'), {'sessions': []})
    reps = idx.get('reports') or []
    if sess not in reps:
        idx['reports'] = sorted(reps + [sess])[-400:]
    idx['latest_report'] = max(idx['reports'])
    json.dump(idx, open(os.path.join(BASE, 'index.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    return []


if '--check' in sys.argv:
    p = sys.argv[sys.argv.index('--check') + 1]
    draft = load(p)
    facts = load(os.path.join(os.path.dirname(p), 'facts.json'))
    if draft is None or facts is None:
        sys.exit('draft.json 또는 같은 폴더의 facts.json을 읽을 수 없음')
    errs = verify(draft, facts)
    for e in errs:
        print('FAIL', e)
    print('OK — 통과' if not errs else f'\n{len(errs)}건 수정 필요')
    sys.exit(1 if errs else 0)

failed = False
for p in sorted(glob.glob(os.path.join(BASE, '*', 'draft.json'))):
    sess = os.path.basename(os.path.dirname(p))
    rep = load(os.path.join(BASE, sess, 'report.json'))
    if rep and rep.get('draft_hash') == hashlib.sha256(open(p, 'rb').read()).hexdigest():
        continue   # 이미 이 초안으로 게시됨 (git checkout은 mtime을 보존하지 않아 내용 해시로 판정)
    errs = publish(sess)
    if errs:
        failed = True
        print(f'❌ {sess} 검증 실패 {len(errs)}건')
        for e in errs:
            print('  -', e)
    else:
        print(f'✅ {sess} 게시')
sys.exit(1 if failed else 0)
