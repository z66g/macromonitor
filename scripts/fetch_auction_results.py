"""국채경매 결과 수집 (결정적 단계 — LLM 없음).

- TreasuryDirect에서 결과를 받아 BTC / PD / Direct / Indirect / 직전 6회 평균을 계산
- 기존 data/auction_results.json과 (cusip, 경매일) 키로 병합하며
  클라우드 에이전트가 채운 summary / tail_bps / wi_rate / _ai / telegram_sent 는 절대 지우지 않음
- 조회가 실패하거나 결과가 비정상적으로 적으면 파일을 쓰지 않고 실패(exit 1)로 끝냄
"""
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from td_common import (apply_grades, auction_date, bidder_shares, d10, fetch, first,  # noqa: E402
                       group_term, num, record_key, sec_type)

PATH = 'data/auction_results.json'
PAGESIZE = {'Note': 100, 'Bond': 60, 'TIPS': 40, 'FRN': 30, 'Bill': 200}
KEEP_DAYS = {'Bill': 45}            # Bill은 화면 표시용으로 최근 45일만
KEEP_DAYS_DEFAULT = 400             # 이표채는 6회 평균 계산 + 표시용으로 400일
PRESERVE_TOP = ('summary', 'telegram_sent', '_ai')
PRESERVE_ANALYSIS = ('tail_bps', 'wi_rate')

today = datetime.date.today()


key = record_key


# ── 1. 기존 파일 ─────────────────────────────────────────
existing = {}
orphans = []                               # 2026-10 스키마 변경으로 날짜가 비어버린 레코드
if os.path.exists(PATH):
    with open(PATH) as f:
        for r in json.load(f).get('results', []):
            if r.get('auction_date'):
                existing[key(r)] = r
            elif r.get('summary'):
                orphans.append(r)
print(f'기존 정상 레코드: {len(existing)}건 / 날짜 손상 + 요약 보유: {len(orphans)}건')

# ── 2. 조회 (실패 시 예외 → 파일 미변경) ───────────────────
raw = []
for t, n in PAGESIZE.items():
    items = fetch('auctioned', t, pagesize=n)
    print(f'auctioned/{t}: {len(items)}건')
    raw += items

dated = [a for a in raw if auction_date(a)]
if len(raw) < 50 or len(dated) < len(raw) * 0.9:
    sys.exit(f'중단: 조회 결과가 비정상 (전체 {len(raw)}, 날짜 있음 {len(dated)}) — 기존 파일 유지')

# ── 3. 지표 계산 ─────────────────────────────────────────
rows = []
for a in dated:
    pd_pct, di_pct, in_pct, pd_a, di_a, in_a = bidder_shares(a)
    rows.append({
        'security_type': sec_type(a),
        'security_term': a.get('securityTerm', ''),
        'group_term':    group_term(a),
        'auction_date':  auction_date(a),
        'issue_date':    d10(a.get('issueDate')),
        'maturity_date': d10(a.get('maturityDate')),
        'cusip':         a.get('cusip', ''),
        'offering_amt':  num(a.get('offeringAmount')),
        'total_accepted': num(first(a, 'totalParAwardedInsideOfOfferingAmount', 'totalAccepted')),
        'total_tendered': num(first(a, 'totalParRecognizedInsideOfOfferingAmount', 'totalTendered')),
        'interest_rate': num(a.get('interestRate')),
        'press_release':  (f"https://www.treasurydirect.gov/instit/annceresult/press/preanre/{auction_date(a)[:4]}/"
                           f"{a['treasuryAuctionResultsPressReleaseFileName']}"
                           if a.get('treasuryAuctionResultsPressReleaseFileName') else None),
        '_analysis': {
            'btc': num(a.get('bidToCoverRatio')),
            'high_yield': num(first(a, 'highYield', 'highRate', 'highInvestmentRate')),
            'pd_pct': pd_pct, 'di_pct': di_pct, 'in_pct': in_pct,
            'pd_accepted': pd_a, 'di_accepted': di_a, 'in_accepted': in_a,
        },
    })

# 직전 6회 평균: 같은 (종류, 비교그룹)에서 이 경매보다 앞선 것만
by_group = {}
for r in rows:
    by_group.setdefault((r['security_type'], r['group_term']), []).append(r)
for grp in by_group.values():
    grp.sort(key=lambda r: r['auction_date'])


def avg_prior(r, field):
    grp = by_group[(r['security_type'], r['group_term'])]
    prior = [x['_analysis'][field] for x in grp
             if x['auction_date'] < r['auction_date'] and x['_analysis'][field] is not None][-6:]
    return round(sum(prior) / len(prior), 2) if prior else None


for r in rows:
    an = r['_analysis']
    an['btc_avg6'] = avg_prior(r, 'btc')
    an['pd_avg6'] = avg_prior(r, 'pd_pct')
    an['in_avg6'] = avg_prior(r, 'in_pct')

# 날짜 손상 레코드 구제: cusip + 만기표기가 같은 조회 결과가 정확히 1건이면 그 경매로 귀속
for o in orphans:
    hits = [r for r in rows if r['cusip'] == o.get('cusip') and r['security_term'] == o.get('security_term')]
    if len(hits) == 1:
        existing[key(hits[0])] = {**o, 'auction_date': hits[0]['auction_date']}
        print(f"  구제: {o['cusip']} {o['security_term']} → {hits[0]['auction_date']}")

# ── 4. 기존 값 보존 병합 + 보관 기간 필터 ──────────────────
out = {}
for r in rows:
    keep_days = KEEP_DAYS.get(r['security_type'], KEEP_DAYS_DEFAULT)
    if (today - datetime.date.fromisoformat(r['auction_date'])).days > keep_days:
        continue
    prev = existing.get(key(r), {})
    for k in PRESERVE_TOP:
        if prev.get(k) is not None:
            r[k] = prev[k]
    r.setdefault('summary', None)
    for k in PRESERVE_ANALYSIS:
        r['_analysis'][k] = prev.get('_analysis', {}).get(k)
    out[key(r)] = r

# 요약이 있는 과거 레코드는 조회 창 밖이어도 보존 (3년)
for k, prev in existing.items():
    if k not in out and prev.get('summary'):
        if (today - datetime.date.fromisoformat(prev['auction_date'])).days <= 365 * 3:
            out[k] = prev

# 등급 / 점수 (보존된 tail 반영)
for r in out.values():
    apply_grades(r['_analysis'])

results = sorted(out.values(), key=lambda r: (r['auction_date'], r['cusip']), reverse=True)

# 안전장치: 이표채 레코드가 기존의 절반 미만으로 줄면 쓰지 않음
coupon = lambda rs: sum(1 for r in rs if r['security_type'] != 'Bill')  # noqa: E731
if coupon(existing.values()) >= 10 and coupon(results) < coupon(existing.values()) * 0.5:
    sys.exit(f'중단: 이표채 레코드 급감 ({coupon(existing.values())} → {coupon(results)}) — 기존 파일 유지')

with open(PATH, 'w') as f:
    json.dump({'updated': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'count': len(results), 'results': results}, f, indent=2, ensure_ascii=False)

pending = [r for r in results if r['security_type'] in ('Note', 'Bond', 'TIPS') and not r.get('summary')
           and (today - datetime.date.fromisoformat(r['auction_date'])).days <= 14]
print(f'저장: {len(results)}건 (최근 14일 요약 대기 이표채 {len(pending)}건)')
for r in pending:
    print(f"  대기: {r['auction_date']} {r['security_type']} {r['security_term']} {r['cusip']}")
