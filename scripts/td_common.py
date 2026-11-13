"""TreasuryDirect 공용 헬퍼 — fetch-auctions / fetch-auction-results 공통.

2026-10 TreasuryDirect 스키마 변경 대응:
  auctionDate            -> competitiveAuctionCloseDate
  totalAccepted          -> totalParAwardedInsideOfOfferingAmount
  totalTendered          -> totalParRecognizedInsideOfOfferingAmount
  primaryDealerAccepted  -> primaryDealersCompetitiveParAwarded
  directBidderAccepted   -> directBiddersCompetitiveParAwarded
  indirectBidderAccepted -> indirectBiddersCompetitiveParAwarded
구 필드명은 fallback으로 남겨 둔다.

원칙: 조회 실패는 빈 리스트로 삼키지 않고 예외로 올린다.
      (빈 결과로 기존 데이터 파일을 덮어쓰는 사고 방지 — 2026-05-27 요약 8건 소실)
"""
import json
import time
import urllib.request

BASE = 'https://www.treasurydirect.gov/TA_WS/securities'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)', 'Accept': 'application/json'}


def fetch(endpoint, sec_type=None, pagesize=None, retries=3):
    params = ['format=json']
    if sec_type:
        params.append('type=' + sec_type)
    if pagesize:
        params.append(f'pagesize={pagesize}')
    url = f'{BASE}/{endpoint}?' + '&'.join(params)
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode('utf-8'))
            if isinstance(data, list):
                return data
            last = f'unexpected payload type {type(data).__name__}'
        except Exception as e:  # noqa: BLE001 — 재시도 후 예외로 올림
            last = str(e)
        time.sleep(3 * (i + 1))
    raise RuntimeError(f'TreasuryDirect {endpoint}/{sec_type} 조회 실패: {last}')


def first(a, *keys):
    for k in keys:
        v = a.get(k)
        if v not in (None, ''):
            return v
    return None


def num(v):
    try:
        return float(v) if v not in (None, '') else None
    except (TypeError, ValueError):
        return None


def d10(v):
    return (v or '')[:10]


def auction_date(a):
    return d10(first(a, 'competitiveAuctionCloseDate', 'auctionDate'))


def sec_type(a):
    """표시용 종류: TIPS/FRN은 type 기준, 그 외 securityType (Bill/Note/Bond)."""
    t = a.get('type') or ''
    return t if t in ('TIPS', 'FRN') else (a.get('securityType') or t)


def group_term(a):
    """6회 평균 비교 그룹. 이표채는 리오프닝을 원물과 묶고(9Y10M -> 10-Year),
    Bill은 4·8·13주물이 26·52주 리오프닝이라 securityTerm 그대로 사용."""
    if sec_type(a) == 'Bill':
        return a.get('securityTerm') or ''
    return first(a, 'originalSecurityTerm', 'securityTerm') or ''


def bidder_shares(a):
    """경쟁입찰 낙찰 기준 PD / Direct / Indirect 비중(%). 언론 보도 관행과 동일."""
    pd = num(first(a, 'primaryDealersCompetitiveParAwarded', 'primaryDealerAccepted')) or 0
    di = num(first(a, 'directBiddersCompetitiveParAwarded', 'directBidderAccepted')) or 0
    ind = num(first(a, 'indirectBiddersCompetitiveParAwarded', 'indirectBidderAccepted')) or 0
    tot = pd + di + ind
    if not tot:
        return None, None, None, None, None, None
    return (round(pd / tot * 100, 1), round(di / tot * 100, 1), round(ind / tot * 100, 1),
            pd or None, di or None, ind or None)


# ── 등급 / 점수 (fetch_auction_results · merge_auction_ai 공용) ──
def grade_btc(v):
    return None if v is None else 'strong' if v >= 2.6 else 'neutral' if v >= 2.3 else 'weak'


def grade_tail(v):
    return None if v is None else 'strong' if v <= 0.5 else 'neutral' if v <= 1.5 else 'weak'


def grade_pd(v):
    return None if v is None else 'strong' if v <= 12 else 'neutral' if v <= 20 else 'weak'


def apply_grades(an):
    grades = {'btc': grade_btc(an.get('btc')), 'tail': grade_tail(an.get('tail_bps')), 'pd': grade_pd(an.get('pd_pct'))}
    an['grade'] = grades
    valid = [g for g in grades.values() if g]
    an['score_pct'] = (round(sum({'strong': 2, 'neutral': 1, 'weak': 0}[g] for g in valid) / (len(valid) * 2) * 100)
                       if valid else None)


def record_key(r):
    return f"{r.get('cusip', '')}|{r.get('auction_date', '')}"
