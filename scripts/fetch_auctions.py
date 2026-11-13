"""예정 경매(upcoming) + 최근 경매(auctioned) 목록 — 유동성 예측모델·경매 캘린더용.

조회 실패 시 예외로 끝나 기존 data/auctions.json을 유지한다.
"""
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from td_common import auction_date, d10, fetch, first, num, sec_type  # noqa: E402

PATH = 'data/auctions.json'


def clean(a, kind):
    row = {
        'type': sec_type(a),
        'term': a.get('securityTerm', ''),
        'auctionDate': auction_date(a),
        'issueDate': d10(a.get('issueDate')),
        'offeringAmt': num(a.get('offeringAmount')) or 0.0,
        'cusip': a.get('cusip', ''),
    }
    if kind == 'auctioned':
        row['highRate'] = first(a, 'highYield', 'highRate', 'highInvestmentRate')
    return row


upcoming = [clean(a, 'upcoming') for a in fetch('upcoming')]          # 전체 종류 한 번에
auctioned = []
for t in ('Bill', 'Note', 'Bond', 'TIPS'):
    auctioned += [clean(a, 'auctioned') for a in fetch('auctioned', t, pagesize=20)]

if not auctioned or any(not r['auctionDate'] for r in upcoming + auctioned):
    sys.exit('중단: 날짜 없는 레코드 — 스키마가 또 바뀌었는지 확인 필요. 기존 파일 유지')

result = {
    'updated': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'upcoming': sorted(upcoming, key=lambda x: x['issueDate']),
    'auctioned': sorted(auctioned, key=lambda x: x['auctionDate'], reverse=True)[:40],
}
with open(PATH, 'w') as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
print(f"완료: upcoming {len(result['upcoming'])}건 / auctioned {len(result['auctioned'])}건")
