"""data/qra.json → 검증 → Cloudflare KV(qra_active_v1) 반영.

QRA 값을 바꾸는 유일한 경로. 클라우드 에이전트(재무부 원문 검증) 또는 소유자가 GitHub에서
data/qra.json을 커밋하면 qra-apply.yml이 이 스크립트를 실행한다. 사이트에서는 수정할 수 없다.

data/qra.json 형식 (재무부 QRA는 한 번에 '이번 분기 + 다음 분기' 두 추정치를 발표):
{
  "announcement_date": "2026-11-02",
  "source_url": "https://home.treasury.gov/news/press-releases/...",   # treasury.gov https 필수
  "pdf_url": null,
  "quarters": [
    {"quarter": "2026 Q4", "net_borrowing_billions": 590, "end_cash_balance_billions": 850,
     "evidence_quote": "...privately-held net marketable borrowing of $590 billion ... end-of-December cash balance of $850 billion"},
    {"quarter": "2027 Q1", "net_borrowing_billions": 560, "end_cash_balance_billions": 850, "evidence_quote": "..."}
  ],
  "verified_at": "2026-11-13T22:10:00Z",
  "applied_by": "agent",          # agent | manual
  "model": "claude-opus-5-5"
}
"""
import datetime
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

PATH = 'data/qra.json'
NS = '66c313c710a04a4eb04775b80a44f313'


def fail(msg):
    sys.exit(f'❌ QRA 검증 실패 — KV 미반영: {msg}')


with open(PATH, encoding='utf-8') as f:
    q = json.load(f)

# ── 검증 ──────────────────────────────────────────────
try:
    ann = datetime.date.fromisoformat(q.get('announcement_date', ''))
except ValueError:
    fail('announcement_date는 YYYY-MM-DD')
if ann > datetime.date.today() + datetime.timedelta(days=1):
    fail('발표일이 미래')
src = q.get('source_url') or ''
host = urllib.parse.urlparse(src).hostname or ''
if not src.startswith('https://') or not (host == 'treasury.gov' or host.endswith('.treasury.gov')):
    fail('source_url은 treasury.gov https 원문이어야 함')
if q.get('pdf_url') and not str(q['pdf_url']).startswith('https://'):
    fail('pdf_url은 https')
quarters = q.get('quarters')
if not isinstance(quarters, list) or not 1 <= len(quarters) <= 2:
    fail('quarters는 1~2개')
ann_q = (ann.year, (ann.month - 1) // 3 + 1)
for e in quarters:
    m = re.fullmatch(r'(\d{4}) Q([1-4])', str(e.get('quarter', '')))
    if not m:
        fail(f"quarter 형식: {e.get('quarter')!r}")
    yq = (int(m.group(1)), int(m.group(2)))
    nxt = (ann_q[0] + (ann_q[1] == 4), ann_q[1] % 4 + 1)
    if yq not in (ann_q, nxt):
        fail(f'{e["quarter"]}는 발표일({ann}) 기준 이번/다음 분기가 아님')
    nb, cb = e.get('net_borrowing_billions'), e.get('end_cash_balance_billions')
    if not isinstance(nb, (int, float)) or not -500 <= nb <= 2500:
        fail(f'net_borrowing_billions 범위: {nb!r}')
    if not isinstance(cb, (int, float)) or not 100 <= cb <= 2000:
        fail(f'end_cash_balance_billions 범위: {cb!r}')
    ev = e.get('evidence_quote') or ''
    if not 20 <= len(ev) <= 1000:
        fail(f'{e["quarter"]} evidence_quote(원문 인용) 필요')

if '--check' in sys.argv:                  # 에이전트 커밋 전 자체 검증용
    print(f"✅ 검증 통과: {', '.join(e['quarter'] for e in quarters)} (발표 {ann})")
    sys.exit(0)

# ── KV 반영 (Cloudflare REST) ────────────────────────────
token, account = os.environ['CLOUDFLARE_API_TOKEN'], os.environ['CLOUDFLARE_ACCOUNT_ID']
base = f'https://api.cloudflare.com/client/v4/accounts/{account}/storage/kv/namespaces/{NS}/values/'


def kv(method, key, body=None, ttl=None):
    url = base + key + (f'?expiration_ttl={ttl}' if ttl else '')
    for attempt, wait in enumerate([10, 30, 60, 120, 0]):
        req = urllib.request.Request(url, method=method, data=body,
                                     headers={'Authorization': f'Bearer {token}'})
        try:
            with urllib.request.urlopen(req, timeout=30):
                return
        except urllib.error.HTTPError as e:
            if e.code == 404 and method == 'DELETE':
                return
            if e.code != 429 or not wait:
                raise
            print(f'  KV 429 → {wait}s 대기')
            time.sleep(wait)


now = datetime.datetime.now(datetime.timezone.utc).isoformat()
active = {**q, 'applied_at': now, 'applied_by': q.get('applied_by') or 'manual'}
kv('PUT', 'qra_active_v1', json.dumps(active, ensure_ascii=False).encode('utf-8'), ttl=86400 * 400)
kv('DELETE', 'qra_pending_v1')
kv('DELETE', 'liq_tower_v1')       # 유동성 예측모델 다음 요청 때 새 QRA로 재계산
print(f"✅ QRA 반영: {', '.join(e['quarter'] for e in quarters)} (발표 {ann})")
