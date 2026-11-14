"""뉴스 다이제스트 생성 (GitHub Actions, Gemini) → KV news_digest_v1.

Worker가 KST 06:00 / 21:00에 이 다이제스트를 텔레그램으로 보낸다(/news-digest-send).
- 후보: 마지막 발송 이후(최대 18시간) 뉴스 중 아직 보내지 않은 것 (KV news_digest_state_v1 — Worker가 발송 때 기록)
- Gemini: 같은 사건은 묶고, 시장 참여자 관점의 핵심 5~8건을 골라 '제목 + 시장 의미 한 줄'로 (JSON)
- 검증(결정적): 후보 번호 실재, 카테고리, 길이, HTML 금지, **제목·의미 문장의 숫자는 후보 원문에 있어야 함**,
  링크·출처는 LLM이 아니라 원래 기사 데이터에서 → 지어낸 기사·수치가 나갈 수 없음
- 실패하거나 검증 통과 0건이면 기존 다이제스트를 덮어쓰지 않는다 (운영 원칙 2)
"""
import datetime as dt
import json
import os
import random
import re
import sys
import time
from email.utils import parsedate_to_datetime

import requests

NOW = dt.datetime.now(dt.timezone.utc)
GEMINI_KEY = os.environ['GEMINI_API_KEY']
KV_BASE = (f"https://api.cloudflare.com/client/v4/accounts/{os.environ['CLOUDFLARE_ACCOUNT_ID']}"
           f"/storage/kv/namespaces/{os.environ['KV_NAMESPACE_ID']}")
HDR = {'Authorization': f"Bearer {os.environ['CLOUDFLARE_API_TOKEN']}"}
CATS = {'fed': '연준·중앙은행', 'treasury': '국채·재정', 'macro': '경제지표·거시', 'credit': '신용·금융',
        'geo': '지정학·원자재', 'tech': 'AI·테크', 'crypto': '크립토'}

time.sleep(random.uniform(0, 20))   # 동시 실행 충돌 완화


def kv_get(key):
    for i in range(3):
        r = requests.get(f'{KV_BASE}/values/{key}', headers=HDR, timeout=20)
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            time.sleep(5 * (i + 1))
            continue
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return None
    raise RuntimeError(f'KV GET {key} 429 반복')


def kv_put(key, value, ttl):
    backoff = [10, 30, 60, 120, 240]
    for wait in backoff + [None]:
        r = requests.put(f'{KV_BASE}/values/{key}', headers=HDR, params={'expiration_ttl': ttl},
                         data=json.dumps(value, ensure_ascii=False).encode('utf-8'), timeout=30)
        if r.status_code != 429:
            r.raise_for_status()
            return
        if wait is None:
            break
        time.sleep(max(wait, int(r.headers.get('Retry-After') or 0)))
    raise RuntimeError(f'KV PUT {key} 429 반복')


def ts(s):
    """RSS pubDate(RFC 822)·ISO 둘 다. 시간대 없으면 UTC로 본다."""
    if not s:
        return None
    t = None
    try:
        t = dt.datetime.fromisoformat(s.strip().replace('Z', '+00:00'))
    except ValueError:
        try:
            t = parsedate_to_datetime(s.strip())
        except (TypeError, ValueError, IndexError):
            return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def key_of(it):
    return (it.get('link') or it.get('title') or '').strip().lower()[:200]


news = kv_get('news_cache_v1') or {}
state = kv_get('news_digest_state_v1') or {}
sent = set(state.get('sent_keys') or [])
since = ts(state.get('last_sent_at')) or (NOW - dt.timedelta(hours=18))
since = max(since, NOW - dt.timedelta(hours=18))

cands = []
for it in news.get('items') or []:
    t = ts(it.get('pubDate'))
    if t is None or t < since or key_of(it) in sent:
        continue
    cands.append(it)
cands = cands[:60]
print(f"후보 {len(cands)}건 (마지막 발송 {state.get('last_sent_at') or '없음'} 이후)")
if not cands:
    print('새 후보 없음 — 기존 다이제스트 유지')
    sys.exit(0)

lines = []
for i, it in enumerate(cands, 1):
    t = ts(it['pubDate']).astimezone(dt.timezone(dt.timedelta(hours=9))).strftime('%m/%d %H:%M')
    lines.append(f"[{i}] ({it.get('sourceName') or it.get('sourceId')}, {t} KST, 분류:{it.get('cat')}) "
                 f"{it.get('titleKo') or ''} | {it.get('title') or ''} | {(it.get('summaryKo') or it.get('summary') or '')[:220]}")

prompt = f"""당신은 거시·유동성 중심 투자 매체 MacroMonitor의 뉴스 편집장이다. 아래 후보 기사로 텔레그램 뉴스 브리핑을 만든다.

목표: 시장 참여자(매크로 펀드·트레이더)가 30초 안에 '무슨 일이 있었고 시장에 무슨 의미인지' 파악하게 한다.

선별 규칙
- 같은 사건을 다룬 기사는 하나로 묶는다 (ids에 여러 번호).
- 중요도: 연준·중앙은행 정책 신호 > 국채 수급·재정 > 주요 경제지표 서프라이즈 > 신용 이벤트 > 지정학·원자재(유가·금) > AI·반도체 빅테크(지수 영향 있는 것) > 크립토(제도·기관 자금).
- 제외: 정치 가십, 연예·스포츠, 지수 영향 없는 개별 기업 소식, 단순 시황 중계("다우 상승 마감" 류), 광고성 기사.
- 핵심 5~8건. 의미 있는 후보가 적으면 그보다 적게 (억지로 채우지 말 것).

작성 규칙
- headline: 한국어 40자 이내, 사실 위주 명사형("연준 월러, 12월 인상 지지 시사"). 과장·감탄 금지.
- why: 한국어 90자 이내, 시장 의미 한 줄 — 금리·달러·주식·유동성·크레딧 중 어디에 어떤 경로로 영향을 주는지.
  매수·매도 권유, "호재/악재" 단정, 확정 예측 금지. 불확실하면 "~할 수 있다"가 아니라 조건을 명시("~면 ~").
- 숫자는 후보 기사 원문에 있는 것만 쓴다. 원문에 없는 수치·사실을 추가하지 말 것.
- category: {', '.join(f'{k}({v})' for k, v in CATS.items())} 중 하나.
- importance: 1~5 (5가 가장 중요).

후보 {len(cands)}건 (번호, 출처, 시각, 분류, 한국어 제목 | 원제 | 요약):
{chr(10).join(lines)}

JSON만 출력:
{{"items":[{{"ids":[3,7],"category":"fed","importance":5,"headline":"...","why":"..."}}]}}"""

body = {'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'temperature': 0.2, 'maxOutputTokens': 4096, 'responseMimeType': 'application/json',
                             'thinkingConfig': {'thinkingBudget': 1024}}}
url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}'
for attempt in range(3):
    r = requests.post(url, json=body, timeout=120)
    if r.status_code in (429, 500, 503) and attempt < 2:
        time.sleep(20 * (attempt + 1))
        continue
    r.raise_for_status()
    break
parts = r.json().get('candidates', [{}])[0].get('content', {}).get('parts', [])
raw = ''.join(p.get('text', '') for p in parts if not p.get('thought'))
parsed = json.loads(re.sub(r'^```json\s*|\s*```$', '', raw.strip(), flags=re.M))

NUM = re.compile(r'\d+(?:[.,]\d+)?')


def nums(t):
    return {n.replace(',', '') for n in NUM.findall(t or '')}


items, dropped = [], []
seen_ids = set()
for x in parsed.get('items') or []:
    ids = [i for i in (x.get('ids') or []) if isinstance(i, int) and 1 <= i <= len(cands) and i not in seen_ids]
    head, why, cat = str(x.get('headline') or '').strip(), str(x.get('why') or '').strip(), x.get('category')
    reason = None
    if not ids:
        reason = '후보 번호 없음/중복'
    elif cat not in CATS:
        reason = f'카테고리 {cat!r}'
    elif not head or len(head) > 60 or len(why) > 140:
        reason = '길이'
    elif '<' in head + why or '>' in head + why:
        reason = 'HTML 문자'
    else:
        src_text = ' '.join(f"{cands[i - 1].get('title', '')} {cands[i - 1].get('titleKo', '')} {cands[i - 1].get('summary', '')} "
                            f"{cands[i - 1].get('summaryKo', '')}" for i in ids)
        extra = nums(head + ' ' + why) - nums(src_text)
        if extra:
            reason = f'원문에 없는 숫자 {sorted(extra)}'
    if reason:
        dropped.append((head, reason))
        continue
    seen_ids.update(ids)
    group = [cands[i - 1] for i in ids]
    lead = sorted(group, key=lambda c: c.get('tier', 2))[0]   # 1차 원문(tier 1) 우선
    items.append({'category': cat, 'category_name': CATS[cat], 'importance': int(x.get('importance') or 3),
                  'headline': head, 'why': why, 'link': lead.get('link'), 'source': lead.get('sourceName'),
                  'sources': sorted({c.get('sourceName') for c in group if c.get('sourceName')}),
                  'keys': [key_of(c) for c in group],
                  # 사이트 뉴스 패널 호환 (title·summary·rank)
                  'title': head, 'summary': why})
for h, why in dropped:
    print(f'  제외: {h!r} — {why}')
items.sort(key=lambda z: -z['importance'])
items = items[:8]
for i, it in enumerate(items, 1):
    it['rank'] = i
if not items:
    print('검증 통과 0건 — 기존 다이제스트 유지')
    sys.exit(0)

digest = {'items': items, 'generatedAt': NOW.isoformat(timespec='seconds'), 'window_from': since.isoformat(timespec='seconds'),
          'candidates': len(cands), 'model': 'gemini-2.5-flash'}
kv_put('news_digest_v1', digest, 86400)
print(f'다이제스트 {len(items)}건 저장 (제외 {len(dropped)})')
for it in items:
    print(f"  [{it['importance']}] {it['category_name']} | {it['headline']} — {it['why']}")
