"""클라우드 에이전트 결과(data/auction_ai.json) → 검증 → data/auction_results.json 반영.

auction_results.json을 쓰는 주체는 GitHub Actions뿐이다. 에이전트는 auction_ai.json만 쓴다.
검증을 통과하지 못한 항목은 반영하지 않고 로그만 남긴다 (LLM 출력이 사이트로 바로 새지 않게).

auction_ai.json 형식:
{
  "updated": "2026-11-13T21:40:00Z",
  "entries": {
    "<cusip>|<YYYY-MM-DD>": {
      "summary": "🎯 총평: Strong\n1. Tail: ...",   # 필수, '🎯'로 시작, 1500자 이하
      "tail_bps": -0.8,        # 숫자 또는 null (찾지 못함). |값| <= 10
      "wi_rate": 4.62,         # 숫자 또는 null. 0 < 값 < 20
      "sources": ["https://..."],   # tail/wi를 적었다면 https 출처 1개 이상 필수
      "researched_at": "2026-11-13T21:35:00Z",
      "model": "claude-opus-5-5"
    }
  }
}
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from td_common import apply_grades, record_key  # noqa: E402

AI_PATH = 'data/auction_ai.json'
RES_PATH = 'data/auction_results.json'


def is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate(e, rec):
    errs = []
    s = e.get('summary')
    if not isinstance(s, str) or not s.strip().startswith('🎯') or len(s) > 1500:
        errs.append('summary 형식')
    t, w = e.get('tail_bps'), e.get('wi_rate')
    if t is not None and (not is_num(t) or abs(t) > 10):
        errs.append(f'tail_bps 범위 밖: {t!r}')
    if w is not None and (not is_num(w) or not 0 < w < 20):
        errs.append(f'wi_rate 범위 밖: {w!r}')
    src = e.get('sources') or []
    if not isinstance(src, list) or any(not (isinstance(u, str) and u.startswith('https://')) for u in src):
        errs.append('sources는 https URL 목록이어야 함')
    if (t is not None or w is not None) and not src:
        errs.append('tail/wi 수치에는 출처 필수')
    hy = rec['_analysis'].get('high_yield')
    if is_num(t) and is_num(w) and is_num(hy) and abs((hy - w) * 100 - t) > 0.3:
        errs.append(f'tail({t}) ≠ (high {hy} − WI {w})×100 — 수치 불일치')
    return errs


if not os.path.exists(AI_PATH):
    sys.exit(0)
with open(AI_PATH, encoding='utf-8') as f:
    ai = json.load(f).get('entries', {})
with open(RES_PATH, encoding='utf-8') as f:
    data = json.load(f)

by_key = {record_key(r): r for r in data['results']}
applied, rejected = 0, 0
for k, e in ai.items():
    rec = by_key.get(k)
    if rec is None:
        print(f'건너뜀(대상 없음): {k}')
        continue
    errs = validate(e, rec)
    if errs:
        rejected += 1
        print(f'❌ 거부 {k}: {"; ".join(errs)}')
        continue
    prev_at = (rec.get('_ai') or {}).get('researched_at') or ''
    if rec.get('summary') and prev_at >= (e.get('researched_at') or ''):
        continue                                   # 이미 같은(또는 더 최신) 결과 반영됨
    rec['summary'] = e['summary'].strip()
    rec['_analysis']['tail_bps'] = e.get('tail_bps')
    rec['_analysis']['wi_rate'] = e.get('wi_rate')
    rec['_ai'] = {'sources': e.get('sources') or [], 'researched_at': e.get('researched_at'),
                  'model': e.get('model')}
    apply_grades(rec['_analysis'])
    applied += 1
    print(f"✅ 반영 {k}: tail={e.get('tail_bps')} wi={e.get('wi_rate')}")

if applied:
    with open(RES_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
print(f'반영 {applied}건 / 거부 {rejected}건')
