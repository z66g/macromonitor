# 국채경매 분석 에이전트 (클라우드 예약작업)

MacroMonitor(macromonitor.zbbg.kr)의 미 국채 경매 분석을 작성한다. **너는 `data/auction_ai.json` 하나만 수정·커밋한다.**
검증·사이트 반영·텔레그램 발송은 GitHub Actions(`auction-ai-merge.yml`)가 한다. 다른 파일은 절대 건드리지 마라.

## 1. 준비
```bash
git pull --rebase origin main
```
`data/auction_ai.json`이 없으면 `{"updated": null, "entries": {}}`로 시작한다.

## 2. 대상 고르기
`data/auction_results.json`의 `results`에서 아래를 모두 만족하는 레코드:
- `security_type` ∈ {Note, Bond, TIPS}
- `auction_date`가 오늘(UTC)로부터 10일 이내
- `summary`가 비어 있음
- `auction_ai.json`에 같은 키(`{cusip}|{auction_date}`) 항목이 없거나, 있는데 `tail_bps`가 null이고
  `researched_at`이 20시간 이상 지났고 경매가 3일 이내 (WI를 못 찾은 건 3일까지 재시도)

대상이 없으면 아무것도 커밋하지 말고 "대상 없음"으로 끝낸다.

## 3. 사실 확인 (레코드마다)
레코드의 공식 수치를 그대로 쓴다 — 지어내지 마라:
`_analysis.high_yield`, `btc`, `btc_avg6`, `pd_pct`, `pd_avg6`, `di_pct`, `in_pct`, `in_avg6`, `offering_amt`.
`press_release`(재무부 결과 PDF)가 있으면 열어서 high yield가 일치하는지 확인한다.

**WI(When-Issued) 금리와 Tail 찾기** — 공식 발표에는 없으므로 보도에서 찾는다.
- 검색 예: `"{security_term} note auction" {경매일} tail`, `{term} Treasury auction {경매일} when-issued`, `{term} auction stops through`
- 신뢰 출처: Reuters, Bloomberg, WSJ, MarketWatch, CNBC, Barron's, ZeroHedge, InvestingLive(ForexLive), Investing.com, 주요 딜러 코멘트
- 정의: `tail_bps = (high_yield − WI) × 100` (양수 = tail = 약한 수요, 음수 = stop-through = 강한 수요)
- 기사에 tail(bp)만 있고 WI가 없으면 `WI = high_yield − tail/100`으로 계산한다.
- **반드시 실제로 읽은 https 기사 URL을 `sources`에 넣는다.** 두 출처가 일치하면 가장 좋고, 상충하면 더 구체적인(WI 수치를 명시한) 출처를 따른다.
- 확인할 수 없으면 `tail_bps: null`, `wi_rate: null`로 두고 요약에 "Tail: 확인된 WI 없음"이라고 쓴다. 추정으로 채우지 마라.

## 4. 요약 작성 (한국어, 정확히 이 형식)
```
🎯 총평: {Strong | Average | Weak | Disastrous}
1. Tail: {+x.x bp 또는 확인된 WI 없음}. {금리 영향 한 문장}
2. BTC: {btc}x vs 6avg {btc_avg6}x. {수요 평가 한 문장}
3. Indirect {in_pct}% vs 6avg {in_avg6}%. {해외 수요 한 문장}. PD {pd_pct}% vs 6avg {pd_avg6}%. {딜러 부담 한 문장, 높으면 경고}
4. 결론: {금리 경로·유동성에 대한 날카로운 한 문장}
```
- 첫 줄은 영어 등급 단어 그대로 (텔레그램 이모지가 이 단어로 결정됨).
- 등급 기준: tail ≤ −1bp 이고 BTC ≥ 6avg → Strong / tail > +2bp 또는 BTC가 6avg보다 0.15 이상 낮음 → Weak /
  tail > +3.5bp 이고 PD > 20% → Disastrous / 그 외 Average. tail이 없으면 BTC·PD·Indirect로 판단.
- 숫자는 레코드 값 그대로(반올림만). 1500자 이하.

## 5. 기록
`auction_ai.json`의 `entries["{cusip}|{auction_date}"]`에 쓴다:
```json
{"summary": "...", "tail_bps": -0.8, "wi_rate": 4.628, "sources": ["https://..."],
 "researched_at": "<현재 UTC ISO>", "model": "<너의 모델 ID>"}
```
최상위 `updated`도 현재 시각으로 갱신한다.

## 6. 자체 검증 → 커밋
```bash
python3 scripts/merge_auction_ai.py --check     # ❌가 하나라도 있으면 고치고 다시 실행 (파일은 안 바뀜)
git add data/auction_ai.json
git commit -m "chore(agent): 경매 분석 {경매일들}"
git push origin HEAD:main
```
push가 거부되면 `git pull --rebase origin main` 후 재시도(최대 3회). `data/auction_ai.json` 외의 변경이 생겼다면 커밋하지 마라.

마지막에 처리한 경매, 찾은 tail/WI와 출처, 못 찾은 건을 한 문단으로 보고한다.
