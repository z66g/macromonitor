# 이벤트 결과 조사 에이전트 (클라우드 예약작업)

MacroMonitor 이벤트 캘린더에 발표가 끝난 미국 경제지표의 **실제치·예상치(컨센서스)·이전치**와 한 줄 해석을 붙인다.
**너는 `data/econ_results_ai.json` 하나만 수정·커밋한다.** 검증(FRED 공식 값 대조)·사이트 반영은 GitHub Actions
(`econ-results-merge.yml` → `scripts/merge_econ_results.py`)가 한다. 다른 파일은 절대 건드리지 마라.

> 이 세션의 네트워크 정책상 Worker 도메인·일부 정부 사이트는 열리지 않는다. 수치는 WebSearch로 찾는다.
> WebFetch가 막힌 기사는 검색 결과 제목·요약에 **명시된 수치만** 쓰고 그 URL을 출처로 남긴다. 추정·기억으로 채우지 마라.

## 1. 준비
```bash
git pull --rebase origin main
```
읽을 파일:
- `data/econ_spec.json` — 지표별 metric id·라벨·단위·기준기간(period) 형식. **이 정의에 있는 metric id만 쓴다.**
- `data/econ_calendar.json` — 조사 대상 이벤트(`events[]`: key, date, name, indicator)
- `data/econ_results.json` — 이미 게시된 결과(`results`), FRED 미반영으로 보류(`pending`), 거부(`rejects`와 사유)
- `data/econ_results_ai.json` — 네가 전에 쓴 항목. 없으면 `{"updated": null, "entries": {}}`로 시작

## 2. 대상 고르기
`econ_calendar.json`의 `events` 중:
- `date`가 오늘(KST) 이하이고 최근 10일 이내
- `estimated`가 true인 이벤트(날짜 추정)는 실제 발표가 그날 있었는지 검색으로 확인한 경우만
- `econ_results.json`의 `results`에 같은 key가 없음
- `econ_results_ai.json`에 같은 key가 없거나, `rejects`에 그 key가 있음(사유를 읽고 고쳐서 다시 조사)
- `pending`에 있는 key는 조사하지 마라 — FRED 반영을 기다리는 중이며 Actions가 다시 검증한다

오늘 발표분은 미국 발표 시각(보통 08:30 ET 또는 10:00 ET, FOMC는 14:00 ET)이 지났을 때만 조사한다.
대상이 없으면 아무것도 커밋하지 말고 "대상 없음"으로 끝낸다.

## 3. 조사 (이벤트마다)
- 검색 예: `US CPI October 2026 actual forecast`, `nonfarm payrolls November 2026 consensus`, `initial jobless claims week ended`
- 신뢰 출처: 발표 기관(BLS·BEA·Census·DOL·ISM·UMich·Fed) 보도자료, Reuters, Bloomberg, WSJ, MarketWatch, CNBC,
  Investing.com, ForexFactory, Trading Economics, InvestingLive(ForexLive)
- **실제치(actual)**: 발표 기관 수치. FRED에 있는 지표는 Actions가 FRED와 대조하므로 단위·기준기간을 정확히 맞춘다.
- **예상치(forecast)**: 발표 직전 시장 컨센서스(중간값). 출처마다 다르면 Reuters/Bloomberg → Investing.com 순으로 택하고 그 URL을 sources에 넣는다.
  찾을 수 없으면 null (FOMC는 시장 예상 금리 상단, STLFSI는 컨센서스가 없으니 null).
- **이전치(previous)**: 직전 기간 발표치. 이번 발표에서 수정됐으면 수정 전 값을 previous, 수정 후 값을 previous_revised에.
- 단위 규칙 (econ_spec.json의 unit):
  - `%` → 퍼센트 숫자 그대로 (3.0% → 3.0, −0.2% → -0.2). MoM·YoY를 혼동하지 마라.
  - `K` → 천 단위 (비농업 +119K → 119, 신규 청구 220K → 220, JOLTS 7.67M → 7670, 주택착공 연율 1.31M → 1310)
  - `pt` → 지수 그대로 (ISM 48.7, 미시간 53.6)
- **period** (기준기간): monthly `YYYY-MM`(지표가 가리키는 달, 발표 달 아님) · quarterly `YYYY-Qn` · weekly 주 종료일 `YYYY-MM-DD`
  (실업수당 = 그 주 토요일, STLFSI = 그 주 금요일) · FOMC = 회의 결정일 `YYYY-MM-DD`
- 한 이벤트에 여러 metric이 정의돼 있으면(예: CPI YoY·MoM·근원) 찾은 것은 모두 채운다. **대표(primary) 지표의 실제치는 필수.**

## 4. 해석 (한국어 한 문장, 200자 이하, HTML 금지)
예상 대비 결과와 시장 함의를 드라이하게: 예) "헤드라인 3.0%로 예상(3.1%) 하회, 근원 3.2%로 부합 — 12월 인하 기대를 소폭 지지."
등급·과장 표현, 투자 권유는 쓰지 않는다. 예상 대비 상회/부합/하회 판정은 Actions가 숫자로 계산하므로 문장과 모순되지 않게 쓴다.

## 5. 기록
`econ_results_ai.json`의 `entries["<key>"]` (key는 econ_calendar.json의 key 그대로):
```json
{"period": "2026-10",
 "metrics": {"yoy": {"actual": 3.0, "forecast": 3.1, "previous": 3.0, "previous_revised": null},
             "core_yoy": {"actual": 3.2, "forecast": 3.2, "previous": 3.3, "previous_revised": null}},
 "interpretation": "...",
 "sources": ["https://..."],
 "researched_at": "<현재 UTC ISO>", "model": "<너의 모델 ID>"}
```
최상위 `updated`도 현재 시각으로 갱신한다. 30일 넘은 entries는 지운다(게시된 결과는 econ_results.json에 남는다).

## 6. 자체 검증 → 커밋
```bash
python3 scripts/merge_econ_results.py --check
```
`reject`가 있으면 사유대로 고친다. 고칠 수 없는 항목은 entries에서 빼라(틀린 수치를 올리지 않는 것이 우선).
통과하면:
```bash
git add data/econ_results_ai.json
git commit -m "chore(agent): 이벤트 결과 <날짜들>"
git pull --rebase origin main && git push origin HEAD:main
```
push가 실패하면 pull --rebase 후 3회까지 재시도. 마지막에 조사한 이벤트·찾지 못한 값·출처를 짧게 보고한다.
