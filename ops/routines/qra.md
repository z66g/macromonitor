# QRA(분기 국채발행계획) 감지 에이전트 (클라우드 예약작업)

MacroMonitor의 유동성 예측모델은 미 재무부 QRA의 순차입 추정치와 분기말 현금잔고(TGA) 목표를 쓴다.
**너는 `data/qra.json` 하나만 수정·커밋한다.** 검증과 Cloudflare KV 반영은 GitHub Actions(`qra-apply.yml`)가 한다.

## 1. 준비
```bash
git pull --rebase origin main
```
현재 값: `data/qra.json`(없을 수 있음). 참고로 사이트가 지금 쓰는 값은
`https://macrolens-worker.macrolens.workers.dev/qra-status`의 `active`.

## 2. 최신 발표 찾기 — 반드시 재무부 원문
- 재무부는 2·5·8·11월 초 월요일 오후 3시(ET)에 **"Treasury Announces Marketable Borrowing Estimates"** 보도자료를 낸다.
- `https://home.treasury.gov/news/press-releases` 목록 또는 검색으로 가장 최근 것을 찾아 **원문 페이지를 직접 연다.**
- 원문에서 두 분기(이번 분기 + 다음 분기)마다 추출:
  - privately-held net marketable borrowing (십억 달러, 정수)
  - end-of-quarter cash balance assumption (십억 달러, 정수)
  - 그 수치가 들어 있는 **원문 문장 그대로**(evidence_quote)
- 분기 표기: "October – December 2026" → `"2026 Q4"`, "January – March 2027" → `"2027 Q1"`.
- 뉴스 요약·블로그가 아니라 treasury.gov 원문만 근거로 쓴다. 숫자가 원문에 그대로 없으면 쓰지 마라.

## 3. 비교
`data/qra.json`의 `announcement_date`·수치가 원문과 같으면 **아무것도 커밋하지 말고** "변경 없음"으로 끝낸다.

## 4. 기록 (형식 정확히)
```json
{
  "announcement_date": "YYYY-MM-DD",
  "source_url": "https://home.treasury.gov/news/press-releases/...",
  "pdf_url": null,
  "quarters": [
    {"quarter": "2026 Q4", "net_borrowing_billions": 0, "end_cash_balance_billions": 0, "evidence_quote": "원문 문장"},
    {"quarter": "2027 Q1", "net_borrowing_billions": 0, "end_cash_balance_billions": 0, "evidence_quote": "원문 문장"}
  ],
  "verified_at": "<현재 UTC ISO>",
  "applied_by": "agent",
  "model": "<너의 모델 ID>"
}
```

## 5. 자체 검증 → 커밋
```bash
python3 scripts/apply_qra.py --check     # 통과해야 함 (KV는 건드리지 않음)
git add data/qra.json
git commit -m "chore(agent): QRA {발표일}"
git push origin HEAD:main
```
push 거부 시 `git pull --rebase origin main` 후 재시도(최대 3회).

마지막에 발표일, 두 분기 수치, 원문 URL을 한 문단으로 보고한다.
