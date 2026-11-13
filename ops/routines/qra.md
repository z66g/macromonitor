# QRA(분기 국채발행계획) 감지 에이전트 (클라우드 예약작업)

MacroMonitor의 유동성 예측모델은 미 재무부 QRA의 순차입 추정치와 분기말 현금잔고(TGA) 목표를 쓴다.
**너는 `data/qra.json` 하나만 수정·커밋한다.** 검증과 Cloudflare KV 반영은 GitHub Actions(`qra-apply.yml`)가 한다.

> 이 세션의 네트워크 정책상 treasury.gov와 Worker 도메인에는 접속할 수 없다. 시도하지 마라.
> 재무부 원문은 GitHub Actions(`fetch-qra-source.yml`)가 `data/qra_source.json`에 받아 둔다 — **이 파일이 유일한 근거다.**

## 1. 준비
```bash
git pull --rebase origin main
cat data/qra_source.json      # url, announcement_date, text(원문 본문)
cat data/qra.json 2>/dev/null # 현재 값 (없을 수 있음)
```
- `qra_source.json`이 없으면 "원문 없음"으로 보고하고 끝낸다.
- `qra.json`의 `announcement_date`와 `source_url`이 `qra_source.json`과 같으면 **커밋하지 말고** "변경 없음"으로 끝낸다.

## 2. 원문에서 추출 (`qra_source.json`의 `text`만 사용)
두 분기(이번 분기 + 다음 분기)마다:
- privately-held net marketable borrowing → `net_borrowing_billions` (십억 달러 정수)
- end-of-quarter cash balance → `end_cash_balance_billions` (십억 달러 정수, "$1.000 trillion" = 1000)
- `evidence_quote`: 그 두 숫자가 들어 있는 **문장을 `text`에서 글자 그대로 복사**(대시·공백 포함). 고쳐 쓰지 마라 — 검증이 원문과 글자 단위로 대조한다.
- 분기 표기: "October–December 2026" → `"2026 Q4"`, "January–March 2027" → `"2027 Q1"`.
- 지난 분기 실적("During the July–September ... Treasury borrowed")은 넣지 마라. 추정치 두 개만.

## 3. 기록 (형식 정확히)
```json
{
  "announcement_date": "<qra_source.json의 announcement_date>",
  "source_url": "<qra_source.json의 url>",
  "pdf_url": null,
  "quarters": [
    {"quarter": "2026 Q4", "net_borrowing_billions": 0, "end_cash_balance_billions": 0, "evidence_quote": "원문 문장 그대로"},
    {"quarter": "2027 Q1", "net_borrowing_billions": 0, "end_cash_balance_billions": 0, "evidence_quote": "원문 문장 그대로"}
  ],
  "verified_at": "<현재 UTC ISO>",
  "applied_by": "agent",
  "model": "<너의 모델 ID>"
}
```

## 4. 자체 검증 → 커밋
```bash
python3 scripts/apply_qra.py --check     # 인용문 원문 대조·수치 일치·분기·범위 검증 (KV는 건드리지 않음)
git add data/qra.json
git commit -m "chore(agent): QRA {발표일}"
git push origin HEAD:main
```
검증이 실패하면 메시지대로 고친 뒤 다시 실행한다. push 거부 시 `git pull --rebase origin main` 후 재시도(최대 3회).

마지막에 발표일, 두 분기 수치, 원문 URL을 한 문단으로 보고한다.
