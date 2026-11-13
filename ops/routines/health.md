# 일일 헬스체크 에이전트 (클라우드 예약작업)

MacroMonitor(macromonitor.zbbg.kr)가 '성공 표시인데 실제로는 아무것도 안 하는' 상태에 빠지지 않았는지 매일 점검한다.
**너는 `data/health.json` 하나만 쓰고 커밋한다. 코드는 고치지 않는다(진단만).**
이상이 있으면 GitHub Actions(`health-alert.yml`)가 소유자에게 GitHub 이슈로 알린다.

## 1. 결정적 점검
```bash
git pull --rebase origin main
python3 scripts/health_check.py --out data/health.json
```

## 2. 이상 항목만 원인 조사 (읽기 전용)
`data/health.json`에서 status가 `warn`/`fail`인 항목마다 짧게 원인을 찾는다.
- 사용할 것: Worker 공개 엔드포인트(`https://macrolens-worker.macrolens.workers.dev/...`, **`force=1` 쓰지 마라** — 공유 캐시를 바꾼다),
  레포 코드(`worker.js`, `scripts/`, `.github/workflows/`), 데이터 파일, 외부 원천(FRED·TreasuryDirect·fiscaldata·federalreserve.gov),
  GitHub 공개 API(`https://api.github.com/repos/z66g/macromonitor/actions/runs?status=failure`, 실패 run의 `/jobs`로 실패 단계 확인).
- 흔한 원인: 외부 API 스키마/엔드포인트 변경, cron 문자열 불일치(`worker.js`의 `CRON` 상수 ↔ `wrangler.toml`),
  서브요청 한도(50), KV 429, 예상된 지연(주말·공휴일·발표 일정 — 이건 원인이 아니라 정상이라고 적어라).

## 3. 진단 기록
`data/health.json`에 `diagnosis` 배열을 추가한다(이상 항목이 없으면 빈 배열):
```json
"diagnosis": [
  {"check": "항목명", "likely_cause": "한 문장", "suggested_fix": "한 문장 (파일·함수 명시)", "confidence": "high|medium|low"}
]
```
정상 범위의 지연(예: 주말이라 수익률 미갱신)이면 해당 check의 status를 `ok`로 낮추고 detail 끝에 "(정상 지연: 이유)"를 붙인 뒤,
최상위 `status`를 남은 항목 기준으로 다시 계산한다(fail > warn > ok).

## 4. 커밋
```bash
git add data/health.json
git commit -m "chore(agent): 헬스체크 {status} {YYYY-MM-DD}"
git push origin HEAD:main
```
push 거부 시 `git pull --rebase origin main` 후 재시도(최대 3회). `data/health.json` 외 파일은 커밋하지 마라.

마지막에 종합 상태와 이상 항목·원인을 3줄 이내로 보고한다.
