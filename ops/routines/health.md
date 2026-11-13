# 헬스체크 진단 에이전트 (클라우드 예약작업)

MacroMonitor의 결정적 헬스체크는 GitHub Actions(`health-check.yml`, 매일 23:00 UTC)가 실행해
`data/health.json`을 커밋하고, 이상이 있으면 GitHub 이슈를 연다. **너는 그 이상 항목의 원인을 진단해 이슈에 덧붙인다.**
**코드는 고치지 않는다. `data/health.json`의 `diagnosis`만 추가해 커밋한다.**

> 이 세션의 네트워크 정책상 Worker 도메인·treasury.gov 등 일부 외부 사이트에는 접속할 수 없다.
> 진단은 레포 코드·데이터·git 이력과, 접속 가능한 공개 페이지로 한다. 접속이 막히면 그 사실을 적고 넘어가라.

## 1. 준비
```bash
git pull --rebase origin main
cat data/health.json
```
- `checked_at`이 6시간보다 오래됐으면 "오늘 점검이 실행되지 않음(health-check.yml 확인 필요)"을 진단으로 적는다.
- `status`가 `ok`이면 **아무것도 커밋하지 말고** "정상"으로 끝낸다.
- 이미 `diagnosis`가 있고 `checked_at`이 같으면 끝낸다 (중복 진단 방지).

## 2. 이상 항목 원인 조사 (읽기 전용)
`status`가 `warn`/`fail`인 항목마다:
- 관련 코드: `worker.js`, `scripts/`, `.github/workflows/`, `ops/routines/`
- 최근 변경: `git log --since="3 days ago" --stat`, 데이터 파일의 최근 커밋
- 흔한 원인: 외부 API 스키마/엔드포인트 변경, cron 문자열 불일치(`worker.js`의 `CRON` 상수 ↔ `wrangler.toml`),
  Cloudflare 서브요청 한도(50), KV 429, 예약작업 에이전트 실패(요약·QRA 미갱신)
- **정상 범위의 지연**(주말·미국 공휴일·발표 일정상 아직 안 나옴)이면 그렇게 판단하고 이유를 적는다.

## 3. 진단 기록
`data/health.json`에 `diagnosis` 배열을 추가한다:
```json
"diagnosis": [
  {"check": "항목명", "likely_cause": "한 문장", "suggested_fix": "한 문장 (파일·함수 명시)", "confidence": "high|medium|low"}
]
```
정상 범위 지연으로 판단한 항목은 status를 `ok`로 낮추고 detail 끝에 "(정상 지연: 이유)"를 붙인 뒤,
최상위 `status`를 남은 항목 기준으로 다시 계산한다(fail > warn > ok). 다른 필드는 바꾸지 마라.

## 4. 커밋
```bash
git add data/health.json
git commit -m "chore(agent): 헬스체크 진단 {YYYY-MM-DD}"
git push origin HEAD:main
```
push 거부 시 `git pull --rebase origin main` 후 재시도(최대 3회). `data/health.json` 외 파일은 커밋하지 마라.

마지막에 종합 상태와 이상 항목·원인을 3줄 이내로 보고한다.
