# MacroMonitor 프로젝트 컨텍스트

## 프로젝트 구조
- **프론트엔드**: GitHub Pages `macromonitor.zbbg.kr` (레포: `github.com/z66g/macromonitor`, 공개)
- **백엔드**: Cloudflare Worker `macrolens-worker.macrolens.workers.dev` (무료 플랜)
- **KV 네임스페이스**: `MMF_KV` (id `66c313c710a04a4eb04775b80a44f313`)
- **로컬 작업 디렉토리**: `~/macromonitor`
- **작업 이력·열린 일**: [`ops/HANDOFF.md`](ops/HANDOFF.md) — 새 세션은 여기부터 확인

## 운영 원칙 (2026-11 전체 점검 이후 — 반드시 지킬 것)
1. **방문자 요청은 공유 상태를 바꾸지 않는다.** 인증 없는 쓰기/삭제/발송 엔드포인트를 만들지 말 것.
   공유 값(QRA 등) 변경은 `data/*.json` 커밋 → 검증 워크플로 → KV 경로로만.
2. **빈 결과로 정상 데이터를 덮어쓰지 않는다.** Worker는 `kvPut`의 `KV_VALIDATORS`, 스크립트는 조회 실패 시 예외로 종료.
3. **LLM은 생성만, 전달은 결정적 파이프라인.** 에이전트/LLM 출력은 검증 스크립트를 통과해야 사이트·텔레그램에 반영.
4. **외부/LLM 텍스트는 `escH()`로, 링크는 `safeUrl()`로** 감싸서 innerHTML에 넣는다 (index.html 전역 헬퍼).
5. **cron 문자열은 `worker.js`의 `CRON` 상수 = `wrangler.toml`.** 불일치하면 해당 작업이 조용히 안 돈다 (2026-05~11 회귀 사례).
6. 데이터 없음은 초록 '안정'이 아니라 회색 `n`('수신 실패')으로 표시.
7. **방문자 요청이 업스트림을 몰아치지 않게.** 여러 종목·시리즈는 cron이 KV에 묶어 두고 사이트는 묶음 1건만 읽는다
   (Yahoo → `/market`). 프론트 fetch는 전부 `fetchT`(타임아웃·재시도·동시 6건 제한)로, 일부가 빠진 섹션은 `markPartial`로 자동 재로드.

## 배포 패턴
```bash
cd ~/macromonitor
git add <files> && git commit -m "..."
git pull origin main --rebase && git push
```
- `worker.js`/`wrangler.toml` 변경 → `deploy-worker.yml` 자동 배포
- 봇이 `data/*.json`을 수시로 커밋하므로 `git pull --rebase` 필수. 워크플로는 `scripts/commit_data.sh` 사용(rebase+재시도)
- 배포 전 확인: `node --check worker.js`, index.html 스크립트 블록 문법, CRON 일치

## 주요 파일
- `worker.js` / `index.html` → 루트
- `scripts/` → 워크플로가 실행하는 Python (YAML heredoc/base64 대신 여기에 작성)
  - `td_common.py` TreasuryDirect 공용(새 스키마 매핑) · `fetch_auction_results.py` · `fetch_auctions.py`
  - `merge_auction_ai.py`(에이전트 결과 검증·병합, `--check`) · `send_auction_telegram.py`
  - `apply_qra.py`(QRA 검증→KV, `--check`) · `health_check.py` · `commit_data.sh`
- `ops/routines/*.md` → 클라우드 예약작업 에이전트 지침 (프롬프트를 레포에서 버전 관리)
- `.github/workflows/*.yml`

## 탭 / 패널
- **T0** H.4.1 파이프 진단, VIX/MOVE, DXY, JPY, OFR FSI, CSP(TIPS·MMF=ICI)
- **T1** NL 모멘텀(주 단위), NL 1~4주 예측, MMF 질, 국채수익률, 인플레 히트맵, 원자재
- **T2** 지역 연준 PMI, 고용 파이프라인, 소비자 체력 · **T3** 글로벌 · **T4** 온체인
- 슬라이드 패널 `news / cal / tower / auction` — **한 번에 하나만**(`window.closeOtherPanels`, Esc로 닫기)

## UI 상태 표시 규칙 (2026-11 UI 점검)
- **헤더 LED**: `setLed(id, state)` 하나로 — `unknown`(빈 링, 확인 전) / `ok` / `warn`(데이터 오래됨) / `fail`(수신 실패).
  로드 4~5초 후 4개 모두 가볍게 확인. 임계: 뉴스 3h · 캘린더 24h · 경매 4일 · 예측모델 10일. 시각 없으면 warn(초록 금지)
- **패널 버튼 활성**: `setPanelBtnActive(btnId, on)` — 모든 열기/닫기 경로에서 같은 색(#58A6FF)
- **신호 색**: `SIG_STYLE` g 안정 · a 주의 · r 위험 · k 위기 · n 수신 실패(회색). 입력이 없으면 반드시 `'n'`
  (규칙 첫 줄을 `x == null ? 'n'`로). 파이프 헤더만 배관 용어 `PIPE_LABEL`(안정·마찰·경색·위기). 일부 행만 'n'이면 최악 신호 + '일부 수신 실패'
- **데이터 상태 배지**: `dsSet`의 asOf는 **데이터 날짜**(오늘 날짜로 채우지 말 것), 'market'·'daily' 신선도는 KST 영업일 문자열 비교.
  로더 catch에서 `dsFailTab()` 호출. 값이 없으면 `$0`/`+0.0%` 대신 `—`
- **시각 표기**: `fmtKST(iso)` → "MM/DD HH:mm KST" (getHours/toLocaleTimeString 금지 — 기기 시간대)
- 모바일(≤767px) 탭 대상 최소 28px, 7px 글자는 9px로 (끝의 공용 style 블록)

## Worker cron (무료 플랜: 계정 전체 cron 5개 한도 → 1개만 사용)
`CRON = '5,15,25,35,45,55 * * * *'` — 10분마다 깨어나 `CRON_JOBS` 표대로 실행:
| 분 | 시각(UTC) | 작업 |
|---|---|---|
| :05 | 매시 | 뉴스 RSS → `news_cache_v1` |
| :35 | 매시 | 뉴스 RSS + T4 온체인(`onchain_macro_v1`) |
| :15 / :25 / :45 / :55 | 1·7·13·19시 외 | Yahoo 시세 스냅샷(33종목) → `market_snap_v1` |
| :15 | 1·7·13·19시 | H.4.1 파싱 → liq + 국채수익률 |
| :25 | 〃 | T2 + 시세 스냅샷 앞 절반 |
| :45 | 〃 | T3 + 유동성 예측모델 + ICI |
| :55 | 〃 | 캘린더 + 시세 스냅샷 전체 |
- 시세 스냅샷: `MARKET_TICKERS`(worker.js), 종목별 실패 시 직전 값 유지, 값이 그대로면 1시간에 한 번만 KV 쓰기.
  사이트는 `/market` 1건 → 3mo·1mo 차트·현재가. 스냅샷에 없거나 6h 넘은 종목, 그 외 기간(6M·1Y 버튼)만 `/yahoo`·`/yahoo-chart` 실시간.
  **사이트에 Yahoo 종목을 추가하면 `MARKET_TICKERS`에도 추가**(서브요청: 비갱신 시각 45건, :25 24건, :55 38건 예산 안에서).
- 1회 실행당 서브요청 50건 한도 → 작업을 시각별로 분산. cron-job.org(:00/:30 GH Actions 트리거)와 겹치지 않게 함.
- `force=1`도 캐시가 10분 이내면 재계산 안 함(`cacheFirst`) — 동시 새로고침 충돌 방지.

## KV 캐시 TTL (cron 주기 6h보다 길게)
```
liq / t2 / t3 / calendar / liqTower / h41Html: 12h · yieldsHist: 24h · newsCache: 3h · onchain: 3h · market: 3일(종목별 fetchedAt)
mmf_history_v1(ICI 주간 누적, /ici는 이것만 읽음): 400일 · tga_dts_v1: 4일 · qra_active_v1: 400일 · h41_rel_v1_YYYYMMDD(발표분별 영구): 400일
```

## H.4.1
- `/h41-history`: `releaseDates.json`(실제 발표일, 휴일 이동 반영) + 발표분별 KV 영구 캐시, 1회 원격 조회 상한 40
- **순유동성**: `reserve_credit − TGA − ON RRP` (NL 모멘텀: TGA=DTS 일별, 결측 시 H.4.1 TGA 대체, 7일 간격 기준점)
- 부채구조 '기타'에 WDTGAL을 더하지 말 것 (WDTGAL = TGA 주평균 → 이중계산)
- QT 드레인 = H.4.1 총자산 13주 평균 감소분 (만기 보유액 아님)

## QRA (유동성 예측모델의 차입·TGA 목표)
- `fetch-qra-source.yml`(월·수 20:30 UTC)이 재무부 'Financing Estimates' 원문 → `data/qra_source.json`
  (클라우드 에이전트 세션은 treasury.gov 접속 불가 → Actions가 원문을 레포에 저장)
- 에이전트가 원문에서 추출 → `data/qra.json`(이번+다음 분기 `quarters[]`) → `qra-apply.yml` → `apply_qra.py` 검증
  (인용문이 원문에 **글자 그대로** 있는지, 수치가 인용문 금액과 같은지, 분기·범위) → KV
- Worker `normalizeQra()`가 현재 분기 값을 실행 시점에 선택. `/qra-status`는 `{active}`만 반환(읽기 전용)
- 사이트에서는 수정 불가. 반영 후 7일간 탭 위 안내 배너(닫기는 localStorage), 유동성 패널 QRA 카드에 적용 방식 캡션
- 소유자 수동 수정: GitHub에서 `data/qra.json` 편집·커밋 (`"applied_by": "manual"`)

## 국채경매 파이프라인
| 단계 | 주체 | 시각 |
|---|---|---|
| 수집·지표·6회평균 | `fetch-auction-results.yml` (cron-job.org dispatch `auction-results`) | KST 06:00 화-토 |
| 예정 경매 목록 | `fetch-auctions.yml` (schedule `20 */4`) | 4시간마다 |
| tail·WI 조사 + 요약 | **클라우드 에이전트** → `data/auction_ai.json` | KST 06:30 화-토 |
| 검증·반영·텔레그램 | `auction-ai-merge.yml` (push 트리거 + 백업 schedule) | 에이전트 커밋 직후 |
- TreasuryDirect 2026-10 스키마 변경: `competitiveAuctionCloseDate`, `*CompetitiveParAwarded` 등 → `td_common.py`
- 병합 키 `cusip|경매일` (리오프닝은 원물과 cusip 동일). 요약·tail·`telegram_sent` 절대 삭제 안 함
- PD/Direct/Indirect = 경쟁입찰 낙찰 기준 비중. 6회 평균은 같은 (종류, 원물 만기) 직전 6회
- 텔레그램: 요약 완성 + 미발송 이표채만, HTML 이스케이프, 4096자 분할

## 클라우드 예약작업 (claude.ai/code/routines, 커넥터 없음)
| 이름 | 모델 | cron(UTC) | 지침 | 쓰는 파일 |
|---|---|---|---|---|
| 국채경매 분석 | Opus 5.5 | `30 21 * * 1-5` | `ops/routines/auction.md` | `data/auction_ai.json` |
| QRA 감지 | Opus 5.5 | `0 22 * * 1,3` | `ops/routines/qra.md` | `data/qra.json` |
| 헬스체크 진단 | Sonnet 5.5 | `30 23 * * *` | `ops/routines/health.md` | `data/health.json`의 `diagnosis` → `health-alert.yml` |
- 결정적 헬스체크 자체는 `health-check.yml`(Actions, 23:00 UTC)이 실행·커밋·이슈 처리. 에이전트는 이상 시 진단만.
- **클라우드 세션 네트워크 제약**: Worker 도메인·treasury.gov·treasurydirect.gov 접속 불가(2026-11 확인).
  외부 데이터가 필요하면 Actions가 받아 `data/`에 저장하고 에이전트는 파일을 읽는 구조로 만들 것.

## 텔레그램 (구독자 브로드캐스트 채널 — 운영 경고는 보내지 말 것, GitHub 이슈 사용)
- 뉴스 다이제스트: Worker `/news-digest-send` (cron-job.org KST 06:00/21:00, ±20분·시간대당 1회만 동작)
- 국채경매 결과: `auction-ai-merge.yml` → `send_auction_telegram.py`
- H.4.1 주간 유동성: `h41-telegram.yml` (목 22:00 UTC) · 데일리 마켓: `daily-market-telegram.yml`

## LLM
- **Gemini 2.5 Flash** (GitHub Actions): 뉴스 번역 `translate-news.yml`(30분), 다이제스트 `generate-news-digest.yml`(3시간)
  - 번역 맵은 현재 뉴스 + 최근 4000건만 유지 (KV 값 25MB 한도)
- **Claude 클라우드 에이전트**: 경매 분석, QRA, 헬스체크 (위 표)
- Worker 안에는 LLM 호출 코드가 없음(CF egress 차단 → 2026-11-13 코드까지 삭제). LLM 작업은 Actions·클라우드 에이전트에서만

## 공개 엔드포인트 (읽기 전용)
`/market /liq /liq-tower /t2 /t3 /calendar /yields-hist /h41-history /h41-html /ici /pipe3 /ofr-fsi /dxy /jpy
/yahoo /yahoo-chart /onchain-macro /news /news-digest /qra-status /auction-results /fred(허용 시리즈만)`

## API 차단 / 외부 변경 이력
- `fiscaldata.treasury.gov` → CF Worker 525 → GitHub Actions 릴레이 (`fetch-dts.yml`)
- NY Fed `rp/repo/propositions/search` 폐지 → `rp/repo/all/results/last/N.json` (하루 2회 운용, 날짜별 합산)
- FRED `WRMFSL` 2021-02 폐지 → ICI 사용
- 캘린더 FRED release ID: CPI 10, PPI 46, PCE 54, JOLTS 192, GDP 53, 소매 9, 주택착공 27, 미시간 91, 내구재 95, STLFSI 187
- FOMC 일정은 `fetchFomcDates()` 하드코딩(2027년까지) — 헬스체크가 소진 60일 전 경고

## 주요 학습/원칙
- **IIFE 안 함수**: `window.함수명 = 함수명` 으로 전역 노출 필수
- **KST 날짜**: `nowKST()` / `todayKSTStr()`. 로컬 자정 Date에 `toISOString()` 쓰지 말 것(KST 하루 밀림)
- **Worker cron 비동기**: `ctx.waitUntil` 안에서 반드시 `await` (응답 후 종료로 유실)
- **Chart.js**: 다시 그리는 차트는 `replaceChart(key, ctx, cfg)`
- **프론트 데이터 로딩**: 새 fetch는 `fetchT`/`sharedFetch`(같은 URL 2분 공유)/`marketQuotes`·`marketChart`(스냅샷)만 사용,
  `fetch()` 직접 호출 금지. 섹션 로더는 끝에서 `markPartial(key, 빠진게있나, 조용한재로드)` 호출
  (6초·12초 뒤 자동 재시도 2회, 탭 재진입 시 `retryPartials`). OFR은 서버사이드 403이라 브라우저 직접 호출 유지
- FRED `realtime_start`(=releasedAt)는 시리즈의 최근 발표/개정일 — 신선도 판정에 사용

## 환경변수 / Secrets
| 키 | 위치 |
|---|---|
| `FRED_API_KEY` | Cloudflare Worker |
| `GEMINI_API_KEY` | GitHub Actions |
| `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID` | GitHub Actions |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Cloudflare Worker + GitHub Actions |
