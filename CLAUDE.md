# MacroMonitor 프로젝트 컨텍스트

## 프로젝트 구조
- **프론트엔드**: GitHub Pages `macromonitor.zbbg.kr` (레포: `github.com/z66g/macromonitor`, 공개)
- **백엔드**: Cloudflare Worker `macrolens-worker.macrolens.workers.dev` (무료 플랜)
- **KV 네임스페이스**: `MMF_KV` (id `66c313c710a04a4eb04775b80a44f313`)
- **로컬 작업 디렉토리**: `~/macromonitor`

## 운영 원칙 (2026-11 전체 점검 이후 — 반드시 지킬 것)
1. **방문자 요청은 공유 상태를 바꾸지 않는다.** 인증 없는 쓰기/삭제/발송 엔드포인트를 만들지 말 것.
   공유 값(QRA 등) 변경은 `data/*.json` 커밋 → 검증 워크플로 → KV 경로로만.
2. **빈 결과로 정상 데이터를 덮어쓰지 않는다.** Worker는 `kvPut`의 `KV_VALIDATORS`, 스크립트는 조회 실패 시 예외로 종료.
3. **LLM은 생성만, 전달은 결정적 파이프라인.** 에이전트/LLM 출력은 검증 스크립트를 통과해야 사이트·텔레그램에 반영.
4. **외부/LLM 텍스트는 `escH()`로, 링크는 `safeUrl()`로** 감싸서 innerHTML에 넣는다 (index.html 전역 헬퍼).
5. **cron 문자열은 `worker.js`의 `CRON` 상수 = `wrangler.toml`.** 불일치하면 해당 작업이 조용히 안 돈다 (2026-05~11 회귀 사례).
6. 데이터 없음은 초록 '안정'이 아니라 회색 `n`('수신 실패')으로 표시.

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
- 슬라이드 패널 `news / cal / tower / auction` — **한 번에 하나만**(`window.closeOtherPanels`)

## Worker cron (무료 플랜: 계정 전체 cron 5개 한도 → 1개만 사용)
`CRON = '5,15,25,35,45,55 * * * *'` — 10분마다 깨어나 `CRON_JOBS` 표대로 실행:
| 분 | 시각(UTC) | 작업 |
|---|---|---|
| :05 / :35 | 매시 | 뉴스 RSS → `news_cache_v1` |
| :15 | 1·7·13·19시 | H.4.1 파싱 → liq + 국채수익률 |
| :25 | 〃 | T2 |
| :45 | 〃 | T3 + 유동성 예측모델 + ICI |
| :55 | 〃 | 캘린더 |
- 1회 실행당 서브요청 50건 한도 → 작업을 시각별로 분산. cron-job.org(:00/:30 GH Actions 트리거)와 겹치지 않게 함.
- `force=1`도 캐시가 10분 이내면 재계산 안 함(`cacheFirst`) — 동시 새로고침 충돌 방지.

## KV 캐시 TTL (cron 주기 6h보다 길게)
```
liq / t2 / t3 / calendar / liqTower / h41Html: 12h · yieldsHist: 24h · newsCache: 3h · onchain: 1h
tga_dts_v1: 4일 · qra_active_v1: 400일 · h41_rel_v1_YYYYMMDD(발표분별 영구): 400일
```

## H.4.1
- `/h41-history`: `releaseDates.json`(실제 발표일, 휴일 이동 반영) + 발표분별 KV 영구 캐시, 1회 원격 조회 상한 40
- **순유동성**: `reserve_credit − TGA − ON RRP` (NL 모멘텀: TGA=DTS 일별, 결측 시 H.4.1 TGA 대체, 7일 간격 기준점)
- 부채구조 '기타'에 WDTGAL을 더하지 말 것 (WDTGAL = TGA 주평균 → 이중계산)
- QT 드레인 = H.4.1 총자산 13주 평균 감소분 (만기 보유액 아님)

## QRA (유동성 예측모델의 차입·TGA 목표)
- `data/qra.json`(이번+다음 분기 `quarters[]`) → `qra-apply.yml` → `apply_qra.py` 검증(treasury.gov 원문·분기·범위·인용) → KV
- Worker `normalizeQra()`가 현재 분기 값을 실행 시점에 선택. 사이트에서는 수정 불가(배너 닫기는 localStorage)
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
| 일일 헬스체크 | Sonnet 5.5 | `30 23 * * *` | `ops/routines/health.md` | `data/health.json` → `health-alert.yml`이 GitHub 이슈 |

## 텔레그램 (구독자 브로드캐스트 채널 — 운영 경고는 보내지 말 것, GitHub 이슈 사용)
- 뉴스 다이제스트: Worker `/news-digest-send` (cron-job.org KST 06:00/21:00, ±20분·시간대당 1회만 동작)
- 국채경매 결과: `auction-ai-merge.yml` → `send_auction_telegram.py`
- H.4.1 주간 유동성: `h41-telegram.yml` (목 22:00 UTC) · 데일리 마켓: `daily-market-telegram.yml`

## LLM
- **Gemini 2.5 Flash** (GitHub Actions): 뉴스 번역 `translate-news.yml`(30분), 다이제스트 `generate-news-digest.yml`(3시간)
  - 번역 맵은 현재 뉴스 + 최근 4000건만 유지 (KV 값 25MB 한도)
- **Claude 클라우드 에이전트**: 경매 분석, QRA, 헬스체크 (위 표)
- Worker 안의 LLM 호출 코드는 사용하지 않음(CF egress 차단) — 라우트 제거됨

## 공개 엔드포인트 (읽기 전용)
`/liq /liq-tower /t2 /t3 /calendar /yields-hist /h41-history /h41-html /ici /pipe3 /ofr-fsi /dxy /jpy
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
- FRED `realtime_start`(=releasedAt)는 시리즈의 최근 발표/개정일 — 신선도 판정에 사용

## 환경변수 / Secrets
| 키 | 위치 |
|---|---|
| `FRED_API_KEY` | Cloudflare Worker |
| `GEMINI_API_KEY` | GitHub Actions |
| `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID` | GitHub Actions |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Cloudflare Worker + GitHub Actions |
