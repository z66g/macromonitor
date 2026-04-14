# MacroMonitor 프로젝트 컨텍스트

## 프로젝트 구조
- **프론트엔드**: GitHub Pages `macromonitor.zbbg.kr` (레포: `github.com/z66g/macromonitor`)
- **백엔드**: Cloudflare Worker `macrolens-worker.macrolens.workers.dev`
- **KV 네임스페이스**: `MMF_KV` (binding: `MMF_KV`)
- **로컬 작업 디렉토리**: `~/macromonitor`

## 배포 패턴 (반드시 준수)
```bash
cd ~/macromonitor
cp ~/Downloads/{file} .          # worker.js/index.html → 루트, .yml → .github/workflows/
git add .
git commit -m "..."
git pull origin main --rebase && git push
```
- Worker 변경 → `deploy-worker.yml` 자동 트리거
- `worker.js` 먼저 배포 후 `index.html`
- `git pull --rebase` 필수 (GitHub Actions 자동 커밋 충돌 방지)

## 주요 파일
- `worker.js` / `index.html` → 루트
- `.github/workflows/*.yml` → GitHub Actions
- `CLAUDE.md` → Claude Code 컨텍스트 (이 파일)

## 탭 구성
- **T0 거시경제 모니터**: H.4.1 파싱, GDPNow, DXY, JPY, OFR FSI
- **T1 유동성&시장**: FRED NL, MMF, 국채수익률, VIX, MOVE, 원자재
- **T2 실물경제**: 지역 연준 PMI, 고용, 소비자
- **T3 글로벌**: 글로벌 유동성, DXY, AUD/USD, EUR/USD
- **T4 온체인**: 스테이블코인, RWA TVL, BTC ETF 플로우

## 슬라이드 패널
- `news-panel`: 뉴스 패널
- `cal-panel`: 이벤트 캘린더 (FRED 경제지표 일정)
- `tower-panel`: 유동성 예측모델 (liqTower)
- `auction-panel`: 국채경매 분석

## H.4.1 파싱 핵심
- `h41HistoryFetcher(url, weeksOverride)` — weeksOverride로 직접 주수 지정
- `fetchH41HtmlData()` — h41HistoryFetcher(url, 3) 호출해 2주치 파싱
- series에서 직접: reserve_balances, rrp(on_rrp_domestic), tga, loans, currency_circ, treasury_total
- FRED 별도 호출: WDTGAL(other_draining)만
- **순유동성 공식**: `reserve_credit - tga - on_rrp_domestic`

## KV 캐시 키 & TTL
```
liq: 2h, h41Html: 6h, calendar: 6h, news: 3h, t2: 6h, t3: 6h, onchain: 1h
newsDigest: 24h
```

## API 구성
| API | Key | 용도 |
|---|---|---|
| Anthropic | `ANTHROPIC_API_KEY` | 뉴스 다이제스트 생성, 뉴스 번역 (Haiku) |
| Gemini | `GEMINI_API_KEY` | QRA 검색, 국채경매 Tail 검색·요약 (Search Grounding) |
| Telegram | `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | 알림 발송 |

- Anthropic 모델: `claude-haiku-4-5-20251001`
- Gemini 모델: `gemini-2.5-flash` → 503/429 시 `gemini-2.0-flash` fallback

## 텔레그램 알림 구성 ★ 신규

### 발송 채널
- 개인 봇 → 브로드캐스트 채널 (구독자 채팅 불가 구조)
- Secrets: GitHub (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`) + Cloudflare Worker 동일

### 알림 1: 뉴스 다이제스트
- **발송 주체**: Cloudflare Worker Cron (`0 */3 * * *`)
- **로직**: `generateNewsDigest()` 완료 후 `await sendTelegramDigest(env, digest)` 호출
- **헬퍼 함수**: `sendTelegram(env, text)`, `sendTelegramDigest(env, digest)` — worker.js 하단
- **포맷**:
  ```
  📰 MacroMonitor 뉴스 | MM-DD HH:MM KST

  1. 요약 텍스트
  2. 요약 텍스트
  3. 요약 텍스트

  🔗 macromonitor.zbbg.kr
  ```

### 알림 2: 국채경매 결과
- **발송 주체**: GitHub Actions `fetch-auction-results.yml` 마지막 step
- **조건**: 오늘 날짜 + summary 완성 건만 발송 (없으면 스킵)
- **포맷**:
  ```
  🏦 국채경매 결과 | {term} {type} | {date}

  💪 총평 첫 줄

  • Tail: -1.2bp ✅
  • BTC: 2.68x (6avg 2.52x) ✅
  • Indirect: 63.3% (6avg 59.1%)
  • PD: 11.2% (6avg 13.8%) ✅
  • 규모: $58.0B

  🔗 macromonitor.zbbg.kr
  ```

### 알림 3: H.4.1 주간 유동성
- **발송 주체**: GitHub Actions `h41-telegram.yml`
- **스케줄**: 매주 목요일 22:00 UTC (금요일 07:00 KST) — H.4.1 릴리즈 후
- **데이터 소스**: `/h41-history?weeks=2` 엔드포인트 → `raw[0]`(최신), `raw[1]`(전주) 비교
- **단위**: 백만달러(M) 기준, `/1e6` → T, `/1e3` → B
- **포맷**:
  ```
  📊 Fed H.4.1 주간 유동성 | April 09, 2026

  • 연준총자산: $6.64T  Δ+17.4B 🟢
  • 준비금:    $3.18T  Δ+0.12T 🟢
  • TGA:       $697.1B  Δ-0.11T 🔴
  • ON RRP:    $2.4B   Δ-0.8B 🔴
  • 순유동성:  $5.94T  Δ+0.12T 🟢

  → 유동성 순증 — 위험자산 우호 환경 🟢

  🔗 macromonitor.zbbg.kr
  ```

## 국채경매 분석 파이프라인

### GitHub Actions 워크플로우
- `fetch-auction-results.yml`: 매일 UTC 16/17/18시 자동 실행 (평일)
  - TreasuryDirect JSONP → 오늘 Note/Bond/TIPS 경매 감지
  - Gemini tail 검색 (Search Grounding) + 요약 생성
  - `data/auction_results.json` 저장 → commit/push
  - **Telegram 발송** (오늘 summary 완성 건만)
- `backfill-auction-summaries.yml`: 수동 실행, START/END 파라미터로 배치 처리
- `h41-telegram.yml`: 매주 목요일 22:00 UTC H.4.1 유동성 알림

### auction_results.json 구조
```json
{
  "updated": "2026-04-13T...",
  "count": N,
  "results": [{
    "security_type": "Note",
    "security_term": "3-Year",
    "auction_date": "2026-04-07",
    "cusip": "91282CQJ3",
    "offering_amt": 58000000000,
    "summary": "🎯 총평: Strong\n1. Tail: ...",
    "_analysis": {
      "btc": 2.68, "btc_avg6": 2.52,
      "tail_bps": -1.2, "high_yield": 3.897, "wi_rate": 3.909,
      "pd_pct": 11.2, "pd_avg6": 13.8,
      "in_pct": 63.3, "in_avg6": 59.1,
      "grade": {"btc": "strong", "tail": "strong", "pd": "strong"}
    }
  }]
}
```

### Worker 엔드포인트
- `/auction-results?limit=100` — GitHub raw에서 읽어 반환 (5분 캐시)
- `/h41-history?weeks=N` — H.4.1 N주치 데이터 (raw 배열: 백만달러 단위)
- `/h41-html` — H.4.1 최신 파싱 (data 키 안에 valueB/deltaB 구조)
- `/news-digest-generate` — 뉴스 다이제스트 즉시 생성 + Telegram 발송
- `/liq-tower` — 유동성 예측모델

## Gemini API 관련
- 모델: `gemini-2.5-flash` 우선, 503/429 시 `gemini-2.0-flash` fallback
- 요청 사이 8초 딜레이 (10 RPM 한도)
- 무료 티어: 500 RPD / 10 RPM — 연속 호출 시 429 주의
- GitHub Secrets: `GEMINI_API_KEY` 등록 필요

## API 차단 이슈
- `fiscaldata.treasury.gov` → CF Worker IP 525 차단 → GitHub Actions 릴레이 사용
- `www.federalreserve.gov/dts` (Akamai) → CF Worker + 브라우저 차단 → GitHub Actions 릴레이
- `atlanta.federalreserve.org` RSS → 403 → FRED vintage API로 대체

## 주요 학습/원칙
- **대형 HTML 편집**: str_replace 사용 (line-range 삭제는 구조 파괴 위험)
- **YAML 안 Python**: base64 인코딩으로 삽입 (중괄호 충돌 방지)
- **IIFE 안 함수**: `window.함수명 = 함수명` 으로 전역 노출 필수
- **KST 날짜**: `nowKST()` / `todayKSTStr()` 전역 헬퍼 사용
- **Worker Cron fire-and-forget 주의**: `sendTelegram` 같은 비동기 호출은 반드시 `await` 필요 (응답 반환 후 Worker 종료로 실행 안 됨)
- **60 거래일 = 12주** H.4.1 데이터
- **H.4.1 순유동성**: `reserve_credit - tga - on_rrp_domestic` (백만달러 기준)
- **H.4.1 history 단위**: `raw[]` 배열은 백만달러(M), series는 십억달러(B)

## 데이터 소스
- H.4.1: `federalreserve.gov/releases/h41/` HTML 직접 파싱
- FRED API: `api.stlouisfed.org` (FRED_API_KEY 필요)
- TreasuryDirect JSONP: `treasurydirect.gov/TA_WS/securities/auctioned`
- DefiLlama: CORS 개방 (RWA TVL)
- GitHub Actions 릴레이: `data/auctions.json`, `data/auction_results.json`

## 환경변수 / Secrets
| 키 | 위치 |
|---|---|
| `FRED_API_KEY` | Cloudflare Worker (wrangler secret) |
| `ANTHROPIC_API_KEY` | Cloudflare Worker (wrangler secret) |
| `GEMINI_API_KEY` | Cloudflare Worker + GitHub Actions |
| `CLOUDFLARE_API_TOKEN` | GitHub Actions |
| `TELEGRAM_BOT_TOKEN` | Cloudflare Worker + GitHub Actions |
| `TELEGRAM_CHAT_ID` | Cloudflare Worker + GitHub Actions |
