# 작업 인계 노트 (새 세션 시작용)

마지막 갱신: 2026-11-13 (07:40 UTC 상태 점검) · 구조·원칙은 [CLAUDE.md](../CLAUDE.md)가 기준이고, 이 파일은 **무엇을 왜 바꿨는지와 지금 열려 있는 일**만 적는다.

## 새 세션에서 시작하는 법
1. `~/macromonitor`에서 새 세션을 연다 → CLAUDE.md는 자동으로 읽힌다.
2. 첫 메시지 예: `ops/HANDOFF.md 읽고 현재 상태 확인한 다음 ___ 해줘`
3. 상태 점검이 필요하면: `data/health.json`, [이슈 라벨 health](https://github.com/z66g/macromonitor/issues?q=label%3Ahealth), 최근 Actions 실행.

## 2026-11 전체 점검·개편 (커밋 a555036 ~ 356a0e4)
**보안·사용자 간 충돌 차단 (bc9120f)**
- 인증 없는 쓰기/디버그 라우트 제거: `/qra-trigger·apply·dismiss·preview·debug`, `/news-trans-flush`, `/ofr`, `/h41`, `/ici-raw` 등.
  방문자 한 명이 QRA 값을 바꾸거나 캐시를 지우던 문제 해소.
- `/fred`는 허용 시리즈만, `/news-digest-send`는 KST 06·21시 ±20분 슬롯당 1회.
- innerHTML에 들어가는 외부/LLM 텍스트 `escH()`·`safeUrl()` 처리.
- KV 쓰기 검증(`KV_VALIDATORS`)으로 빈 결과가 정상 캐시를 덮지 않게 함.

**정확성 (bc9120f, fb15acb)**
- 유동성 예측모델: QRA를 분기별로 저장하고 실행 시점에 현재 분기 선택(`normalizeQra`), 주간 순차입 상·하한, QT = 총자산 13주 평균 감소분.
- 부채구조 '기타'에서 WDTGAL 제거(TGA 이중계산).
- NL 모멘텀: 주 단위 기준점, DTS 결측 시 H.4.1 TGA 대체.
- 캘린더 FRED release ID 교정(JOLTS 192, GDP 53, 소매 9 등), 2027 FOMC 일정, FOMC 의사록 = 회의 +21일.
- `/h41-history`: 실제 발표일(`releaseDates.json`) + 발표분별 영구 캐시 → 52주 조회 1초대.
- 사이트: 패널 상호 배타(`closeOtherPanels`), 경매 N 뱃지 키 `cusip|경매일`, fetch 타임아웃, 탭 재시도 버튼, 데이터 없음 = 회색 'n'.

**Cron (a555036)**
- Cloudflare 무료 플랜은 **계정 전체 cron 5개** → 1개(`5,15,25,35,45,55 * * * *`)로 통합, `CRON_JOBS` 표로 분 단위 분기.
- `CRON` 상수와 `wrangler.toml` 불일치로 2026-05~11 작업이 조용히 멈췄던 회귀 → 상수 하나로 통일.

**LLM 작업의 클라우드 예약작업 전환 (90c6a4a, 75cc5a2)**
- 원칙: LLM은 `data/*.json` 하나만 쓰고, 검증·반영·발송은 Actions 스크립트가 한다.
- 국채경매 분석 / QRA 감지 / 헬스체크 진단 → Claude 클라우드 예약작업 (CLAUDE.md 표 참고).
- 뉴스 번역·다이제스트는 **Gemini 유지** (사용자 결정).
- 클라우드 세션은 treasury.gov·treasurydirect.gov·Worker 도메인에 접속 불가 →
  QRA 원문은 `fetch-qra-source.yml`이 `data/qra_source.json`에 저장, 헬스체크는 `health-check.yml`(Actions)이 직접 실행.
- 실측 검증(2026-11-13): 경매 3건 tail 반영·텔레그램 발송, QRA 11/2 발표(Q4 618/850, 2027Q1 800/1000) 반영, 헬스체크 이슈 자동화 동작.

## 정리 (2026-11-13)
- 미사용 파일 삭제: `auction_test.html`, `favicon*.svg`(사이트는 PNG 아이콘 사용), `package.json`/`package-lock.json`(미사용 xlsx 의존성), 미추적 `AGENTS.md`·`.claude/launch.json`.

## 상태 점검 (2026-11-13 07:40 UTC)
- main = `031df06`, 최근 Actions 전부 성공(번역·다이제스트·Pages·QRA Apply·경매 병합·Health Check). 열린 이슈는 #1 하나.
- `data/health.json`(03:39Z): Actions 실패 1건 외 15개 항목 모두 ok.

## 이전 작업 요약 (2026-04~10)
- TGA: H.4.1 TGA 기준 주간 순차입 계산, DTS 일별 TGA 파이프라인(`fetch-dts.yml` → KV `tga_dts_v1`)을 T1·유동성 패널·텔레그램에 사용, 1일·주간 변화 동시 표시.
- 국채경매 텔레그램에서 Bills 제외. H.4.1 텔레그램 신호 방향 수정. 파비콘 = 홈 화면 아이콘 PNG.
- 인플레 히트맵 PPI를 `PPIFIS`(최종수요)로 교체. FRED `WRMFSL` 폐지 → ICI.
- KV 429 대응(쓰기 횟수 축소·변경 시에만 쓰기), 캐시 TTL을 cron 주기보다 길게.
- `asOf`(관측일)와 `releasedAt`(발표일) 분리해 신선도 판정.

## 열려 있는 일 / 지켜볼 것
- [ ] [이슈 #1](https://github.com/z66g/macromonitor/issues/1) (health): 11/13 03:02Z 배포 실패 기록만 남음(이미 수정됨).
  24h 창 때문에 11/13 23:00 점검에서도 warn이 남는 문제 → `c_actions`가 '같은 워크플로의 최근 완료 실행이 성공이면 복구됨'으로 분류하도록 수정
  (브랜치 `claude/handoff-status-review-2pe5sa`). **main 반영 후** 다음 23:00 UTC 점검에서 자동 종료되는지 확인.
- [ ] 헬스체크 진단 에이전트(23:30 UTC)는 아직 이상 상황에서 실행된 적 없음 — 첫 warn/fail 때 `diagnosis`가 이슈에 붙는지 확인.
- [ ] 다음 QRA(2027-02 초) 때 `fetch-qra-source.yml` → 에이전트 → `qra-apply.yml` 흐름이 사람 개입 없이 도는지 확인.
- [ ] FOMC 하드코딩은 2027년까지 — 2027 하반기에 2028 일정 추가(헬스체크가 60일 전 경고).

## 사용자가 정한 것 (바꾸기 전에 물어볼 것)
- TGA는 FRED가 아니라 H.4.1(주간)·DTS(일별) 사용.
- 뉴스 번역·다이제스트는 Gemini(GitHub Actions) 유지.
- Anthropic API 키는 나중을 위해 Cloudflare Worker에 그대로 둠.
- 클라우드 예약작업: 경매·QRA = Opus 5.5, 헬스체크 = Sonnet 5.5, 커넥터 없음.
- 큰 점검의 최종 검수는 Opus 5.5 또는 Fable 5.2로.
- cron-job.org: `check-qra` 작업 삭제 완료(2026-11-13). 남은 작업은 `auction-results` dispatch, 뉴스 다이제스트 발송, :00/:30 GH Actions 트리거.
