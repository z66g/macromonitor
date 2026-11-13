/**
 * MacroLens — Cloudflare Worker
 * 역할:
 *   1. FRED API 프록시 (API Key 은닉)
 *   2. NY Fed Primary Dealer CSV 프록시 (CORS 우회)
 *   3. Fed H.4.1 HTML 파싱 (대차대조표 자동 분석)
 *   4. 모든 응답에 CORS 헤더 부착
 *
 * 환경변수 (CF Dashboard > Workers > Settings > Variables):
 *   FRED_API_KEY  =  발급받은 FRED API 키
 */

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json; charset=utf-8',
  'Cache-Control': 'no-store, no-cache, must-revalidate',
};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// KV 캐시 키 & TTL 설정 (FRED 데이터만 캐시)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const KV_KEYS = {
  liq:        'liq_data_v2',
  yieldsHist: 'yields_hist_v1',
  calendar:   'calendar_v1',
  t2:         't2_data_v1',
  t3:         't3_global_v1',
  qraActive:  'qra_active_v1',
  liqTower:   'liq_tower_v1',
  onchainMacro: 'onchain_macro_v1',
  h41Html:    'h41_html_v1',
  newsCache:  'news_cache_v1',
  newsTransMap: 'news_trans_map_v1',
  newsDigest:        'news_digest_v1',    // 핵심뉴스 (장마감/장시작)
  newsDigestHistory: 'news_digest_history_v1', // 다이제스트 누적 (배치 텔레그램용)
  tgaDts:            'tga_dts_v1',             // DTS 일별 TGA 잔고 (GitHub Actions)
  market:            'market_snap_v1',         // Yahoo 시세·3개월 일봉 스냅샷 (cron 갱신, 사이트는 이것만 읽음)
};
// ── Cron — wrangler.toml [triggers] crons 와 반드시 동일하게 유지 ──
// Cloudflare 무료 플랜은 '계정 전체' cron 5개 한도(다른 Worker와 공유) → cron 1개만 사용하고
// 실행 시각(UTC)으로 작업을 고른다. 한 번 실행당 서브요청 50건 한도도 지켜진다.
const CRON = '5,15,25,35,45,55 * * * *';
const REFRESH_HOURS = [1, 7, 13, 19];        // 6시간 갱신 시각 (UTC) = KST 10·16·22·04시
// cron-job.org가 :00/:30에 GH Actions(KV 쓰기)를 트리거하므로 Worker는 그 분을 피함
const CRON_JOBS = {
  5:  'news',                                // 매시 — 뉴스 RSS (~15건)
  35: 'newsOnchain',                         // 매시 — 뉴스 RSS + T4 온체인 (~15 + ~14건)
  15: 'groupA',                              // 갱신 시각 — H.4.1 → liq + 수익률 (~27건)   · 그 외 시각: 시세 스냅샷
  25: 'groupB',                              // 갱신 시각 — T2 (~21건) + 시세 절반(~17건) · 그 외 시각: 시세 스냅샷
  45: 'groupC',                              // 갱신 시각 — T3 + 유동성 예측모델 + ICI (~27건) · 그 외 시각: 시세 스냅샷
  55: 'groupD',                              // 갱신 시각 — 캘린더(~8건) + 시세 전체(~34건)    · 그 외 시각: 시세 스냅샷
};
// 시세 스냅샷(Yahoo) — 매시 :15/:25/:45/:55 (갱신 시각엔 :25 절반 + :55 전체) → 최대 지연 약 20~30분, 보통 10~20분.
// 방문자 요청은 KV만 읽으므로 Yahoo 동시 요청 폭주(429)로 차트가 비는 일이 없다.

// TTL은 cron 주기(6h)보다 길게 — 한 번 갱신이 실패/지연돼도 캐시 공백이 생기지 않도록
const KV_TTL = {
  liq:        43200,
  yieldsHist: 86400,
  calendar:   43200,
  t2:         43200,
  t3:         43200,
  qraActive:  86400 * 95,
  liqTower:   43200,
  onchainMacro: 10800,   // cron이 매시 :35 갱신 — 몇 번 실패해도 캐시 공백 없게
  market:       86400 * 3,   // 종목별로 직전 정상 값을 유지하므로 길게 (신선도는 종목별 fetchedAt으로 판정)
  h41Html:    43200,
  newsCache:  10800,   // 30분 cron 기준 여유 — 몇 번 실패해도 GH Actions(번역·다이제스트)가 캐시를 읽을 수 있게
  newsTransMap: 2592000,
  newsDigest:        86400,              // 24h
  newsDigestHistory: 86400 * 2,          // 48h (하루 2회 발송 사이 안전 보장)
};

const kvGet = async (env, key) => {
  try { return await env.MMF_KV.get(key, { type: 'json' }); }
  catch(e) { return null; }
};
// ── 캐시 우선 조회 — force=1(새로고침)이어도 캐시가 FORCE_MIN_AGE 이내면 재계산하지 않음 ──
// 여러 방문자가 동시에 새로고침해도 공유 캐시를 연달아 덮어쓰거나 FRED 한도를 소진하지 않도록.
const FORCE_MIN_AGE_MS = 10 * 60 * 1000;
const cacheFirst = async (env, key, force) => {
  const cached = await kvGet(env, key);
  if (!cached) return null;
  if (!force) return cached;
  const age = Date.now() - new Date(cached._savedAt || 0).getTime();
  return age < FORCE_MIN_AGE_MS ? cached : null;
};

// ── 저장 전 검증: 업스트림 장애로 비거나 깨진 결과가 정상 캐시를 덮어쓰지 않도록 ──
// 검증 실패 시 KV를 건드리지 않고(이전 정상 값 유지) 호출자에게 데이터만 반환한다.
const countSeries = (o, acc = { total: 0, empty: 0 }) => {
  if (Array.isArray(o) || !o || typeof o !== 'object') return acc;
  for (const [k, v] of Object.entries(o)) {
    if (k === 'series' && Array.isArray(v)) { acc.total++; if (!v.length) acc.empty++; }
    else countSeries(v, acc);
  }
  return acc;
};
const KV_VALIDATORS = {
  [KV_KEYS.liq]:        d => d?.yields?.dgs10?.current != null && d?.inflation?.cpi?.current != null,
  [KV_KEYS.t2]:         d => !d?.error && d?.labor?.unrate?.current != null && d?.consumer?.umcsent?.current != null,
  [KV_KEYS.t3]:         d => { const c = countSeries(d); return c.total > 0 && c.empty <= 1; },
  [KV_KEYS.calendar]:   d => !d?.error && (d?.events?.length ?? 0) > 0,
  [KV_KEYS.yieldsHist]: d => (d?.labels?.length ?? 0) > 0,
  [KV_KEYS.liqTower]:   d => d?.fed?.h41 != null && (d?.vampire?.weeks?.length ?? 0) > 0,
  [KV_KEYS.h41Html]:    d => d?.success === true && d?.parseQuality !== 'failed',
  [KV_KEYS.newsCache]:  d => (d?.items?.length ?? 0) > 0,
  [KV_KEYS.onchainMacro]: d => (d?.stablecoin?.totalB ?? 0) > 0 || (d?.rwa?.protocols?.length ?? 0) > 0 || (d?.etf?.dailyFlows?.length ?? 0) > 0,
  [KV_KEYS.market]:     d => Object.values(d?.tickers || {}).some(v => v?.price != null),
};
const kvPut = async (env, key, data, ttl) => {
  const validate = KV_VALIDATORS[key];
  if (validate && !validate(data)) {
    console.error('[KV PUT SKIP] 검증 실패 — 기존 캐시 유지:', key);
    return data;
  }
  try {
    const payload = { ...data, _savedAt: new Date().toISOString() };
    await env.MMF_KV.put(key, JSON.stringify(payload), { expirationTtl: ttl });
    return payload;
  } catch(e) {
    console.error('[KV PUT ERROR]', key, e.message);
    return data;
  }
};

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS });
    }

    const url   = new URL(request.url);
    const path  = url.pathname;
    const force = url.searchParams.get('force') === '1';

    try {
      if (path.startsWith('/fred'))          return await fredProxy(url, env);
      if (path.startsWith('/ofr-fsi'))       return await ofrFsi(env);
      if (path.startsWith('/h41-history'))   return await h41HistoryFetcher(url, null, env);
      if (path.startsWith('/h41-html'))      return await h41HtmlParser(env, ctx, url.searchParams.get('force') === '1');
      if (path.startsWith('/dxy'))           return await dxyAnalysis();
      if (path.startsWith('/jpy'))           return await jpyCarryRisk();
      if (path.startsWith('/market'))        return await marketSnapshot(env, ctx);
      if (path.startsWith('/yahoo-chart'))   return await yahooChart(url);
      if (path.startsWith('/yahoo'))         return await yahooProxy(url);
      if (path.startsWith('/ici'))           return await iciMMF(env, ctx);
      if (path.startsWith('/pipe3'))         return await pipe3CreditRisk(env, ctx);
      // ⚠️ 더 구체적인 경로를 반드시 먼저 — /liq-tower가 /liq보다 앞에
      if (path.startsWith('/liq-tower'))     return await liqTowerCached(env, force, ctx);
      if (path.startsWith('/liq'))           return await liqCached(env, force, ctx);
      if (path.startsWith('/yields-hist'))   return await yieldsHistCached(env, force, ctx);
      if (path.startsWith('/calendar'))      return await calendarEndpoint(env, force, ctx);
      if (path.startsWith('/t2'))            return await t2Cached(env, force, ctx);
      if (path.startsWith('/t3'))            return await t3Cached(env, force, ctx);
      if (path.startsWith('/onchain-macro')) return await onchainMacroCached(env, force, ctx);
      // QRA는 읽기 전용 — 갱신은 클라우드 에이전트 → data/qra.json → GitHub Actions → KV
      if (path.startsWith('/qra-status'))    return await qraStatus(env);
      if (path.startsWith('/auction-results'))  return await auctionResults(url);

      // ── 뉴스 다이제스트 ────────────────────────────────────
      if (path.startsWith('/news-digest-send'))     return await newsDigestSend(env);
      if (path.startsWith('/news-digest'))           return await newsDigestEndpoint(env);

      // ── 뉴스 피드 ──────────────────────────────────────────
      if (path.startsWith('/news'))             return await newsEndpoint(env, force, ctx); // force=1 시 캐시 무시

      return json({ error: 'Unknown route' }, 404);
    } catch(e) {
      return json({ error: e.message }, 500);
    }
  },

  // Cron 트리거 — wrangler.toml의 CRON 문자열과 반드시 일치해야 함 (CRON 상수 참조)
  // 한 번의 실행당 서브요청 50건 한도(무료 플랜) 때문에 갱신 작업을 4개 시각으로 분산.
  // cron-job.org가 :00/:30에 GH Actions(KV 쓰기)를 트리거하므로 Worker는 그 시각을 피함.
  async scheduled(event, env, ctx) {
    const run = async (name, fn) => {
      try { await fn(); } catch(e) { console.error(`cron ${name}:`, e.message); }
    };
    if (event.cron !== CRON) console.error('cron 문자열 불일치:', event.cron, '— wrangler.toml과 CRON 상수를 맞추세요');
    const t = new Date(event.scheduledTime);
    const job = CRON_JOBS[t.getUTCMinutes()];
    if (job === 'news') {
      ctx.waitUntil(run('news', () => newsScheduledRefresh(env)));
      return;
    }
    if (job === 'newsOnchain') {
      ctx.waitUntil(Promise.all([run('news', () => newsScheduledRefresh(env)),
                                 run('onchain', () => refreshOnchain(env))]));
      return;
    }
    if (!job) return;
    if (!REFRESH_HOURS.includes(t.getUTCHours())) {   // 갱신 시각이 아니면 시세 스냅샷만
      ctx.waitUntil(run('market', () => refreshMarket(env, MARKET_TICKERS, 45)));
      return;
    }
    switch (job) {
      case 'groupA':
        ctx.waitUntil((async () => {
          await run('h41Html', () => h41HtmlParser(env, null, true));
          await Promise.all([run('liq', () => refreshLiq(env)), run('yields', () => refreshYieldsHist(env))]);
        })());
        return;
      case 'groupB':
        ctx.waitUntil((async () => {
          await run('t2', () => refreshT2(env));
          await run('market', () => refreshMarket(env, MARKET_TICKERS.slice(0, MARKET_HALF), 24));   // T2 ~21건 + 24 ≤ 50
        })());
        return;
      case 'groupC':
        ctx.waitUntil(Promise.all([run('t3', () => refreshT3(env)), run('liqTower', () => refreshLiqTower(env)),
                                   run('ici', () => iciMMF(env, null))]));   // 방문자 없어도 주간 이력 누적
        return;
      case 'groupD':
        ctx.waitUntil((async () => {
          await run('calendar', async () => kvPut(env, KV_KEYS.calendar, await fetchCalendar(env), KV_TTL.calendar));
          await run('market', () => refreshMarket(env, MARKET_TICKERS, 38));   // 캘린더 ~8건 + 38 ≤ 50
        })());
        return;
    }
  }
};

// ── KV-first 래퍼 ─────────────────────────────────────
async function liqCached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.liq, force);
  if (cached) return json(cached);
  const resp = await liqDataEndpoint(env);
  const data = await resp.json();  // clone 불필요 — 아래서 json(data)로 반환
  const putPromise = kvPut(env, KV_KEYS.liq, data, KV_TTL.liq);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(data);  // body 이미 소비됐으므로 resp 재사용 금지
}

async function yieldsHistCached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.yieldsHist, force);
  if (cached) return json(cached);
  const resp = await yieldsHistory(env);
  const data = await resp.json();
  const putPromise = kvPut(env, KV_KEYS.yieldsHist, data, KV_TTL.yieldsHist);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(data);
}

// ── Cron 갱신 함수 ────────────────────────────────────
async function calendarEndpoint(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.calendar, force);
  if (cached) return json(cached);
  const data = await fetchCalendar(env);
  const putPromise = kvPut(env, KV_KEYS.calendar, data, KV_TTL.calendar);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(data);
}

// ── FRED 이벤트 캘린더 ─────────────────────────────────
async function fetchCalendar(env) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return { error: 'FRED_API_KEY 없음', events: [] };

  // 주요 릴리즈 ID 및 메타 (high/medium만 표시)
  const RELEASES = {
    10:  { nameKo: '소비자물가 (CPI)',              imp: 'high',   tag: '인플레', series:'CPIAUCSL',        fmt:'yoy'  },
    46:  { nameKo: '생산자물가 (PPI)',              imp: 'high',   tag: '인플레', series:'PPIFIS',          fmt:'yoy'  },
    54:  { nameKo: '개인소비지출 (PCE · Core PCE)', imp: 'high',   tag: '인플레', series:'PCEPILFE',        fmt:'yoy'  },
    // 10: NFP → 매월 첫째 금요일 수학 계산 (buildMarketEvents)
    // 33: ISM 제조업 → 매월 첫 영업일 수학 계산 (buildMarketEvents)
    // 57: ISM 서비스 → 매월 셋째 영업일 수학 계산 (buildMarketEvents)
    192: { nameKo: '구인이직보고서 (JOLTS)',         imp: 'medium', tag: '고용',   series:'JTSJOL',          fmt:'val'  },
    53:  { nameKo: 'GDP 성장률',                    imp: 'high',   tag: '성장',   series:'A191RL1Q225SBEA', fmt:'val'  },
    9:   { nameKo: '소매판매',                      imp: 'medium', tag: '성장',   series:'RSAFS',           fmt:'mom'  },
    27:  { nameKo: '주택착공',                      imp: 'medium', tag: '주택',   series:'HOUST',           fmt:'val'  },
    91:  { nameKo: '소비자심리 (미시간)',            imp: 'medium', tag: '경기',   series:'UMCSENT',         fmt:'val'  },
    95:  { nameKo: '내구재 주문',                   imp: 'medium', tag: '성장',   series:'DGORDER',         fmt:'mom'  },
    187: { nameKo: '세인트루이스 FSI',               imp: 'medium', tag: '신용',   series:'STLFSI4',         fmt:'val'  },
  };

  // 발표 데이터 fetch 헬퍼 (D-DAY 이벤트용)
  const fetchLatest = async (seriesId, fmtType) => {
    if (!seriesId) return null;
    try {
      const limit = fmtType === 'yoy' ? 14 : 2;
      const u = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&limit=${limit}&sort_order=desc`;
      const r = await fetch(u, { cf: { cacheTtl: 1800 } });
      if (!r.ok) return null;
      const d = await r.json();
      const obs = (d.observations || []).filter(o => o.value !== '.');
      if (!obs.length) return null;
      const cur  = parseFloat(obs[0].value);
      const prev = obs[1] ? parseFloat(obs[1].value) : null;
      if (fmtType === 'yoy') {
        const yr = obs[12] ? parseFloat(obs[12].value) : null;
        if (yr == null || yr === 0) return null;
        const yoy = ((cur - yr) / yr * 100).toFixed(2);
        return { label: `YoY ${yoy >= 0 ? '+' : ''}${yoy}%`, val: parseFloat(yoy) };
      } else if (fmtType === 'mom') {
        if (prev == null || prev === 0) return null;
        const mom = ((cur - prev) / Math.abs(prev) * 100).toFixed(1);
        return { label: `MoM ${mom >= 0 ? '+' : ''}${mom}%`, val: parseFloat(mom) };
      } else {
        // ICSA 단위 = 건(219,000 → 21.9만), JTSJOL 단위 = 천 건(7,523 → 752.3만)
        const man = seriesId === 'ICSA' ? cur / 10000 : seriesId === 'JTSJOL' ? cur / 10 : null;
        const dispVal = man != null ? man.toFixed(1) + '만 건' : cur.toFixed(1);
        return { label: dispVal, val: cur };
      }
    } catch(e) { return null; }
  };

  // KST(UTC+9) 기준 오늘 날짜 사용
  // todayKST/past/end는 이미 +9h 적용됐으므로 fmt는 그대로 slice만
  const fmt = d => d.toISOString().slice(0, 10);
  const today = new Date();
  const todayKST = new Date(today.getTime() + 9 * 60 * 60 * 1000);
  const end = new Date(todayKST);
  end.setDate(end.getDate() + 45);

  try {
    // realtime_start=1776-07-04: FRED에 언제 등록됐든 모두 가져옴
    // 날짜 필터(과거 7일~미래 45일)는 코드에서 처리
    const past = new Date(todayKST);
    past.setDate(past.getDate() - 7);
    const pastStr = fmt(past);
    const endStr  = fmt(end);
    // realtime_start=과거7일 ~ realtime_end=미래45일: 해당 기간 발표일만 조회
    const u = `https://api.stlouisfed.org/fred/releases/dates`
      + `?api_key=${apiKey}&file_type=json`
      + `&realtime_start=${pastStr}&realtime_end=${endStr}`
      + `&sort_order=asc&limit=1000&include_release_dates_with_no_data=true`;
    const r = await fetch(u, { cf: { cacheTtl: 3600 } });
    if (!r.ok) return { error: `FRED ${r.status}`, events: [] };
    const d = await r.json();

    const todayStr = fmt(todayKST);
    // 날짜 문자열 기반 diff: 타임스탬프 비교 대신 날짜 문자열로 통일
    const dateDiff = (dateStr) => {
      const e = new Date(dateStr + 'T12:00:00Z');
      const t = new Date(todayStr  + 'T12:00:00Z');
      return Math.round((e - t) / 86400000);
    };
    const toDday = (diff) => diff === 0 ? 'D-DAY' : diff > 0 ? `D-${diff}` : `D+${Math.abs(diff)}`;

    const events = (d.release_dates || [])
      .filter(e => RELEASES[e.release_id])
      .map(e => {
        const meta = RELEASES[e.release_id];
        const diff = dateDiff(e.date);
        return {
          date:      e.date,
          dday:      toDday(diff),
          name:      meta.nameKo,
          imp:       meta.imp,
          tag:       meta.tag,
          series:    meta.series   ?? null,
          seriesFmt: meta.fmt      ?? null,
          released:  null,  // D-DAY 시 채워짐
        };
      })
      // 같은 날 같은 릴리즈 중복 제거
      .filter((e, i, arr) =>
        arr.findIndex(x => x.date === e.date && x.name === e.name) === i
      )
      .sort((a, b) => a.date.localeCompare(b.date));

    // ── 파생상품/재무부/기타 이벤트 병합 ──────────────────────────
    const toStr = fmt(end);
    const fomcDates      = fetchFomcDates();
    const marketEvents   = buildMarketEvents(todayStr, toStr, fomcDates);
    const blackoutEvents = buildFomcBlackout(todayStr, toStr, fomcDates);

    // D-day 계산 적용 (전체 병합 후) — 날짜 문자열 기반으로 통일
    const allEvents = [...events, ...marketEvents, ...blackoutEvents].map(e => {
      if (e.dday !== null && e.dday !== undefined) return e;
      return { ...e, dday: toDday(dateDiff(e.date)) };
    });

    // 날짜순 정렬
    allEvents.sort((a, b) => a.date.localeCompare(b.date));

    // D-DAY 이벤트 수치 fetch (FRED + buildMarketEvents 모두 포함)
    const todayEvents = allEvents.filter(e => e.date === todayStr && e.seriesFmt);
    if (todayEvents.length > 0) {
      const results = await Promise.all(
        todayEvents.map(e => fetchLatest(e.series, e.seriesFmt))
      );
      todayEvents.forEach((e, i) => { e.released = results[i]; });
    }

    return { events: allEvents, fetchedAt: fmt(todayKST), _savedAt: new Date().toISOString() };
  } catch(e) {
    return { error: e.message, events: [] };
  }
}

// ── 캘린더 보조 함수 ────────────────────────────────────────────

// 연준 공식 FOMC 캘린더 파싱
// federalreserve.gov/monetarypolicy/fomccalendars.htm
// 형태: "Jan. 28-29" 또는 "Apr. 29*" (단일) 등
// FOMC 실제 회의 날짜 (성명서 발표일 = 두 번째 날 기준)
// 연준 공식 일정: federalreserve.gov/monetarypolicy/fomccalendars.htm
// ※ 연준이 다음 해 일정을 발표하면(보통 여름) 추가. 헬스체크 에이전트가 소진 30일 전 경고.
function fetchFomcDates() {
  return [
    // 2025
    '2025-01-29','2025-03-19','2025-05-07','2025-06-18',
    '2025-07-30','2025-09-17','2025-10-29','2025-12-10',
    // 2026
    '2026-01-28','2026-03-18','2026-04-29','2026-06-17',
    '2026-07-29','2026-09-16','2026-10-28','2026-12-09',
    // 2027 (공식 일정: 1/26-27, 3/16-17, 4/27-28, 6/8-9, 7/27-28, 9/14-15, 10/26-27, 11/16-17)
    '2027-01-27','2027-03-17','2027-04-28','2027-06-09',
    '2027-07-28','2027-09-15','2027-10-27','2027-11-17',
  ];
}
function usMarketHolidays(year) {
  // 고정 공휴일
  const fixed = [
    `${year}-01-01`, // 신년
    `${year}-06-19`, // 준틴스
    `${year}-07-04`, // 독립기념일
    `${year}-12-25`, // 크리스마스
  ];
  // 변동 공휴일 (N번째 요일)
  const nthWeekday = (y, m, weekday, n) => {
    const d = new Date(y, m - 1, 1);
    let count = 0;
    while (true) {
      if (d.getDay() === weekday) { count++; if (count === n) break; }
      d.setDate(d.getDate() + 1);
    }
    return d.toISOString().slice(0, 10);
  };
  const variable = [
    nthWeekday(year, 1, 1, 3),  // MLK Day: 1월 셋째 월
    nthWeekday(year, 2, 1, 3),  // Presidents Day: 2월 셋째 월
    (() => { const d = new Date(Date.UTC(year, 5, 0)); while (d.getUTCDay() !== 1) d.setUTCDate(d.getUTCDate() - 1); return d.toISOString().slice(0, 10); })(),  // Memorial Day: 5월 마지막 월
    nthWeekday(year, 9, 1, 1),  // Labor Day: 9월 첫째 월
    nthWeekday(year, 11, 4, 4), // Thanksgiving: 11월 넷째 목
  ];
  // 주말 보정 (토→금, 일→월)
  return [...fixed, ...variable].map(dateStr => {
    const d = new Date(dateStr);
    if (d.getDay() === 6) { d.setDate(d.getDate() - 1); }
    if (d.getDay() === 0) { d.setDate(d.getDate() + 1); }
    return d.toISOString().slice(0, 10);
  });
}

// N번째 특정 요일 계산 (1=월 ... 5=금, 0=일)
function nthWeekdayOfMonth(year, month, weekday, n) {
  const d = new Date(year, month - 1, 1);
  let count = 0;
  while (true) {
    if (d.getDay() === weekday) { count++; if (count === n) break; }
    d.setDate(d.getDate() + 1);
  }
  return d.toISOString().slice(0, 10);
}

// 마지막 특정 요일 계산
function lastWeekdayOfMonth(year, month, weekday) {
  const d = new Date(year, month, 0); // 해당 월 마지막 날
  while (d.getDay() !== weekday) d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

// 다음 영업일 (공휴일/주말 건너뜀)
function nextBusinessDay(dateStr, holidays) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + 1);
  while (d.getDay() === 0 || d.getDay() === 6 || holidays.includes(d.toISOString().slice(0,10))) {
    d.setDate(d.getDate() + 1);
  }
  return d.toISOString().slice(0, 10);
}

// 파생상품 + 재무부 + 기타 이벤트 생성 (향후 90일)
function buildMarketEvents(fromDate, toDate, fomcDates = []) {
  const events = [];
  const from = new Date(fromDate);
  const to   = new Date(toDate);

  // 대상 연월 범위 계산
  const months = [];
  const cur = new Date(from.getFullYear(), from.getMonth(), 1);
  while (cur <= to) {
    months.push({ year: cur.getFullYear(), month: cur.getMonth() + 1 });
    cur.setMonth(cur.getMonth() + 1);
  }

  for (const { year, month } of months) {
    const holidays = usMarketHolidays(year);

    // ① OpEx — 매월 셋째 주 금요일
    let opexDate = nthWeekdayOfMonth(year, month, 5, 3);
    // 공휴일이면 전일(목요일)로 당김
    if (holidays.includes(opexDate)) {
      const d = new Date(opexDate);
      d.setDate(d.getDate() - 1);
      opexDate = d.toISOString().slice(0, 10);
    }

    // ② 네 마녀의 날 — 3/6/9/12월 셋째 주 금요일
    const isQW = [3, 6, 9, 12].includes(month);

    if (opexDate >= fromDate && opexDate <= toDate) {
      if (isQW) {
        events.push({
          date:      opexDate,
          dday:      null,
          name:      '네 마녀의 날 (파생상품 동시 만기)',
          imp:       'high',
          tag:       '파생/청산',
          category:  'derivatives',
          weight:    3,
          estimated: false,
        });
      } else {
        events.push({
          date:      opexDate,
          dday:      null,
          name:      '옵션 만기일 (OpEx)',
          imp:       'medium',
          tag:       '파생/청산',
          category:  'derivatives',
          weight:    2,
          estimated: false,
        });
      }
    }

    // ③ 세금 납부일 — 4월 15일 (주말/공휴일 시 다음 영업일)
    if (month === 4) {
      let taxDate = `${year}-04-15`;
      const td = new Date(taxDate);
      if (td.getDay() === 6) taxDate = `${year}-04-17`; // 토→월
      if (td.getDay() === 0) taxDate = `${year}-04-16`; // 일→월
      if (holidays.includes(taxDate)) taxDate = nextBusinessDay(taxDate, holidays);
      if (taxDate >= fromDate && taxDate <= toDate) {
        events.push({
          date:      taxDate,
          dday:      null,
          name:      '미 세금 납부일 (TGA 흡수 최대)',
          imp:       'high',
          tag:       '재무부',
          category:  'treasury',
          weight:    3,
          estimated: false,
        });
      }
    }

    // ④ QRA 추정일 — 1/4/7/10월 마지막 주 수요일 (±3일 불확실)
    if ([1, 4, 7, 10].includes(month)) {
      const qraDate = lastWeekdayOfMonth(year, month, 3); // 3=수요일
      if (qraDate >= fromDate && qraDate <= toDate) {
        events.push({
          date:      qraDate,
          dday:      null,
          name:      'QRA 발표 추정일 (분기 자금조달 계획)',
          imp:       'high',
          tag:       '재무부',
          category:  'treasury',
          weight:    3,
          estimated: true, // ±3일 불확실
        });
      }
    }

    // ⑤ 월말 리밸런싱 구간 — 매월 마지막 영업일 기준 -2일 ~ +1일
    const lastDay = new Date(year, month, 0);
    while (lastDay.getDay() === 0 || lastDay.getDay() === 6 || usMarketHolidays(year).includes(lastDay.toISOString().slice(0,10))) {
      lastDay.setDate(lastDay.getDate() - 1);
    }
    const rebalDate = lastDay.toISOString().slice(0, 10);
    if (rebalDate >= fromDate && rebalDate <= toDate) {
      events.push({
        date:      rebalDate,
        dday:      null,
        name:      '월말 기관 리밸런싱 구간',
        imp:       'medium',
        tag:       '수급',
        category:  'rebalancing',
        weight:    1,
        estimated: true,
      });
    }
  }

  // ⑦ H.4.1 연준 자산 주간 발표 — 매주 목요일 (4:30pm ET)
  // ⑧ 시카고 NFCI — 매주 수요일 8:30am ET (공휴일 시 목요일)
  const cursor = new Date(fromDate);
  const holidays = usMarketHolidays(new Date(fromDate).getFullYear());
  while (cursor.toISOString().slice(0, 10) <= toDate) {
    const dow = cursor.getDay();
    const dateStr = cursor.toISOString().slice(0, 10);

    if (dow === 4) { // 목요일 — H.4.1 + 실업수당
      events.push({
        date: dateStr, dday: null,
        name: 'H.4.1 연준 자산 주간 발표 (WALCL·RRP·TGA)',
        imp: 'low', tag: '연준', category: 'fed', weight: 1, estimated: false,
      });
      events.push({
        date: dateStr, dday: null,
        name: '실업수당 청구 (신규·연속) 주간 발표',
        imp: 'medium', tag: '고용', category: 'macro', weight: 1, estimated: false,
      });
    }
    if (dow === 3) { // 수요일 — NFCI (공휴일이면 목요일로 자동 밀림)
      let nfciDate = dateStr;
      if (holidays.includes(nfciDate)) {
        const next = new Date(cursor);
        next.setDate(next.getDate() + 1);
        nfciDate = next.toISOString().slice(0, 10);
      }
      events.push({
        date: nfciDate, dday: null,
        name: '시카고 NFCI (금융여건지수) 주간 발표',
        imp: 'low', tag: '신용', category: 'macro', weight: 1, estimated: false,
      });
    }
    cursor.setDate(cursor.getDate() + 1);
  }

  // ⑩ NFP + 실업률 — 매월 첫째 금요일
  // ⑪ ISM 제조업 — 매월 첫 영업일
  // ⑫ ISM 서비스 — 매월 셋째 영업일
  for (const { year, month } of months) {
    const holidays = usMarketHolidays(year);

    // 첫째 금요일 계산
    const nfpDate = nthWeekdayOfMonth(year, month, 5, 1);
    if (nfpDate >= fromDate && nfpDate <= toDate) {
      events.push({
        date: nfpDate, dday: null,
        name: '고용보고서 (NFP · 실업률)',
        imp: 'high', tag: '고용', category: 'macro', weight: 3,
        estimated: false, series: 'PAYEMS', seriesFmt: 'mom',
      });
      // 실업률은 같은 날 별도 표시
      events.push({
        date: nfpDate, dday: null,
        name: '실업률 (UNRATE)',
        imp: 'high', tag: '고용', category: 'macro', weight: 3,
        estimated: false, series: 'UNRATE', seriesFmt: 'val',
      });
    }

    // 첫 영업일 계산 (주말/공휴일 건너뜀)
    const d1 = new Date(year, month - 1, 1);
    while (d1.getDay() === 0 || d1.getDay() === 6 || holidays.includes(d1.toISOString().slice(0,10))) {
      d1.setDate(d1.getDate() + 1);
    }
    const ismMfgDate = d1.toISOString().slice(0, 10);
    if (ismMfgDate >= fromDate && ismMfgDate <= toDate) {
      events.push({
        date: ismMfgDate, dday: null,
        name: 'ISM 제조업 PMI',
        imp: 'medium', tag: '경기', category: 'macro', weight: 2,
        estimated: false, series: null, seriesFmt: null,
      });
    }

    // 셋째 영업일 계산
    const d3 = new Date(year, month - 1, 1);
    let bizCount = 0;
    while (bizCount < 3) {
      if (d3.getDay() !== 0 && d3.getDay() !== 6 && !holidays.includes(d3.toISOString().slice(0,10))) {
        bizCount++;
      }
      if (bizCount < 3) d3.setDate(d3.getDate() + 1);
    }
    const ismSvcDate = d3.toISOString().slice(0, 10);
    if (ismSvcDate >= fromDate && ismSvcDate <= toDate) {
      events.push({
        date: ismSvcDate, dday: null,
        name: 'ISM 서비스업 PMI',
        imp: 'medium', tag: '경기', category: 'macro', weight: 2,
        estimated: false, series: null, seriesFmt: null,
      });
    }
  }
  for (const fomcDate of fomcDates) {
    const fm = new Date(fomcDate);
    const fomcMonth = fm.getMonth() + 1;
    // FOMC 이후 월요일 = SLOOS 발표일 (1/4/7/10월 FOMC만 해당)
    if (![1, 4, 7, 10].includes(fomcMonth)) continue;
    const nextMonday = new Date(fm);
    nextMonday.setDate(fm.getDate() + 1);
    while (nextMonday.getDay() !== 1) nextMonday.setDate(nextMonday.getDate() + 1);
    const sloosDate = nextMonday.toISOString().slice(0, 10);
    if (sloosDate >= fromDate && sloosDate <= toDate) {
      events.push({
        date: sloosDate, dday: null,
        name: 'SLOOS 대출기준 설문 발표 (FOMC 후 월요일)',
        imp: 'high', tag: '신용', category: 'macro', weight: 2, estimated: false,
      });
    }
  }

  // ⑥ FOMC 실제 회의일 추가
  for (const dateStr of fomcDates) {
    if (dateStr >= fromDate && dateStr <= toDate) {
      events.push({
        date:      dateStr,
        dday:      null,
        name:      'FOMC 회의 (금리 결정)',
        imp:       'high',
        tag:       '연준',
        category:  'fed',
        weight:    3,
        estimated: false,
      });
    }
    // 의사록: 회의 3주 후 공개 (연준 관행)
    const minutes = new Date(Date.parse(dateStr) + 21 * 86400000).toISOString().slice(0, 10);
    if (minutes >= fromDate && minutes <= toDate) {
      events.push({
        date: minutes, dday: null, name: `FOMC 의사록 공개 (${dateStr.slice(5)} 회의)`,
        imp: 'high', tag: '연준', category: 'fed', weight: 2, estimated: false,
      });
    }
  }

  return events;
}

// FOMC 블랙아웃 기간 생성 (FOMC 회의일 기준 -10 캘린더일 ~ 회의 당일)
function buildFomcBlackout(fromDate, toDate, fomcDates = []) {
  const blackouts = [];

  for (const fomcDateStr of fomcDates) {
    const fomcDate = new Date(fomcDateStr);

    // 블랙아웃 시작: 회의일 -10 캘린더일
    const start = new Date(fomcDate);
    start.setDate(start.getDate() - 10);
    const startStr = start.toISOString().slice(0, 10);

    // 범위 내 블랙아웃만 추가
    if (fomcDateStr >= fromDate && startStr <= toDate) {
      blackouts.push({
        date:      startStr,
        endDate:   fomcDateStr,
        dday:      null,
        name:      `FOMC 블랙아웃 기간 (~${fomcDateStr.slice(5)} 회의)`,
        imp:       'low',
        tag:       '연준',
        category:  'fed_blackout',
        weight:    1,
        estimated: false,
        isRange:   true,
      });
    }
  }
  return blackouts;
}

async function t2Cached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.t2, force);
  if (cached) return json(cached);
  const resp = await t2DataEndpoint(env, force);
  const data = await resp.json();
  const putPromise = kvPut(env, KV_KEYS.t2, data, KV_TTL.t2);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(data);
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 실물경제 탭 (t2) — 3개 섹션 데이터
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function t2DataEndpoint(env, force = false) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return json({ error: 'FRED_API_KEY 없음' }, 500);

  // FRED 단일 시리즈 fetch 헬퍼 (최근 N개, desc)
  const fredArr = async (id, limit = 2) => {
    try {
      const u = `https://api.stlouisfed.org/fred/series/observations`
        + `?series_id=${id}&api_key=${apiKey}&file_type=json`
        + `&limit=${limit}&sort_order=desc`;
      const r = await fetch(u, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return [];
      const d = await r.json();
      return (d.observations || [])
        .filter(o => o.value !== '.')
        .map(o => ({ date: o.date, value: parseFloat(o.value), released: o.realtime_start ?? null }));
    } catch(e) { return []; }
  };

  // Atlanta Fed GDPNow 파싱 (HTML 스크래핑 → FRED fallback)
  const gdpNowFetch = async (force = false) => {
    // RSS는 최신 항목 누락 문제로 제거, FRED vintage 방식만 사용
    try {
      const fredUrl = `https://api.stlouisfed.org/fred/series/observations`
        + `?series_id=GDPNOW&api_key=${apiKey}&file_type=json`
        + `&realtime_start=1776-07-04&realtime_end=9999-12-31`
        + `&sort_order=desc&limit=20`
        + `&_cb=${Date.now()}`;
      const r = await fetch(fredUrl, {
        cf: { cacheKey: `gdpnow-${Date.now()}`, cacheEverything: false },
      });
      if (!r.ok) throw new Error(`FRED HTTP ${r.status}`);
      const d = await r.json();
      const obs = (d.observations || []).filter(o => o.value !== '.');
      if (!obs.length) throw new Error('no obs');

      // realtime_start 내림차순 → obs[0] = 가장 최근 수정본
      obs.sort((a, b) => (b.realtime_start || '').localeCompare(a.realtime_start || ''));
      const current  = parseFloat(obs[0].value);
      const qtrDate  = obs[0].date;
      const asOf     = obs[0].realtime_start?.slice(0, 10) ?? null;


      return {
        current, prevEst: null, delta: null, asOf, qtrDate,
        components: null, qualityWarning: false, warningReason: null,
        source: 'fred',
      };
    } catch(e) {
      console.error('GDPNow FRED failed:', e.message);
      return null;
    }
  };

  // 병렬 fetch — 전체 시리즈
  const [
    gdpNow,
    mfgNY, mfgPhi, mfgDal, mfgRich,          // Section 1: 제조업 4개
    svcDal, svcNY, svcRich,                    // Section 1: 서비스 3개
    jolts, tempHelp, icsa, ic4wsa, payems, unrate, sahm,  // Section 2 (5-Stage Pipeline)
    umcsent, psavert, delinq, rsafs, rsxfs, cpiT2,  // Section 3
  ] = await Promise.all([
    gdpNowFetch(force),
    // ── 지역 연준 제조업 4개 (25개월 차트용)
    fredArr('GACDISA066MSFRBNY',  25),  // NY 엠파이어 스테이트
    fredArr('GACDFSA066MSFRBPHI', 25),  // 필라델피아 Fed
    fredArr('BACTSAMFRBDAL',      25),  // Dallas Fed
    fredArr('MFGBAPIRCHM',        25),  // Richmond Fed — fallback 허용
    // ── 지역 연준 서비스업 3개 (확정 FRED ID)
    fredArr('TSSOSBACTUAMFRBDAL', 25),  // Dallas TSSOS (Texas 서비스)
    fredArr('BACDINA066MNFRBNY',  25),  // NY 서비스 (Business Leaders Survey)
    fredArr('GABNDIF066MNFRBPHI', 25),  // Philly 비제조업 (Richmond 미제공, Philly로 대체)
    fredArr('JTSJOL',        14),  // YoY(12개월) + 스파크라인 + 3개월 추세
    fredArr('TEMPHELPS',     8),   // consecNeg(4개 필요) + 스파크라인
    fredArr('ICSA',          5),   // 서킷브레이커용 최신 주간 raw값
    fredArr('IC4WSA',       56),   // 4주 이동평균: YoY(52주) + 스파크라인(24주)
    fredArr('PAYEMS',        6),   // 3MMA(4개 필요) + 스파크라인
    fredArr('UNRATE',        6),   // 스파크라인
    fredArr('SAHMREALTIME',  6),   // 스파크라인
    fredArr('UMCSENT',      38),   // YoY(12M) + 3MMA + 3년 스파크라인(36M)
    fredArr('PSAVERT',      62),   // 3MMA + delta + 5년 시계열(60M)
    fredArr('DRCCLACBS',    22),   // YoY(4분기) + 5년 시계열(20Q)
    fredArr('RSAFS',        14),   // 명목 헤드라인 참고값
    fredArr('RSXFS',        15),   // 통제그룹 근사: 자동차 제외 소매 MoM 계산용
    fredArr('CPIAUCSL',     15),   // CPI: 실질화 연산용
  ]);

  // 요약 헬퍼
  const s = arr => ({
    current: arr[0]?.value ?? null,
    prev:    arr[1]?.value ?? null,
    delta:   (arr[0]?.value != null && arr[1]?.value != null)
             ? +(arr[0].value - arr[1].value).toFixed(3) : null,
    asOf:    arr[0]?.date ?? null,
  });

  // ── 지역 연준 복합 계산 헬퍼 ──
  const regionalComposite = (arrays) => {
    // 날짜 기준 정렬 후 최근 24개월 히스토리 생성
    // 각 시리즈의 가장 최근 날짜 기준으로 교차점 찾기
    const validArrays = arrays.filter(a => a.length > 0);
    if (!validArrays.length) return { current: null, prev: null, asOf: null, series: [], hist: [] };

    // 최신값 (각 시리즈의 [0])
    const vals = validArrays.map(a => a[0]?.value ?? null).filter(v => v != null);
    const current = vals.length ? +(vals.reduce((s,v)=>s+v,0)/vals.length).toFixed(1) : null;
    const prevVals = validArrays.map(a => a[1]?.value ?? null).filter(v => v != null);
    const prev = prevVals.length ? +(prevVals.reduce((s,v)=>s+v,0)/prevVals.length).toFixed(1) : null;

    // 히스토리: NY 기준 날짜로 정렬 (가장 긴 시리즈)
    const anchor = validArrays.reduce((a,b) => a.length >= b.length ? a : b);
    const hist = anchor.slice(0, 24).reverse().map(entry => {
      const date = entry.date;
      const pointVals = validArrays.map(a => {
        const match = a.find(d => d.date === date);
        return match?.value ?? null;
      }).filter(v => v != null);
      return {
        date,
        composite: pointVals.length ? +(pointVals.reduce((s,v)=>s+v,0)/pointVals.length).toFixed(1) : null,
      };
    });

    // 개별 시리즈 최근값 (라인 차트용)
    const seriesHist = validArrays.map((arr, i) => ({
      data: anchor.slice(0, 24).reverse().map(entry => {
        const match = arr.find(d => d.date === entry.date);
        return match?.value ?? null;
      }),
    }));

    return { current, prev, delta: current != null && prev != null ? +(current-prev).toFixed(1) : null,
             asOf: anchor[0]?.date ?? null, hist, seriesHist, count: validArrays.length };
  };

  const mfgComposite = regionalComposite([mfgNY, mfgPhi, mfgDal, mfgRich]);
  const svcComposite = regionalComposite([svcDal, svcNY, svcRich]);

  // 신호등 로직 (0 기준)
  const mfgSignal = (() => {
    const vals = [mfgNY[0]?.value, mfgPhi[0]?.value, mfgDal[0]?.value, mfgRich[0]?.value]
      .filter(v => v != null);
    if (!vals.length) return { level: 'unknown', label: '—' };
    const below0 = vals.filter(v => v < 0).length;
    if (below0 === vals.length) return { level: 'red',    label: '🔴 전면 수축 — ISM 하락·고용 축소 선행' };
    if (below0 >= vals.length/2) return { level: 'yellow', label: '🟡 혼조 — 지역별 편차. 추가 관망' };
    return                              { level: 'green',  label: '🟢 전국적 확장 국면 회복' };
  })();

  const svcSignal = (() => {
    const vals = [svcDal[0]?.value, svcNY[0]?.value, svcRich[0]?.value]
      .filter(v => v != null);
    if (!vals.length) return { level: 'unknown', label: '—' };
    const below0 = vals.filter(v => v < 0).length;
    if (below0 === vals.length) return { level: 'red',    label: '🔴 서비스 전면 수축 — 소비 둔화 위험' };
    if (below0 >= vals.length/2) return { level: 'yellow', label: '🟡 서비스 혼조 — 지역별 편차 지속' };
    return                              { level: 'green',  label: '🟢 서비스 확장 유지' };
  })();

  // ── Section 2: Labor Pipeline 계산 ────────────────────────
  const joltsYoY = (jolts[0]?.value != null && jolts[12]?.value != null && jolts[12].value > 0)
    ? +((jolts[0].value - jolts[12].value) / jolts[12].value * 100).toFixed(1) : null;
  const joltsDecline3m = jolts.length >= 3 &&
    jolts[0].value < jolts[1].value && jolts[1].value < jolts[2].value;
  const joltsSeries = jolts.slice(0, 6).reverse()
    .map(d => ({ date: d.date.slice(0,7), value: +(d.value/1000).toFixed(3) }));

  const tempConsecNeg = tempHelp.length >= 4 &&
    (tempHelp[0].value - tempHelp[1].value) < 0 &&
    (tempHelp[1].value - tempHelp[2].value) < 0 &&
    (tempHelp[2].value - tempHelp[3].value) < 0;
  const tempDelta = (tempHelp[0]?.value != null && tempHelp[1]?.value != null)
    ? +(tempHelp[0].value - tempHelp[1].value).toFixed(1) : null;
  const tempSeries = tempHelp.slice(0, 6).reverse()
    .map(d => ({ date: d.date.slice(0,7), value: d.value }));

  const ic4wsaCur    = ic4wsa[0]?.value ?? null;
  const ic4wsaYearAgo = ic4wsa.length >= 53 ? (ic4wsa[52]?.value ?? null) : null;   // 52주 전
  const ic4wsaYoy    = (ic4wsaCur != null && ic4wsaYearAgo != null && ic4wsaYearAgo > 0)
    ? +((ic4wsaCur - ic4wsaYearAgo) / ic4wsaYearAgo * 100).toFixed(1) : null;
  const icsaLatest   = icsa[0]?.value ?? null;
  const ic4wsaCB     = (icsaLatest != null && ic4wsaCur != null && ic4wsaCur > 0) &&
    ((icsaLatest - ic4wsaCur) / ic4wsaCur >= 0.15);
  const ic4wsaCBPct  = (ic4wsaCB && ic4wsaCur)
    ? +(((icsaLatest - ic4wsaCur) / ic4wsaCur) * 100).toFixed(0) : null;
  const ic4wsaSeries = ic4wsa.slice(0, 24).reverse()
    .map(d => ({ date: d.date.slice(5), value: d.value }));

  const nfpLatest = (payems[0]?.value != null && payems[1]?.value != null)
    ? Math.round(payems[0].value - payems[1].value) : null;
  const nfpDeltas = [];
  for (let i = 0; i < Math.min(payems.length - 1, 5); i++) {
    if (payems[i]?.value != null && payems[i+1]?.value != null)
      nfpDeltas.push(Math.round(payems[i].value - payems[i+1].value));
  }
  const nfpMma3 = nfpDeltas.length >= 3
    ? Math.round((nfpDeltas[0] + nfpDeltas[1] + nfpDeltas[2]) / 3) : null;
  const nfpCB   = nfpLatest != null && nfpLatest < 0;
  const nfpSeries = nfpDeltas.slice().reverse().map((v, i) => ({
    date: payems[nfpDeltas.length - 1 - i]?.date?.slice(0,7) ?? '',   // reverse 후 i번째 = nfpDeltas[n-1-i] = payems[n-1-i]의 달
    value: v,
  }));

  const sahmCur      = sahm[0]?.value ?? null;
  const sahmTriggered = sahmCur != null && sahmCur >= 0.50;
  const sahmSeries   = sahm.slice(0, 6).reverse()
    .map(d => ({ date: d.date.slice(0,7), value: d.value }));
  const unrateDelta  = (unrate[0]?.value != null && unrate[1]?.value != null)
    ? +(unrate[0].value - unrate[1].value).toFixed(2) : null;
  const unrateSeries = unrate.slice(0, 6).reverse()
    .map(d => ({ date: d.date.slice(0,7), value: d.value }));
  const tempTrend = tempHelp.length >= 3
    ? (tempHelp[0].value < tempHelp[1].value && tempHelp[1].value < tempHelp[2].value
       ? 'declining' : tempHelp[0].value > tempHelp[1].value ? 'rising' : 'flat')
    : null;

  // ── Section 3: Consumer Health 계산 ──────────────────────
  const mkSentiment = (arr) => {
    if (!arr.length) return { current:null, prev:null, mma3:null, yoy:null, decline3m:false, sig:'green', asOf:null };
    const cur  = arr[0]?.value ?? null;
    const prev = arr[1]?.value ?? null;
    const yoy  = (cur != null && arr[12]?.value != null && arr[12].value > 0)
      ? +((cur - arr[12].value) / arr[12].value * 100).toFixed(1) : null;
    const mma3 = (arr[0]?.value != null && arr[1]?.value != null && arr[2]?.value != null)
      ? +((arr[0].value + arr[1].value + arr[2].value) / 3).toFixed(1) : null;
    const decline3m = arr.length >= 4 &&
      arr[0].value < arr[1].value && arr[1].value < arr[2].value && arr[2].value < arr[3].value;
    const sig = yoy != null && yoy < -10 ? 'red'
      : (decline3m || (yoy != null && yoy < 0)) ? 'yellow' : 'green';
    // 스파크라인용 시계열 (최대 36개월, 고→최신 순서)
    const series = arr.slice(0, 36).reverse()
      .map(d => ({ date: d.date.slice(0,7), value: d.value }));
    // 12개월 평균 (기준 점선용)
    const recent12 = arr.slice(0, 12).filter(d => d.value != null);
    const avg12 = recent12.length
      ? +(recent12.reduce((s,d) => s+d.value, 0) / recent12.length).toFixed(1) : null;
    return { current: cur, prev, mma3, yoy, decline3m, sig,
      asOf: arr[0]?.date?.slice(0,7) ?? null,                     // 데이터 실제 월 (예: 2026-02)
      releasedAt: arr[0]?.released ?? null,                       // FRED 발표/revision 일자 (예: 2026-04-15)
      series, avg12 };
  };
  const umcData  = mkSentiment(umcsent);

  const psaCur   = psavert[0]?.value ?? null;
  const psaPrev  = psavert[1]?.value ?? null;
  const psaDelta = (psaCur != null && psaPrev != null) ? +(psaCur - psaPrev).toFixed(2) : null;
  const psaMma3  = (psavert[0]?.value != null && psavert[1]?.value != null && psavert[2]?.value != null)
    ? +((psavert[0].value + psavert[1].value + psavert[2].value) / 3).toFixed(1) : null;
  const psaSig   = psaCur == null ? 'green' : psaCur < 4.0 ? 'red' : psaCur < 5.0 ? 'yellow' : 'green';

  const delCur      = delinq[0]?.value ?? null;
  const delPrev     = delinq[1]?.value ?? null;
  const delYoyDelta = (delCur != null && delinq[4]?.value != null)
    ? +(delCur - delinq[4].value).toFixed(2) : null;
  const delSig      = delCur == null ? 'green' : delCur > 3.0 ? 'red' : delCur > 2.5 ? 'yellow' : 'green';

  const rsxfsMom = [];
  for (let i = 0; i < Math.min(rsxfs.length - 1, 13); i++) {
    const p = rsxfs[i+1]?.value, c = rsxfs[i]?.value;
    if (c != null && p != null && p > 0)
      rsxfsMom.push({ date: rsxfs[i].date, value: +((c - p) / p * 100).toFixed(2) });
    else break;
  }
  const cpiMomArr = [];
  for (let i = 0; i < Math.min(cpiT2.length - 1, 13); i++) {
    const p = cpiT2[i+1]?.value, c = cpiT2[i]?.value;
    if (c != null && p != null && p > 0)
      cpiMomArr.push({ date: cpiT2[i].date, value: +((c - p) / p * 100).toFixed(2) });
    else break;
  }
  // Real Core MoM: 같은 달끼리 매칭 (CPI가 소매판매보다 며칠 먼저 나와 인덱스로 맞추면 한 달 어긋남)
  const cpiMomByMonth = Object.fromEntries(cpiMomArr.map(c => [c.date.slice(0, 7), c.value]));
  const realCoreMomArr = rsxfsMom
    .filter(r => cpiMomByMonth[r.date.slice(0, 7)] != null)
    .map(r => ({ date: r.date, value: +(r.value - cpiMomByMonth[r.date.slice(0, 7)]).toFixed(2) }));
  const realCoreCur  = realCoreMomArr[0]?.value ?? null;
  const realCore3MMA = realCoreMomArr.length >= 3
    ? +((realCoreMomArr[0].value + realCoreMomArr[1].value + realCoreMomArr[2].value) / 3).toFixed(2) : null;
  const rsafsHist = rsafs.slice(0, 13).reverse().map((d, i, arr) => {
    if (i === 0) return { date: d.date, mom: null };
    const mom = arr[i-1].value > 0
      ? +((d.value - arr[i-1].value) / arr[i-1].value * 100).toFixed(2) : null;
    return { date: d.date, mom };
  }).filter(d => d.mom !== null);
  const rsafsLatestMom = rsafsHist.length ? rsafsHist[rsafsHist.length-1].mom : null;
  const retailSig = realCoreCur == null ? 'green'
    : realCoreCur >= 0 ? 'green'
    : (realCore3MMA != null && realCore3MMA >= 0) ? 'yellow' : 'red';
  // 스파크라인용 시계열 (12개월, 고→최신 순서)
  const realRetailSeries = realCoreMomArr.slice(0, 12).reverse();
  // 3MMA 시계열 (같은 날짜 기준, null 허용)
  const realRetailMma3Series = realRetailSeries.map((d, i, arr) => ({
    date: d.date,
    value: i >= 2
      ? +((arr[i].value + arr[i-1].value + arr[i-2].value) / 3).toFixed(2)
      : null,
  }));
  const sentGreen = umcData.sig === 'green';
  const divCB     = sentGreen && psaSig === 'red' && delSig === 'red';

  return json({
    _savedAt: new Date().toISOString(),
    // Section 1: Corporate Pulse
    corporate: {
      gdpNow,
      mfg: { ...mfgComposite, signal: mfgSignal,
             labels: ['NY 엠파이어', '필라델피아', 'Dallas', 'Richmond'] },
      svc: { ...svcComposite, signal: svcSignal,
             labels: ['Dallas 서비스', 'NY 서비스', 'Philly 비제조업'] },
    },
    // Section 2: Labor Pipeline (5-Stage)
    // asOf = 데이터 실제 월 / releasedAt = FRED 발표·revision 일자
    labor: {
      jolts: {
        current:    jolts[0]?.value ?? null,
        asOf:       jolts[0]?.date?.slice(0,7) ?? null,
        releasedAt: jolts[0]?.released ?? null,
        yoy:        joltsYoY, decline3m: joltsDecline3m, series: joltsSeries,
      },
      tempHelp: {
        current:    tempHelp[0]?.value ?? null, prev: tempHelp[1]?.value ?? null,
        delta:      tempDelta,
        asOf:       tempHelp[0]?.date?.slice(0,7) ?? null,
        releasedAt: tempHelp[0]?.released ?? null,
        consecNeg:  tempConsecNeg, series: tempSeries,
      },
      ic4wsa: {
        current:        ic4wsaCur, icsaLatest,
        asOf:           icsa[0]?.date?.slice(0,10) ?? null,        // weekly이므로 day까지
        releasedAt:     icsa[0]?.released ?? null,
        yoy:            ic4wsaYoy, circuitBreaker: ic4wsaCB, cbPct: ic4wsaCBPct, series: ic4wsaSeries,
      },
      payems: {
        latest: nfpLatest, mma3: nfpMma3,
        asOf:       payems[0]?.date?.slice(0,7) ?? null,
        releasedAt: payems[0]?.released ?? null,
        circuitBreaker: nfpCB, series: nfpSeries,
      },
      sahm: {
        current:    sahmCur,
        asOf:       sahm[0]?.date?.slice(0,7) ?? null,
        releasedAt: sahm[0]?.released ?? null,
        triggered: sahmTriggered, series: sahmSeries,
      },
      unrate: {
        current: unrate[0]?.value ?? null, prev: unrate[1]?.value ?? null,
        delta:   unrateDelta,
        asOf:       unrate[0]?.date?.slice(0,7) ?? null,
        releasedAt: unrate[0]?.released ?? null,
        series:  unrateSeries,
      },
    },
    // Section 3: Consumer Health (3-Stage Pipeline)
    consumer: {
      umcsent:  { ...umcData },
      psavert:  { current: psaCur, prev: psaPrev, delta: psaDelta, mma3: psaMma3, sig: psaSig,
                  asOf:       psavert[0]?.date?.slice(0,7) ?? null,
                  releasedAt: psavert[0]?.released ?? null,
                  series5y: psavert.slice(0, 62).reverse()
                    .filter(d => d.date >= '2019-01-01')
                    .map(d => ({ date: d.date, value: d.value })) },
      delinq:   { current: delCur, prev: delPrev, yoyDelta: delYoyDelta, sig: delSig,
                  asOf:       delinq[0]?.date?.slice(0,7) ?? null,
                  releasedAt: delinq[0]?.released ?? null,
                  series5y: delinq.slice(0, 22).reverse()
                    .filter(d => d.date >= '2019-01-01')
                    .map(d => ({ date: d.date, value: d.value })) },
      realRetail: { current: realCoreCur, mma3: realCore3MMA, nominalMom: rsafsLatestMom,
                    sig: retailSig,
                    asOf:       rsafs[0]?.date?.slice(0,7) ?? null,
                    releasedAt: rsafs[0]?.released ?? null,
                    series: realRetailSeries, mma3Series: realRetailMma3Series },
      divCB,
      rsafsHist,
    },
  });
}

async function refreshT2(env) {
  try {
    const resp = await t2DataEndpoint(env, true);  // force=true: 엣지 캐시도 우회
    const data = await resp.json();
    await kvPut(env, KV_KEYS.t2, data, KV_TTL.t2);
  } catch(e) { console.error('refreshT2:', e.message); }
}

// ── T3 Global ───────────────────────────────────────────
async function t3Cached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.t3, force);
  if (cached) return json(cached);
  const body = await t3GlobalData(env, force);
  const putPromise = kvPut(env, KV_KEYS.t3, body, KV_TTL.t3);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(body);
}

async function refreshT3(env) {
  try {
    const body = await t3GlobalData(env, false);
    await kvPut(env, KV_KEYS.t3, body, KV_TTL.t3);
  } catch(e) { console.error('refreshT3:', e.message); }
}

// t3GlobalEndpoint: KV 캐시 래퍼 (외부 호환용)
async function t3GlobalEndpoint(env, force = false) {
  const body = await t3GlobalData(env, force);
  return json(body);
}

async function t3GlobalData(env, force = false) {
  // ── Yahoo Finance 차트 fetch 헬퍼 (period1/period2 기반) ─
  const yfFetch = async (ticker) => {
    const now    = Math.floor(Date.now() / 1000);
    const p1     = now - 5 * 365 * 24 * 3600; // 5년 전
    const url    = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&period1=${p1}&period2=${now}`;
    try {
      const r = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'Accept': 'application/json' },
        cf: force ? { cacheTtl: 0, cacheEverything: false } : { cacheTtl: 3600 },
      });
      if (!r.ok) return [];
      const d = await r.json();
      const result = d?.chart?.result?.[0];
      if (!result) return [];
      const ts = result.timestamp || [];
      const cl = result.indicators?.quote?.[0]?.close || [];
      return ts
        .map((t, i) => ({ date: new Date(t * 1000).toISOString().slice(0,10), value: cl[i] }))
        .filter(p => p.value != null && p.value > 0 && isFinite(p.value));
    } catch(e) { return []; }
  };

  // USDCNH=X 폴백: 데이터 부족 시 USDCNY=X 사용
  const yfCnh = async () => {
    const data = await yfFetch('USDCNH=X');
    if (data.length >= 30) return data;
    const fallback = await yfFetch('USDCNY=X');
    return fallback.length > data.length ? fallback : data;
  };

  const yf = yfFetch; // 하위 호환

  // ── FRED 헬퍼 ────────────────────────────────────────
  const apiKey = env?.FRED_API_KEY;
  const fiveYAgo = new Date(); fiveYAgo.setFullYear(fiveYAgo.getFullYear() - 5);
  const obsStart = fiveYAgo.toISOString().slice(0,10);

  const fredArr = async (id) => {
    if (!apiKey) return [];
    try {
      const u = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&sort_order=asc&observation_start=${obsStart}&limit=2000&frequency=d&aggregation_method=lin`;
      const r = await fetch(u, { cf: force ? { cacheTtl: 0, cacheEverything: false } : { cacheTtl: 86400 } });
      if (!r.ok) return [];
      const d = await r.json();
      return (d.observations || [])
        .filter(o => o.value !== '.')
        .map(o => ({ date: o.date, value: parseFloat(o.value) }));
      // asc 순서 = 이미 chronological
    } catch(e) { return []; }
  };

  // ── 병렬 fetch ───────────────────────────────────────
  const [dxy, gold, silver, copper, usdcnh, spy, eem, broadDollar,
         audusd, kospi, eurusd, fez] = await Promise.all([
    yf('DX-Y.NYB'),
    yf('GC=F'),
    yf('SI=F'),
    yf('HG=F'),
    yfCnh(),
    yf('SPY'),
    yf('EEM'),
    fredArr('DTWEXBGS'),
    yf('AUDUSD=X'),   // 아시아 원자재 프록시
    yf('^KS11'),      // KOSPI (yfFetch가 encodeURIComponent 처리)
    yf('EURUSD=X'),   // 유로존 통화 프록시
    yf('FEZ'),        // 유로스톡스 50 ETF
  ]);

  // ── 계산 헬퍼 ────────────────────────────────────────
  const sma = (arr, n) => arr.map((v, i, a) => {
    if (i < n-1) return null;
    const slice = a.slice(i-n+1, i+1).map(x => x.value);
    if (slice.some(x => x == null)) return null;
    return +(slice.reduce((s,x) => s+x, 0) / n).toFixed(4);
  });

  // ratio 시리즈 (날짜 기준 join)
  const ratioSeries = (a, b) => {
    const bMap = Object.fromEntries(b.map(d => [d.date, d.value]));
    return a.map(d => {
      const bv = bMap[d.date];
      if (!bv || bv === 0) return null;
      return { date: d.date, value: +(d.value / bv).toFixed(6) };
    }).filter(Boolean);
  };

  // 최근 N개 (가장 오래된 날짜순)
  const recent = (arr, n) => arr.slice(-n);

  // 3MMA 계산 후 마지막 2개 비교 (최근 20개 중 유효값 사용)
  const mma3Trend = (arr) => {
    if (arr.length < 3) return null;
    const vals = arr.slice(-20).map(d => d.value).filter(v => v != null && v > 0 && isFinite(v));
    if (vals.length < 3) return null;
    const cur3 = (vals.slice(-3).reduce((s,v) => s+v, 0) / 3);
    const prv3 = vals.length >= 4
      ? (vals.slice(-4,-1).reduce((s,v) => s+v, 0) / 3)
      : cur3; // 비교값 없으면 flat
    return { cur: +cur3.toFixed(4), prv: +prv3.toFixed(4), dir: cur3 > prv3 ? 'up' : cur3 < prv3 ? 'down' : 'flat' };
  };

  // 200DMA 최신값
  const dma200Latest = (arr) => {
    const vals = arr.slice(-200).map(d => d.value).filter(Boolean);
    if (vals.length < 200) return null;
    return +(vals.reduce((s,v) => s+v, 0) / 200).toFixed(4);
  };

  // ── GSR (금/은 비율) ────────────────────────────────
  const gsrSeries = ratioSeries(gold, silver);
  const gsr200DMA = dma200Latest(gsrSeries);
  const gsrCur    = gsrSeries.at(-1)?.value ?? null;
  const gsrAbove  = (gsrCur != null && gsr200DMA != null) ? gsrCur > gsr200DMA : null;
  // 신호: 현재가 > 200DMA → 위험 (안전자산 도피)
  const gsrSig    = gsrCur == null ? 'green' : gsrAbove ? 'red' : 'green';
  // 200DMA 대비 이격도(%)
  const gsrSpread = (gsrCur != null && gsr200DMA != null && gsr200DMA > 0)
    ? +((gsrCur - gsr200DMA) / gsr200DMA * 100).toFixed(2) : null;

  // ── 구리/금 비율 (Dr. Copper) ───────────────────────
  const cgSeries = ratioSeries(copper, gold);
  const cgMma3   = mma3Trend(cgSeries);
  const cgCur    = cgSeries.at(-1)?.value ?? null;
  // 신호: 현재가 > 3MMA → 성장 팽창 / < 3MMA → 수축 압력
  const cgSig    = cgCur == null || !cgMma3 ? 'yellow'
    : cgCur > cgMma3.cur ? 'green' : 'red';
  // 3MMA 대비 현재가 모멘텀(%)
  const cgSpread = (cgCur != null && cgMma3?.cur && cgMma3.cur > 0)
    ? +((cgCur - cgMma3.cur) / cgMma3.cur * 100).toFixed(3) : null;

  // ── USD/CNH ──────────────────────────────────────────
  const cnhMma3 = mma3Trend(usdcnh);
  const cnhCur  = usdcnh.at(-1)?.value ?? null;
  // 신호: 현재가 > 3MMA → 위안화 약세 → 압박 / < 3MMA → 강세 → 안정
  const cnhSig  = cnhCur == null || !cnhMma3 ? 'yellow'
    : cnhCur > cnhMma3.cur ? 'red' : 'green';
  // 3MMA 대비 이격 pips (×10000)
  const cnhSpread = (cnhCur != null && cnhMma3?.cur)
    ? +((cnhCur - cnhMma3.cur) / cnhMma3.cur * 100).toFixed(2) : null;

  // ── SPY/EEM 비율 ────────────────────────────────────
  const seRatio  = ratioSeries(spy, eem);
  const seMma3   = mma3Trend(seRatio);
  const seCur    = seRatio.at(-1)?.value ?? null;
  // 신호: 현재가 > 3MMA → 미국 쏠림(yellow) / < 3MMA → 자본 확산(green)
  const seSig    = seCur == null || !seMma3 ? 'yellow'
    : seCur > seMma3.cur ? 'yellow' : 'green';
  // 3MMA 대비 변화율(%)
  const seSpread = (seCur != null && seMma3?.cur && seMma3.cur > 0)
    ? +((seCur - seMma3.cur) / seMma3.cur * 100).toFixed(3) : null;

  // ── DXY vs Broad Dollar — 다이버전스 차트용 시리즈 ──
  // yf()는 이미 chronological, broadDollar는 fredArr asc로 chronological
  const cutoff = obsStart;
  const dxySeries5y   = dxy.filter(d => d.date >= cutoff);
  const broadSeries5y = broadDollar; // 이미 asc + observation_start로 5년치

  return {
    _savedAt: new Date().toISOString(),
    // Block 1 Row 1: 다이버전스 차트
    dxy:         { series: dxySeries5y, latest: dxySeries5y.at(-1) },
    broadDollar: { series: broadSeries5y, latest: broadSeries5y.at(-1) },
    spy5y:       spy.filter(d => d.date >= cutoff),

    // ── Block 2: 성장 엔진 ────────────────────────────────
    asiaEngine: (() => {
      const audS   = audusd.filter(d => d.date >= cutoff);
      const kospiS = kospi.filter(d => d.date >= cutoff);
      return {
        aud:   { series: audS,   latest: audS.at(-1)   ?? audusd.at(-1)   },
        kospi: { series: kospiS, latest: kospiS.at(-1) ?? kospi.at(-1)    },
      };
    })(),
    euroEngine: (() => {
      const eurS = eurusd.filter(d => d.date >= cutoff);
      const fezS = fez.filter(d => d.date >= cutoff);
      return {
        eurusd: { series: eurS, latest: eurS.at(-1) ?? eurusd.at(-1) },
        fez:    { series: fezS, latest: fezS.at(-1) ?? fez.at(-1)   },
      };
    })(),
    // Block 1 Row 2: 4-Card KPI
    gsr: {
      current: gsrCur, dma200: gsr200DMA, aboveDMA: gsrAbove, sig: gsrSig, spread: gsrSpread,
      series: recent(gsrSeries, 400).map(d => ({ date: d.date, value: d.value })),
    },
    copperGold: {
      current: cgCur, mma3: cgMma3, sig: cgSig, spread: cgSpread,
      series: recent(cgSeries, 400).map(d => ({ date: d.date, value: d.value })),
    },
    usdcnh: {
      current: cnhCur, mma3: cnhMma3, sig: cnhSig, spread: cnhSpread,
      series: recent(usdcnh, 400).map(d => ({ date: d.date, value: d.value })),
    },
    spyEem: {
      current: seCur, mma3: seMma3, sig: seSig, spread: seSpread,
      series: recent(seRatio, 400).map(d => ({ date: d.date, value: d.value })),
    },
  };
}


async function refreshLiq(env) {
  try {
    const resp = await liqDataEndpoint(env);
    const data = await resp.json();
    await kvPut(env, KV_KEYS.liq, data, KV_TTL.liq);
  } catch(e) { console.error('refreshLiq:', e.message); }
}

async function refreshYieldsHist(env) {
  try {
    const resp = await yieldsHistory(env);
    const data = await resp.json();
    await kvPut(env, KV_KEYS.yieldsHist, data, KV_TTL.yieldsHist);
  } catch(e) { console.error('refreshYieldsHist:', e.message); }
}


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 1. FRED API 프록시
//    GET /fred?series_id=SOFR&limit=30&sort_order=desc
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 프론트엔드가 실제로 쓰는 시리즈만 허용 — 임의 파라미터 주입·FRED 키 남용 차단
const FRED_PROXY_ALLOW = new Set(['DFII10', 'WRMFNS', 'IORB', 'DTWEXBGS', 'WALCL']);
async function fredProxy(url, env) {
  const seriesId  = (url.searchParams.get('series_id') || '').toUpperCase();
  const limit     = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '60', 10) || 60, 1), 1000);
  const sortOrder = url.searchParams.get('sort_order') === 'asc' ? 'asc' : 'desc';
  const units     = ['lin', 'chg', 'ch1', 'pch', 'pc1', 'pca'].includes(url.searchParams.get('units'))
    ? url.searchParams.get('units') : '';

  if (!FRED_PROXY_ALLOW.has(seriesId)) return json({ error: 'series_id not allowed' }, 400);

  const apiKey = env.FRED_API_KEY;
  let fredUrl  = `https://api.stlouisfed.org/fred/series/observations`
    + `?series_id=${encodeURIComponent(seriesId)}`
    + `&api_key=${apiKey}`
    + `&file_type=json`
    + `&limit=${limit}`
    + `&sort_order=${sortOrder}`;

  if (units) fredUrl += `&units=${units}`;

  const resp = await fetch(fredUrl, { cf: { cacheTtl: 3600 } }); // 1h 캐시
  if (!resp.ok) return json({ error: `FRED error ${resp.status}` }, resp.status);

  const data = await resp.json();
  return json(data);
}


async function iciMMF(env, ctx) {
  const KV_KEY = 'mmf_history_v1';

  // ── Step 1: KV에서 기존 누적 히스토리 읽기 ──
  let kvHistory = [];
  try {
    if (env?.MMF_KV) {
      const stored = await env.MMF_KV.get(KV_KEY, { type: 'json' });
      if (Array.isArray(stored)) kvHistory = stored;
    }
  } catch(e) { /* KV 읽기 실패 시 빈 배열로 진행 */ }

  // ── Step 2: ICI 페이지 스크래핑 ──
  const iciUrl = 'https://www.ici.org/research/stats/mmf';
  const resp = await fetch(iciUrl, {
    headers: {
      'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept':          'text/html,application/xhtml+xml',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    cf: { cacheTtl: 21600 },
  });

  if (!resp.ok) {
    // 스크래핑 실패 시 KV 히스토리만이라도 반환
    return json({
      error:   `ICI fetch failed: ${resp.status}`,
      source:  'ICI (KV fallback)',
      history: kvHistory,
      kvCount: kvHistory.length,
    }, resp.status >= 500 ? 502 : 200);
  }

  const html = await resp.text();

  // ── Step 3: 파싱 ──
  // 최신 전체 총자산
  const totalMatch = html.match(/\$([\d.]+)\s*trillion/i);
  const total = totalMatch ? parseFloat(totalMatch[1]) : null;

  // 흐름 (소매/기관 분리)
  const flowPat = /(?:increased|decreased) by \$([\d.]+)\s*billion to \$([\d.]+)\s*trillion/gi;
  const flows = [];
  let m;
  while ((m = flowPat.exec(html)) !== null && flows.length < 5) {
    const sign = m[0].toLowerCase().startsWith('decreased') ? -1 : 1;
    flows.push({ change: sign * parseFloat(m[1]), value: parseFloat(m[2]) });
  }
  const changeB = flows[0]?.change ?? null;
  const retailT = flows[1]?.value  ?? null;
  const instT   = total != null && retailT != null ? +(total - retailT).toFixed(3) : null;

  // 날짜 파싱 — "week ended Wednesday, March 19" 또는 "week ended Wednesday, March 19, 2026"
  const dateMatch = html.match(/week ended\s+\w+,\s+(\w+ \d+(?:,?\s*\d{4})?)/i);
  const weekEnded = dateMatch ? dateMatch[1].trim() : null;

  // 연도 보정 후 ISO(YYYY-MM-DD) 변환
  const normalizeDate = (d) => {
    if (!d) return null;
    // 연도 없으면 현재 연도 추가
    const withYear = /\d{4}/.test(d) ? d : `${d}, ${new Date().getFullYear()}`;
    // "April 1, 2026" → Date → ISO
    try {
      const parsed = new Date(withYear);
      if (!isNaN(parsed.getTime())) {
        return parsed.toISOString().slice(0, 10);  // "2026-04-01"
      }
    } catch(e) {}
    return withYear;  // 파싱 실패 시 원본 반환
  };
  const weekEndedFull = normalizeDate(weekEnded);

  if (total === null) {
    return json({
      error:   'ICI 파싱 실패: trillion 수치 미발견',
      htmlLength: html.length,
      history: kvHistory,
      kvCount: kvHistory.length,
    }, 500);
  }

  // ── Step 4: KV 누적 — 이번 주차가 없으면 추가 ──
  const newEntry = {
    weekEnded: weekEndedFull,
    total:     +total.toFixed(3),          // $T
    totalB:    +(total * 1000).toFixed(1), // $B 환산 (4W Δ 계산용)
    retail:    retailT != null ? +retailT.toFixed(3) : null,
    inst:      instT   != null ? +instT.toFixed(3)   : null,
    changeB:   changeB != null ? +changeB.toFixed(2) : null,
    savedAt:   new Date().toISOString().slice(0, 10),
  };

  // 같은 주가 있으면 수정치 반영, 없으면 추가 → 주차 내림차순 정렬 (최신이 index 0)
  const strip = h => JSON.stringify({ ...h, savedAt: undefined });
  const exists = kvHistory.findIndex(h => h.weekEnded === newEntry.weekEnded);
  const changed = exists < 0 || strip(kvHistory[exists]) !== strip(newEntry);
  if (exists >= 0) kvHistory[exists] = newEntry;
  else kvHistory.push(newEntry);
  kvHistory = kvHistory
    .sort((a, b) => new Date(b.weekEnded) - new Date(a.weekEnded))
    .slice(0, 52);                                   // 최대 52주 보관

  // ── Step 5: 값이 바뀐 경우에만 저장 (방문자 요청마다 공유 이력을 다시 쓰지 않도록) ──
  if (changed && env?.MMF_KV) {
    const putPromise = env.MMF_KV.put(KV_KEY, JSON.stringify(kvHistory), {
      expirationTtl: 60 * 60 * 24 * 400,
    }).catch(e => console.error('[KV PUT ERROR] iciMMF', e.message));
    if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;   // cron 경로는 await
  }

  // ── Step 6: 반환 ──
  return json({
    source:        'ICI (ici.org) + KV 누적',
    weekEnded:     weekEndedFull,
    total,
    retail:        retailT,
    institutional: instT,
    changeB,
    unit:          'trillion_usd',
    history:       kvHistory,   // KV 누적 전체 히스토리 (최신→과거 순)
    kvCount:       kvHistory.length,
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 3-C. H.4.1 역사적 데이터 (주간 아카이브)
//    GET /h41-history?weeks=N    최근 N주치 핵심 지표
//    Fed 아카이브: federalreserve.gov/releases/h41/YYYYMMDD/
//    그래프용 시계열 생성
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function h41HistoryFetcher(url, weeksOverride = null, env = null) {
  const weeks  = weeksOverride ?? Math.min(Math.max(parseInt(url.searchParams.get('weeks') || '12', 10) || 12, 1), 52);
  const debug  = url.searchParams.get('debug') === '1';

  // ── STEP 1: Fed H.4.1 인덱스에서 실제 릴리즈 URL 목록 수집 ──
  // 수요일 data → 목요일 발표 → 아카이브도 목요일 날짜 사용
  const INDEX_URL = 'https://www.federalreserve.gov/releases/h41/';
  let releaseUrls = [];
  const urlAttempts = [];

  // 실제 발표일 목록 — 추수감사절·성탄절 등으로 밀린 발표일도 정확 (정적 HTML에는 링크가 없음)
  try {
    const rdResp = await fetch('https://www.federalreserve.gov/releases/h41/releaseDates.json', {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)' }, cf: { cacheTtl: 3600 },
    });
    if (rdResp.ok) {
      const rd = await rdResp.json();
      const dates = rd.flatMap(y => (y.Months || []).flatMap(m => m.Dates || []))
        .filter(d => /^\d{8}$/.test(d)).sort().reverse().slice(0, weeks);
      releaseUrls = dates.map(d => ({ date: d, url: `https://www.federalreserve.gov/releases/h41/${d}/` }));
    }
  } catch(e) {
    urlAttempts.push({ step: 'releaseDates', error: e.message });
  }

  if (!releaseUrls.length) try {
    const idxResp = await fetch(INDEX_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)' },
      cf: { cacheTtl: 3600 },
    });
    if (idxResp.ok) {
      const idxHtml = await idxResp.text();
      // 인덱스 페이지에서 /releases/h41/YYYYMMDD/ 형태 링크 추출
      const linkRe = /href="\/releases\/h41\/(\d{8})\/"/gi;
      let m;
      while ((m = linkRe.exec(idxHtml)) !== null) {
        const dateStr = m[1];
        if (!releaseUrls.find(u => u.date === dateStr)) {
          releaseUrls.push({
            date: dateStr,
            url: `https://www.federalreserve.gov/releases/h41/${dateStr}/`,
          });
        }
      }
      // 날짜 내림차순 정렬 후 최근 N개만
      releaseUrls.sort((a, b) => b.date.localeCompare(a.date));
      releaseUrls = releaseUrls.slice(0, weeks);
    }
  } catch(e) {
    urlAttempts.push({ step: 'index', error: e.message });
  }

  // 인덱스 파싱 실패 시 목요일 날짜 추정으로 폴백
  if (!releaseUrls.length) {
    const pad = n => String(n).padStart(2, '0');
    const now = new Date();
    // 가장 최근 수요일 구하기
    let d = new Date(now);
    const dow = d.getDay(); // 0=Sun
    const daysToLastWed = (dow + 4) % 7;
    d.setDate(d.getDate() - daysToLastWed);
    // 수요일+1 = 목요일(발표일)
    for (let i = 0; i < weeks; i++) {
      const thu = new Date(d);
      thu.setDate(thu.getDate() + 1); // 목요일
      const dateStr = `${thu.getFullYear()}${pad(thu.getMonth()+1)}${pad(thu.getDate())}`;
      releaseUrls.push({
        date: dateStr,
        url: `https://www.federalreserve.gov/releases/h41/${dateStr}/`,
      });
      d.setDate(d.getDate() - 7);
    }
    urlAttempts.push({ step: 'fallback', msg: '인덱스 실패 → 목요일 날짜 추정 사용' });
  }

  // ── STEP 2: 각 릴리즈 HTML 파싱 ──
  const parseMetrics = async ({ date, url: fedUrl }) => {
    urlAttempts.push({ date, url: fedUrl, status: 'fetching' });
    try {
      const resp = await fetch(fedUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)' },
        cf: { cacheTtl: 604800 },
      });
      const status = resp.status;
      if (!resp.ok) {
        urlAttempts.push({ date, url: fedUrl, status, error: `HTTP ${status}` });
        return { date, url: fedUrl, httpStatus: status, error: `HTTP ${status}` };
      }
      const html = await resp.text();
      urlAttempts.push({ date, url: fedUrl, status: 'ok', htmlLen: html.length });

      const clean = s => s.replace(/<[^>]+>/g,' ').replace(/&amp;/g,'&')
        .replace(/&nbsp;/g,' ').replace(/&#x[\da-fA-F]+;/g,' ')
        .replace(/&#\d+;/g,' ').replace(/&[a-zA-Z]+;/g,' ')
        .replace(/\s+/g,' ').trim();
      const parseN = s => {
        if (!s) return null;
        const c = s.replace(/[^0-9.]/g,'');
        if (!c) return null;
        const n = parseFloat(c);
        return isNaN(n) || n === 0 ? null : n;
      };
      const tables = [];
      const tRe = /<table[\s\S]*?<\/table>/gi;
      let tm;
      while ((tm = tRe.exec(html)) !== null) tables.push(tm[0]);

      const extractRows = tbl => {
        const rows = [];
        const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
        let tr;
        while ((tr = trRe.exec(tbl)) !== null) {
          const cells = [];
          const tdRe = /<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/gi;
          let td;
          while ((td = tdRe.exec(tr[1])) !== null) cells.push(clean(td[1]));
          if (cells.length >= 1) rows.push(cells);
        }
        return rows;
      };
      const hasDataRow = (rows, pat, min=100000) => rows.some(r => {
        const lc = r[0].toLowerCase().replace(/\s+\d+$/,'').trim();
        const v = r.length > 1 ? parseN(r[1]) : null;
        return lc.includes(pat) && v != null && v >= min;
      });
      const getVal = (rows, pat) => {
        for (const r of rows) {
          const lc = r[0].toLowerCase().replace(/\s+\d+$/,'').trim();
          if (lc.includes(pat) && r.length > 1) {
            const v = parseN(r[1]);
            if (v != null && v > 0) return v;
          }
        }
        return null;
      };
      // fima_repo는 평상시 $0 → 행이 있어도 0일 수 있으므로 별도 처리
      const getValOrZero = (rows, pat) => {
        if (!rows) return 0;
        for (const r of rows) {
          const lc = r[0].toLowerCase().replace(/\s+\d+$/,'').trim();
          if (lc.includes(pat) && r.length > 1) {
            const v = parseFloat((r[1]||'').replace(/[^0-9.]/g,''));
            return isNaN(v) ? 0 : v;
          }
        }
        return 0;
      };

      let tAssets=null, tLiabDet=null, tLiabSum=null, tMbs=null, tMat=null, tMemo=null;
      for (const t of tables) {
        const rows = extractRows(t);
        if (!tAssets   && hasDataRow(rows,'reserve bank credit',5000000))       tAssets   = rows;
        if (!tLiabSum  && hasDataRow(rows,'currency in circulation',2000000))   tLiabSum  = rows;
        if (!tLiabDet  && hasDataRow(rows,'federal reserve notes, net',2000000)
                       && hasDataRow(rows,'depository institutions',2000000)
                       && rows.some(r=>r.length>=14))                           tLiabDet = rows;
        if (!tMbs      && hasDataRow(rows,'mortgage-backed securities held outright',1000000)) tMbs = rows;
        if (!tMat      && rows.some(r => r[0].toLowerCase().includes('remaining maturity') && r.length >= 8)) tMat = rows;
        // Memorandum: securities held in custody > $2T
        if (!tMemo     && hasDataRow(rows,'securities held in custody',2000000)) tMemo = rows;
      }

      // ── 만기별 파싱 (Table[6] 매트릭스) ──
      // 구조: 헤더행(Remaining Maturity | Within 15 days | ...) + 자산행(Holdings cells[1..6])
      const MAT_KEYS   = ['within_15d','d16_90d','d91_1y','y1_5y','y5_10y','over_10y'];
      const matResult  = {};
      if (tMat) {
        let inSection = null;
        const holdingsSeen = {};
        for (const cells of tMat) {
          const lc = cells[0].toLowerCase().replace(/\s+\d+$/,'').trim();
          if (lc.includes('u.s. treasury securities'))  inSection = 'treasury';
          if (lc.includes('federal agency debt'))        inSection = 'agency';
          if (lc === 'holdings' && inSection) {
            holdingsSeen[inSection] = (holdingsSeen[inSection]||0) + 1;
            if (holdingsSeen[inSection] === 1) {          // 첫 번째 Holdings = 보유량
              MAT_KEYS.forEach((k, ci) => {
                const v = parseN(cells[ci+1]);
                if (v != null) matResult[inSection+'_'+k] = v; // Millions
              });
            }
          }
        }
      }

      const dm = html.match(/([A-Z][a-z]+ \d{1,2},\s*\d{4})/);
      // ON RRP domestic = rrp_total(Table2) - rrp_foreign(Table2)
      const rrp_total_m   = getVal(tLiabSum, 'reverse repurchase agreements');
      const rrp_foreign_m = getVal(tLiabSum, 'foreign official and international');
      const on_rrp_m = (rrp_total_m != null && rrp_foreign_m != null)
        ? rrp_total_m - rrp_foreign_m : rrp_total_m;
      return {
        date,
        releaseDate: dm ? dm[1] : date,
        httpStatus: 200,
        tableCount: tables.length,
        reserve_credit:   getVal(tAssets,  'reserve bank credit'),
        treasury_total:   getVal(tAssets,  'u.s. treasury securities'),
        securities_total: getVal(tAssets,  'securities held outright'),
        loans:            getVal(tAssets,  'loans'),                       // ← 긴급대출 추가
        reserve_balances: getVal(tLiabDet, 'depository institutions'),
        rrp_total:        rrp_total_m,
        rrp_foreign:      rrp_foreign_m,
        on_rrp_domestic:  on_rrp_m,
        rrp_bs:           getVal(tLiabDet, 'reverse repurchase agreements'),
        tga:              getVal(tLiabDet, 'u.s. treasury, general account'),
        fed_notes_net:    getVal(tLiabDet, 'federal reserve notes, net'),
        mbs_total:        getVal(tMbs,     'mortgage-backed securities held outright'),
        // Custody & FIMA
        custody_treasury: getVal(tMemo,    'marketable u.s. treasury'),
        fima_repo:        getValOrZero(tAssets, 'foreign official'),
        maturity:         matResult,
      };
    } catch(e) {
      urlAttempts.push({ date, url: fedUrl, status: 'error', error: e.message });
      return { date, url: fedUrl, httpStatus: 0, error: e.message };
    }
  };

  // 지난 H.4.1 발표분은 바뀌지 않으므로 발표일별로 KV에 영구 캐시 → 새 발표분만 원격 조회.
  // 1회 실행당 원격 조회 상한(서브요청 50건 한도) — 남은 주는 다음 요청에서 채워짐.
  const REL_KEY = d => `h41_rel_v1_${d}`;
  const MAX_REMOTE = 40;
  const cachedRel = env?.MMF_KV
    ? await Promise.all(releaseUrls.map(r => env.MMF_KV.get(REL_KEY(r.date), { type: 'json' }).catch(() => null)))
    : releaseUrls.map(() => null);
  const missing = releaseUrls.filter((_, i) => !cachedRel[i]).slice(0, MAX_REMOTE);
  const fetched = {};
  for (let i = 0; i < missing.length; i += 8) {
    const batch = await Promise.all(missing.slice(i, i+8).map(parseMetrics));
    for (const r of batch) {
      fetched[r.date] = r;
      if (env?.MMF_KV && r && !r.error && r.reserve_balances != null) {
        await env.MMF_KV.put(REL_KEY(r.date), JSON.stringify(r), { expirationTtl: 86400 * 400 }).catch(() => {});
      }
    }
  }
  const results = releaseUrls.map((r, i) => cachedRel[i] || fetched[r.date]).filter(Boolean);

  const valid = results.filter(r => r && !r.error && r.reserve_balances != null);
  const B = v => v ? +(v/1000).toFixed(1) : null;

  return json({
    success: true,
    weeks_requested: weeks,
    weeks_retrieved: valid.length,
    urls_tried: releaseUrls.length,
    series: {
      labels:           valid.map(r => r.releaseDate),
      // ── Pipe1 핵심 ──
      reserve_balances: valid.map(r => B(r.reserve_balances)),
      rrp:              valid.map(r => B(r.rrp)),
      tga:              valid.map(r => B(r.tga)),
      buffer:           valid.map(r => (r.reserve_balances&&r.rrp) ? B(r.reserve_balances+r.rrp) : null),
      loans:            valid.map(r => B(r.loans)),                    // ← 긴급대출 시계열 추가
      currency_circ:    valid.map(r => B(r.fed_notes_net)),             // 화폐 유통량 proxy
      // ── 자산 ──
      reserve_credit:      valid.map(r => B(r.reserve_credit)),   // 총자산 프록시 (B)
      treasury_total:      valid.map(r => B(r.treasury_total)),
      mbs_total:           valid.map(r => B(r.mbs_total)),
      // ── ON RRP domestic + 원본 RRP 분리 ──
      on_rrp_domestic:     valid.map(r => r.on_rrp_domestic != null ? +(r.on_rrp_domestic/1000).toFixed(1) : null),
      rrp_total:           valid.map(r => B(r.rrp_total)),
      rrp_foreign:         valid.map(r => B(r.rrp_foreign)),
      // ── Custody & FIMA ──
      custody_treasury:    valid.map(r => B(r.custody_treasury)),
      fima_repo:           valid.map(r => B(r.fima_repo)),
      // ── 만기별 국채 (treasury) — Millions → Billions ──
      treasury_within_15d: valid.map(r => B(r.maturity?.treasury_within_15d)),
      treasury_d16_90d:    valid.map(r => B(r.maturity?.treasury_d16_90d)),
      treasury_d91_1y:     valid.map(r => B(r.maturity?.treasury_d91_1y)),
      treasury_y1_5y:      valid.map(r => B(r.maturity?.treasury_y1_5y)),
      treasury_y5_10y:     valid.map(r => B(r.maturity?.treasury_y5_10y)),
      treasury_over_10y:   valid.map(r => B(r.maturity?.treasury_over_10y)),
      // ── 만기별 Agency/MBS ──
      agency_within_15d:   valid.map(r => B(r.maturity?.agency_within_15d)),
      agency_d16_90d:      valid.map(r => B(r.maturity?.agency_d16_90d)),
      agency_d91_1y:       valid.map(r => B(r.maturity?.agency_d91_1y)),
      agency_y1_5y:        valid.map(r => B(r.maturity?.agency_y1_5y)),
      agency_y5_10y:       valid.map(r => B(r.maturity?.agency_y5_10y)),
      agency_over_10y:     valid.map(r => B(r.maturity?.agency_over_10y)),
    },
    raw: results,
    ...(debug ? { urlAttempts } : {}),
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 3-B. Fed H.4.1 원본 HTML 직접 파싱
//    GET /h41-html
//    CORS 차단 없음 (Worker 서버사이드 fetch)
//    Table 1: 자산/부채 전체, Table 2: 만기별 보유량
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function h41HtmlParser(env, ctx, forceRefresh = false) {
  const FED_URL = 'https://www.federalreserve.gov/releases/h41/current/';

  // ── KV 캐시 확인 (force=1 아닐 때) ──
  const cachedH41 = await cacheFirst(env, KV_KEYS.h41Html, forceRefresh);
  if (cachedH41) return json(cachedH41);

  let html;
  try {
    const resp = await fetch(FED_URL, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)', 'Accept': 'text/html' },
      cf: { cacheTtl: 21600 },
    });
    if (!resp.ok) throw new Error(`Fed HTTP ${resp.status}`);
    html = await resp.text();
  } catch(e) {
    // ── fetch 실패 시 KV 캐시 fallback ──
    const cached = await kvGet(env, KV_KEYS.h41Html);
    if (cached) return json({ ...cached, _fromCache: true });
    return json({ success: false, error: `Fed 원본 HTML 접근 실패: ${e.message}` }, 502);
  }

  // ── 날짜 파싱 ──
  const releaseDateMatch = html.match(/([A-Z][a-z]+ \d{1,2},\s*\d{4})/);
  const releaseDate = releaseDateMatch ? releaseDateMatch[1].trim() : null;

  // ── 셀 정제 (엔티티 이중 디코딩 + 완전 정리) ──
  const cleanCell = (raw) => {
    let s = raw
      .replace(/<[^>]+>/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&amp;/g, '&')           // 이중 인코딩 처리
      .replace(/&nbsp;/g, ' ')
      .replace(/&#x[\da-fA-F]+;/g, ' ') // 16진수 엔티티
      .replace(/&#\d+;/g, ' ')          // 10진수 엔티티 (&#160; 등)
      .replace(/&[a-zA-Z]+;/g, ' ')     // 이름 엔티티 (&mdash; 등)
      .replace(/\s+/g, ' ')
      .trim();
    return s;
  };

  // ── 숫자 파싱 (괄호=음수, 쉼표/공백 허용) ──
  const parseNum = (s) => {
    if (!s) return null;
    const neg = /^\([\d,. ]+\)$/.test(s.trim());
    const clean = s.replace(/[^0-9.]/g, '');
    if (!clean) return null;
    const n = parseFloat(clean);
    return isNaN(n) ? null : (neg ? -n : n);
  };

  // ── 테이블 행 추출 (th + td 모두) ──
  const extractRows = (tableHtml) => {
    const rows = [];
    const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let tr;
    while ((tr = trRe.exec(tableHtml)) !== null) {
      const cellRe = /<(?:td|th)[^>]*>([\s\S]*?)<\/(?:td|th)>/gi;
      const cells = [];
      let cell;
      while ((cell = cellRe.exec(tr[1])) !== null) cells.push(cleanCell(cell[1]));
      if (cells.length >= 1) rows.push(cells);
    }
    return rows;
  };

  // ── 테이블 분리 ──
  const tables = [];
  const tblRe = /<table[\s\S]*?<\/table>/gi;
  let tbl;
  while ((tbl = tblRe.exec(html)) !== null) tables.push(tbl[0]);

  // ── 테이블 식별 (숫자값 임계값 기반 — 헤더/각주 테이블 오매칭 방지) ──
  // 텍스트 포함 여부만으로 식별 시 숨겨진 테이블에 오매칭 발생
  // 반드시 실제 수치(>threshold)가 있는 행을 가진 테이블만 매칭
  let tAssets=null, tLiabSum=null, tLiabDet=null, tMbs=null, tMat=null, tMemo=null, tNotes=null;

  const hasDataRow = (rows, labelPattern, minVal=100000, colIdx=1) => {
    // label 매칭 + 실제 수치(minVal M 이상) 행 존재 여부
    for (const cells of rows) {
      const lc = cells[0].toLowerCase().replace(/\s+\d+$/, '').trim();
      if (!lc.includes(labelPattern)) continue;
      if (cells.length <= colIdx) continue;
      const v = parseNum(cells[colIdx]);
      if (v !== null && Math.abs(v) >= minVal) return true;
    }
    return false;
  };

  for (const t of tables) {
    const rows = extractRows(t);
    // ── 자산 테이블 (T1): "Reserve Bank credit" > $5T ──
    if (!tAssets && hasDataRow(rows, 'reserve bank credit', 5000000))
      tAssets = rows;
    // ── 부채 요약 (T2): "Currency in circulation" > $2T ──
    if (!tLiabSum && hasDataRow(rows, 'currency in circulation', 2000000))
      tLiabSum = rows;
    // ── 부채 상세 (T16): "Federal Reserve notes, net" > $2T AND col:14+ ──
    // col:14 조건 추가: 지역별 분리 테이블만 해당 → 헤더/요약 테이블 제외
    if (!tLiabDet &&
        hasDataRow(rows, 'federal reserve notes, net', 2000000, 1) &&
        hasDataRow(rows, 'depository institutions', 2000000, 1) &&
        rows.some(r => r.length >= 14)) {
      tLiabDet = rows;
    }
    // ── MBS (T8): "Mortgage-backed securities held outright" > $1T ──
    if (!tMbs && hasDataRow(rows, 'mortgage-backed securities held outright', 1000000))
      tMbs = rows;
    // ── 만기 (T6): "Remaining Maturity" 행 + col:8 ──
    if (!tMat && rows.some(r => r[0].toLowerCase().includes('remaining maturity') && r.length >= 8))
      tMat = rows;
    // ── 메모란덤 (T4): "Securities held in custody" > $2T ──
    if (!tMemo && hasDataRow(rows, 'securities held in custody', 2000000))
      tMemo = rows;
    // ── FR Notes (T19): "Federal Reserve notes outstanding" > $2T ──
    if (!tNotes && hasDataRow(rows, 'federal reserve notes outstanding', 2000000))
      tNotes = rows;
  }

  // ── 행 매칭 유틸 ──
  // deltaMode 'raw' : cells[colIdx+1]이 이미 변화량 (Table 1/2/4/8/19 구조)
  // deltaMode 'none': delta 계산 안 함 (Table 16, col:14 지역값이라 무의미)
  // deltaMode 'calc': cells[colIdx+1]이 이전값, 차이 계산
  const matchRow = (rows, patterns, colIdx=1, deltaMode='raw') => {
    if (!rows) return null;
    for (const cells of rows) {
      const lc = cells[0].toLowerCase().replace(/\s+\d+$/, '').trim();
      const match = patterns.some(p => lc.includes(p.toLowerCase()));
      if (match && cells.length > colIdx) {
        const val = parseNum(cells[colIdx]);
        if (val !== null && val !== 0) {
          let deltaB = null;
          if (deltaMode === 'raw' && cells.length > colIdx+1) {
            const d = parseNum(cells[colIdx+1]);
            if (d !== null) deltaB = d / 1000;
          } else if (deltaMode === 'calc' && cells.length > colIdx+1) {
            const prev = parseNum(cells[colIdx+1]);
            if (prev !== null) deltaB = (val - prev) / 1000;
          }
          return { val, deltaB, valueB: val/1000, valueM: val };
        }
      }
    }
    return null;
  };

  const rec = (key, label, rows, patterns, colIdx=1, deltaMode='raw') => {
    const r = matchRow(rows, patterns, colIdx, deltaMode);
    if (!r) return;
    data[key] = { label, ...r };
  };

  // recOrZero: 행을 찾지 못하면 0으로 기본값 설정 (FIMA Repo 평상시 $0 처리용)
  const recOrZero = (key, label, rows, patterns, colIdx=1, deltaMode='raw') => {
    const r = matchRow(rows, patterns, colIdx, deltaMode);
    data[key] = r
      ? { label, ...r }
      : { label, val: 0, deltaB: null, valueB: 0, valueM: 0, isZeroDefault: true };
  };

  const data = {};

  // ═══════════════════════════════════════════
  // SECTION 1: 자산 공급 요인 (Table[1], col:5)
  // cells[1]=현재주, cells[2]=전주, cells[3]=변화, cells[4]=작년
  // ═══════════════════════════════════════════
  rec('reserve_credit',   '연준 신용 총액',          tAssets, ['reserve bank credit']);
  rec('securities_total', '보유 증권 합계',           tAssets, ['securities held outright']);
  rec('treasury_total',   '미 국채 합계',             tAssets, ['u.s. treasury securities']);
  rec('treasury_bills',   '단기채 (T-Bills)',         tAssets, ['bills']);
  rec('treasury_notes',   '중장기채 (Notes&Bonds)',   tAssets, ['notes and bonds, nominal']);
  rec('treasury_tips',    'TIPS (물가연동)',           tAssets, ['inflation-indexed']);
  rec('agency_debt',      'Agency 채권',              tAssets, ['federal agency debt securities']);
  rec('mbs_t1',           'MBS (자산테이블)',          tAssets, ['mortgage-backed securities']);
  rec('loans',            '연준 대출',                tAssets, ['loans']);
  rec('repo_assets',      '환매조건부채권(자산합계)',   tAssets, ['repurchase agreements']);
  // FIMA Repo: 자산(Assets) 측 "Repurchase agreements > Foreign official"
  // 평상시 $0 (외국 중앙은행이 연준에서 비상 차입 없음) → recOrZero로 기본값 0 처리
  // ⚠️ 패턴 주의: tAssets에서만 검색하므로 tLiabSum의 "foreign official and international"과 구분됨
  recOrZero('fima_repo',  'FIMA Repo (긴급 대출)',    tAssets, ['foreign official']);
  rec('fx_assets',        '외화 자산',                tAssets, ['foreign currency denominated']);
  rec('gold_sdr',         '금·SDR',                  tAssets, ['gold stock', 'special drawing rights cert']);
  rec('total_supply',     '총 공급 요인',              tAssets, ['total factors supplying']);

  // ═══════════════════════════════════════════
  // SECTION 2: 부채 흡수 요인 (Table[2], col:5)
  // rrp_foreign = 외국 중앙은행의 정상 예치금(Foreign Official RRP)
  // → Pipe0 ON RRP domestic = rrp_total - rrp_foreign 계산에 사용 (올바른 용도)
  // ═══════════════════════════════════════════
  rec('currency_circ',    '유통 화폐',               tLiabSum, ['currency in circulation']);
  rec('rrp_total',        '역레포 합계',              tLiabSum, ['reverse repurchase agreements']);
  rec('rrp_foreign',      '외국계 정상 예치(역레포)', tLiabSum, ['foreign official and international']);
  rec('treasury_cash',    '재무부 현금보유',           tLiabSum, ['treasury cash holdings']);
  rec('reserve_bal_t2',   '준비금 잔고 (요인테이블)',  tLiabSum, ['reserve balances with federal']);
  rec('tga_t2',           'TGA (요인테이블)',          tLiabSum, ['u.s. treasury, general account', 'treasury, general account']);
  rec('total_absorb',     '총 흡수 요인',             tLiabSum, ['total factors absorbing']);

  // ═══════════════════════════════════════════
  // SECTION 3: 통합대차대조표 부채 (Table[16], col:14, col[1]=Total)
  // ═══════════════════════════════════════════
  rec('fed_notes_net',    'FR Notes (순)',            tLiabDet, ['federal reserve notes, net'], 1, 'none');
  rec('rrp_bs',           '역레포 (대차대조표)',       tLiabDet, ['reverse repurchase agreements'], 1, 'none');
  rec('deposits_total',   '예금 합계',               tLiabDet, ['deposits'], 1, 'none');
  rec('reserve_balances', '★ 은행 준비금',            tLiabDet, ['depository institutions'], 1, 'none');
  rec('tga',              '★ TGA (재무부 계정)',      tLiabDet, ['u.s. treasury, general account', 'treasury, general account'], 1, 'none');
  rec('foreign_deposits', '외국계 예금',              tLiabDet, ['foreign official', 'foreign and international'], 1, 'none');

  // ═══════════════════════════════════════════
  // SECTION 4: MBS 상세 (Table[8], col:2)
  // cells[1]=Wednesday값 (단일 컬럼)
  // ═══════════════════════════════════════════
  rec('mbs_total',        'MBS 보유 합계',            tMbs, ['mortgage-backed securities held outright']);
  rec('mbs_residential',  '주거용 MBS',               tMbs, ['residential mortgage-backed']);
  rec('mbs_commercial',   '상업용 MBS',               tMbs, ['commercial mortgage-backed']);
  rec('mbs_buy_commit',   'MBS 매입 약정',             tMbs, ['commitments to buy']);
  rec('mbs_sell_commit',  'MBS 매도 약정',             tMbs, ['commitments to sell']);

  // ═══════════════════════════════════════════
  // SECTION 5: 메모란덤 (Table[4], col:5) — 외국계 커스터디
  // ═══════════════════════════════════════════
  rec('custody_total',    '외국계 커스터디 합계',      tMemo, ['securities held in custody']);
  rec('custody_treasury', '커스터디 국채',             tMemo, ['marketable u.s. treasury']);
  rec('custody_agency',   '커스터디 Agency+MBS',      tMemo, ['federal agency debt and mortgage']);
  rec('custody_other',    '커스터디 기타',             tMemo, ['other securities']);
  rec('sec_lent',         '대출 증권',                tMemo, ['securities lent to dealers']);

  // ═══════════════════════════════════════════
  // SECTION 6: FR Notes & 담보 (Table[19], col:2)
  // ═══════════════════════════════════════════
  rec('notes_outstanding',    'FR Notes 발행 잔고',   tNotes, ['federal reserve notes outstanding']);
  rec('notes_collateralized', '담보 대상 Notes',      tNotes, ['federal reserve notes to be collateral']);
  rec('collateral_total',     '담보 합계',            tNotes, ['collateral held against']);
  rec('collateral_pledged',   '담보 국채+Agency+MBS', tNotes, ['u.s. treasury, agency debt, and mortgage']);

  // ═══════════════════════════════════════════
  // SECTION 7: 만기별 분포 (Table[6], 8열 매트릭스)
  // 구조: 행=자산유형, 열=만기구간
  // 열순서: [레이블 | Within15d | 16-90d | 91d-1y | 1y-5y | 5y-10y | 10y+ | Total]
  // ═══════════════════════════════════════════
  const matData = {};
  if (tMat) {
    // 헤더 행 파악: "Remaining Maturity" 행의 cells[1..7] = 만기 구간명
    let colHeaders = [];
    let inSection = null;
    let holdingsCount = { treasury: 0, agency: 0 };

    for (let i = 0; i < tMat.length; i++) {
      const cells = tMat[i];
      const lc = cells[0].toLowerCase().replace(/\s+\d+$/, '').trim();

      // 헤더 파악
      if (lc.includes('remaining maturity') && cells.length >= 4) {
        colHeaders = cells.slice(1).map(c => c.trim());
      }
      // 섹션 진입
      if (lc.includes('u.s. treasury securities'))       inSection = 'treasury';
      if (lc.includes('federal agency debt securities') ||
          lc.includes('mortgage-backed securities') && inSection === 'treasury') inSection = 'agency';

      // Holdings 행: 해당 섹션의 보유량
      if (lc === 'holdings' && inSection) {
        const sectionKey = inSection;
        holdingsCount[sectionKey] = (holdingsCount[sectionKey] || 0) + 1;
        // 첫 번째 Holdings = 보유량, 두 번째 Holdings = 다른 항목
        if (holdingsCount[sectionKey] === 1) {
          const MAT_KEYS = ['within_15d','d16_90d','d91_1y','y1_5y','y5_10y','over_10y'];
          const MAT_LABELS = ['15일 이하','16~90일','91일~1년','1~5년','5~10년','10년 초과'];
          cells.slice(1).forEach((cell, ci) => {
            if (ci >= MAT_KEYS.length) return;
            const val = parseNum(cell);
            if (val !== null) {
              matData[`${sectionKey}_${MAT_KEYS[ci]}`] = {
                asset:    sectionKey === 'treasury' ? '미 국채' : 'Agency/MBS',
                maturity: MAT_LABELS[ci],
                colHeader: colHeaders[ci] || MAT_LABELS[ci],
                valueM:  val,
                valueB:  val / 1000,
              };
            }
          });
          // Total 컬럼 (cells[7] if available)
          const totalCell = cells[7];
          if (totalCell) {
            const tot = parseNum(totalCell);
            if (tot !== null) {
              matData[`${sectionKey}_total`] = {
                asset:    sectionKey === 'treasury' ? '미 국채' : 'Agency/MBS',
                maturity: '합계',
                colHeader: '합계',
                valueM:  tot,
                valueB:  tot / 1000,
              };
            }
          }
        }
      }
    }
  }

  // ── 집계: 주요 요약 (대시보드 표시용) ──
  const summary = {
    // 자산
    total_securities:   data.securities_total?.valueB ?? null,
    treasury:           data.treasury_total?.valueB ?? null,
    agency_mbs:         (data.agency_debt?.valueM ?? 0) + (data.mbs_total?.valueM ?? 0),
    // 부채
    reserve_balances:   data.reserve_balances?.valueB ?? null,  // ★ Pipe1 핵심
    rrp:                data.rrp_bs?.valueB ?? data.rrp_total?.valueB ?? null,   // Table[16] Wednesday level 우선
    tga:                data.tga?.valueB ?? data.tga_t2?.valueB ?? null,
    currency:           data.currency_circ?.valueB ?? null,
    // 버퍼 (Pipe1 스코어링용)
    buffer_T:           null,  // 아래서 계산
    releaseDate,
  };
  // buffer_T: Pipe1 스코어링 핵심 — 준비금(WRESBAL 대체, Table[16]) + RRP(Table[16])
  // 단위: Billions USD
  if (summary.reserve_balances && summary.rrp) {
    summary.buffer_T = summary.reserve_balances + summary.rrp;
    summary.buffer_T_score = summary.buffer_T < 2000 ? '🔴 경색' :
                              summary.buffer_T < 2500 ? '🟡 주의' : '🟢 정상';
  }

  // ── 파싱 품질 ──
  const t1Found = Object.keys(data).length;
  const t2Found = Object.keys(matData).length;
  const parseQuality = t1Found >= 10 ? 'good' : t1Found >= 5 ? 'partial' : 'failed';

  // ── 디버그 ──
  const tableSamples = tables.slice(0, 22).map((t, idx) => {
    const rows = extractRows(t);
    const valid = rows.filter(c => c.length >= 2 && c[0].length > 3 && c[1]?.length > 0);
    return {
      tableIndex: idx,
      rowCount: rows.length,
      validRowCount: valid.length,
      sampleRows: valid.slice(0, 8).map(c => ({
        label: c[0].substring(0, 80),
        value: (c[1] || '').substring(0, 20),
        colCount: c.length,
      })),
    };
  }).filter(t => t.validRowCount > 0);

  // ── KV 저장 (성공 시) ──
  const resultPayload = {
    success: true,
    source:  'Federal Reserve H.4.1 HTML Direct Parse',
    url:     FED_URL,
    releaseDate,
    _savedAt: new Date().toISOString(),
    parseQuality,
    summary,
    data,
    maturity: matData,
  };
  if (ctx?.waitUntil) {
    ctx.waitUntil(kvPut(env, KV_KEYS.h41Html, resultPayload, KV_TTL.h41Html));
  } else {
    await kvPut(env, KV_KEYS.h41Html, resultPayload, KV_TTL.h41Html);  // cron 경로: 반드시 await
  }

  return json({
    success: true,
    source:  'Federal Reserve H.4.1 HTML Direct Parse',
    url:     FED_URL,
    releaseDate,
    parseQuality,
    summary,          // ★ 대시보드 핵심값 (Pipe1 스코어링용)
    data,             // 전체 항목 (섹션별)
    maturity: matData,// 만기별 분포
    debug: {
      htmlLength:  html.length,
      tableCount:  tables.length,
      t1FoundCount: t1Found,
      t2FoundCount: t2Found,
      parseQuality,
      tablesIdentified: {
        assets: !!tAssets, liabSum: !!tLiabSum, liabDet: !!tLiabDet,
        mbs: !!tMbs, maturity: !!tMat, memo: !!tMemo, notes: !!tNotes,
      },
      tableSamples,
    },
  });
}


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Yahoo 공용 — 429/5xx 일시 오류 재시도 + 일봉 파싱
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const YAHOO_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Accept': 'application/json',
};
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function yahooFetch(url, cacheTtl, retries = 2) {
  for (let a = 0; ; a++) {
    const resp = await fetch(url, { headers: YAHOO_HEADERS, cf: { cacheTtl } });
    if (resp.ok || a >= retries || !(resp.status === 429 || resp.status >= 500)) return resp;
    await sleep(600 * (a + 1));
  }
}
// Yahoo chart 응답 → { name, currency, dates, closes, volumes, price, prevClose, ... } (일봉)
function parseYahooDaily(data, ticker) {
  const result     = data?.chart?.result?.[0];
  const meta       = result?.meta;
  const timestamps = result?.timestamp || [];
  const closes     = result?.indicators?.quote?.[0]?.close  || [];
  const volumes    = result?.indicators?.quote?.[0]?.volume || [];
  if (!meta || !timestamps.length) return null;
  // null 제거 + 같은 날짜 중복 봉(장중 갱신)은 마지막 것만
  const byDate = new Map();
  timestamps.forEach((ts, i) => {
    const c = closes[i];
    if (c === null || c === undefined) return;
    byDate.set(new Date(ts * 1000).toISOString().slice(0, 10), { close: c, volume: volumes[i] ?? 0 });
  });
  const dates = [...byDate.keys()];
  if (!dates.length) return null;
  const rows  = [...byDate.values()];
  const n     = rows.length;
  const price = meta.regularMarketPrice ?? rows[n - 1].close;
  const prev  = n >= 2 ? rows[n - 2].close : (meta.chartPreviousClose ?? null);   // 직전 거래일 종가
  return {
    ticker,
    name:      meta.shortName || ticker,
    currency:  meta.currency || '',
    price,
    prevClose: prev,
    delta:     (price != null && prev) ? price - prev : null,
    pctChange: (price != null && prev) ? (price - prev) / prev * 100 : null,
    date:      dates[n - 1],
    marketTime: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toISOString() : null,
    dates,
    closes:    rows.map(r => parseFloat(r.close.toFixed(4))),
    volumes:   rows.map(r => r.volume),
  };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 시세 스냅샷 — cron이 Yahoo를 순차(동시 4건) 조회해 KV에 저장, 사이트는 /market 한 번만 읽음
//   종목별로 실패하면 직전 정상 값 유지(fetchedAt으로 신선도 판정). 전부 실패하면 KV를 건드리지 않음.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const MARKET_TICKERS = [
  // 앞 절반 = 갱신 시각 :25에도 갱신되는 우선 종목 (T0 게이지·비율·T1 원자재 주요)
  '^VIX', '^MOVE', 'SPY', 'TLT', 'QQQ', 'DIA', 'GC=F', 'HG=F', 'BTC-USD',
  'CL=F', 'BZ=F', 'NG=F', 'SI=F', 'TQQQ', 'SQQQ', 'UVXY', 'URA',
  // 뒤 절반
  'XLK', 'XLV', 'XLF', 'XLY', 'XLP', 'XLE', 'XLU', 'XLI', 'XLB', 'XLRE', 'XLC',
  'ALI=F', 'ZW=F', 'ZS=F', 'LBR=F', 'BDRY',
];
const MARKET_HALF  = 17;
const MARKET_RANGE = '3mo';   // 사이트 초기 화면은 3mo·1mo만 사용 (그 외 기간 버튼은 /yahoo-chart 실시간)

// budget = 이번 실행에서 쓸 Yahoo 요청 상한 (같은 cron 실행의 다른 작업과 합쳐 서브요청 50건을 넘지 않게)
async function refreshMarket(env, tickers, budget = tickers.length + 8) {
  const prev = (await kvGet(env, KV_KEYS.market)) || {};
  const out  = { ...(prev.tickers || {}) };
  let ok = 0, changed = false, used = 0;
  const queue = tickers.map(t => ({ t, tries: 0 }));
  const worker = async () => {
    while (queue.length && used < budget) {
      const job = queue.shift(), t = job.t;
      try {
        // URL을 /yahoo-chart(1h 엣지 캐시)와 다르게 → 오래된 엣지 캐시를 받지 않도록
        const url  = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(t)}?interval=1d&range=${MARKET_RANGE}&includePrePost=false`;
        used++;
        const resp = await yahooFetch(url, 240, 0);
        if (resp.status === 429 || resp.status >= 500) {
          if (job.tries++ < 1) { await sleep(800); queue.push(job); continue; }   // 일시 오류 → 맨 뒤에서 1회 재시도
        }
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const parsed = parseYahooDaily(await resp.json(), t);
        if (!parsed) throw new Error('빈 응답');
        const old = out[t];
        if (!old || old.price !== parsed.price || old.date !== parsed.date) changed = true;
        out[t] = { ...parsed, fetchedAt: new Date().toISOString() };
        ok++;
      } catch (e) {
        console.error('market', t, e.message);
      }
    }
  };
  await Promise.all([worker(), worker(), worker(), worker()]);
  if (!ok) throw new Error(`시세 전부 실패 (${tickers.length}종목) — 기존 스냅샷 유지`);
  // 장 마감 후 등 값이 그대로면 KV 쓰기 생략 (단 1시간에 한 번은 기록해 checkedAt 갱신)
  const age = Date.now() - new Date(prev._savedAt || 0).getTime();
  if (!changed && age < 3600 * 1000) return prev;
  for (const k of Object.keys(out)) if (!MARKET_TICKERS.includes(k)) delete out[k];   // 목록에서 빠진 종목 정리
  return kvPut(env, KV_KEYS.market, { tickers: out, range: MARKET_RANGE, checkedAt: new Date().toISOString() }, KV_TTL.market);
}

let _marketInitAt = 0;
async function marketSnapshot(env, ctx) {
  const snap = await kvGet(env, KV_KEYS.market);
  if (snap?.tickers) return new Response(JSON.stringify(snap), { headers: CORS });   // 압축 없이 들여쓰면 3배 커짐
  // 최초 배포 직후 등 스냅샷이 없을 때: 비어 있다고 알리고 백그라운드로 1회 생성 (사이트는 종목별 실시간 경로로 대체)
  if (ctx?.waitUntil && Date.now() - _marketInitAt > 5 * 60 * 1000) {   // 같은 isolate에서 5분에 한 번만
    _marketInitAt = Date.now();
    ctx.waitUntil(refreshMarket(env, MARKET_TICKERS).catch(e => console.error('market init', e.message)));
  }
  return json({ tickers: {}, missing: true });
}

async function yahooProxy(url) {
  const raw = url.searchParams.get('tickers') || '';
  const tickers = raw.split(',')
    .map(s => s.trim().replace(/%3D/gi, '='))  // GC%3DF → GC=F
    .filter(Boolean);
  if (!tickers.length) return json({ error: 'tickers parameter required' }, 400);
  if (tickers.length > 15) return json({ error: 'Max 15 tickers per request' }, 400);

  const results = await Promise.allSettled(
    tickers.map(async (ticker) => {
      // Yahoo Finance v8 crumbless endpoint (공개 quote)
      // interval=1m + range=1d → meta에 regularMarketChangePercent 포함
      const yahooUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1m&range=1d`;
      const resp = await yahooFetch(yahooUrl, 60, 1); // 1분 캐시 (장중 실시간), 429/5xx 1회 재시도
      if (!resp.ok) throw new Error(`Yahoo HTTP ${resp.status}`);
      const data = await resp.json();

      const meta   = data?.chart?.result?.[0]?.meta;
      const quotes = data?.chart?.result?.[0]?.indicators?.quote?.[0];
      const timestamps = data?.chart?.result?.[0]?.timestamp || [];

      if (!meta) throw new Error('No meta data');

      const closes = quotes?.close || [];
      // 마지막 유효값
      let latest = null, prev = null;
      for (let i = closes.length - 1; i >= 0; i--) {
        if (closes[i] !== null) { if (latest === null) latest = closes[i]; else if (prev === null) { prev = closes[i]; break; } }
      }

      // regularMarketChangePercent: meta에 있으면 사용 (장중 실시간)
      const regularPct = meta.regularMarketChangePercent ?? null;
      const regularPrice = meta.regularMarketPrice ?? latest;

      return {
        ticker,
        price:      regularPrice ?? latest,
        prevClose:  prev,
        delta:      (regularPrice && prev) ? regularPrice - prev : null,
        pctChange:  regularPct !== null ? regularPct : ((latest !== null && prev !== null) ? ((latest - prev) / prev) * 100 : null),
        currency:   meta.currency,
        name:       meta.shortName || ticker,
        date:       timestamps.length ? new Date(timestamps[timestamps.length-1] * 1000).toISOString().slice(0,10) : null,
      };
    })
  );

  const output = {};
  results.forEach((r, i) => {
    output[tickers[i]] = r.status === 'fulfilled'
      ? r.value
      : { ticker: tickers[i], error: r.reason?.message, price: null };
  });

  return json({ success: true, quotes: output, count: tickers.length });
}


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Yahoo Finance 차트 히스토리
//    GET /yahoo-chart?ticker=^VIX&range=1y
//    GET /yahoo-chart?ticker=^MOVE&range=6mo
//
//    range 옵션: 1mo 3mo 6mo 1y 2y 5y
//    interval: 1d (일간 고정)
//    응답: { ticker, dates[], closes[], meta }
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function yahooChart(url) {
  const ticker = url.searchParams.get('ticker') || '^VIX';
  const range  = url.searchParams.get('range')  || '1y';

  // 허용 range 값
  const ALLOWED = ['1mo','3mo','6mo','1y','2y','5y'];
  const safeRange = ALLOWED.includes(range) ? range : '1y';

  const chartUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}`
    + `?interval=1d&range=${safeRange}`;

  const resp = await yahooFetch(chartUrl, 3600); // 1h 캐시, 429/5xx 재시도

  if (!resp.ok) {
    return json({ error: `Yahoo chart HTTP ${resp.status}`, ticker, range: safeRange }, 502);
  }

  const data = await resp.json();
  const result     = data?.chart?.result?.[0];
  const meta       = result?.meta;
  const timestamps = result?.timestamp || [];
  const closes     = result?.indicators?.quote?.[0]?.close   || [];
  const volumes    = result?.indicators?.quote?.[0]?.volume  || [];

  if (!meta || !timestamps.length) {
    return json({ error: 'No chart data', ticker, range: safeRange }, 404);
  }

  // null 제거 + 날짜 포맷, volume 포함
  const pairs = timestamps
    .map((ts, i) => ({
      date:   new Date(ts * 1000).toISOString().slice(0,10),
      close:  closes[i],
      volume: volumes[i] ?? 0,
    }))
    .filter(p => p.close !== null && p.close !== undefined);

  return json({
    ticker,
    range:    safeRange,
    name:     meta.shortName || ticker,
    currency: meta.currency || '',
    count:    pairs.length,
    dates:    pairs.map(p => p.date),
    closes:   pairs.map(p => parseFloat(p.close.toFixed(2))),
    volumes:  pairs.map(p => p.volume),  // 자금 흐름 계산용
  });
}


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PIPE 2-A: DXY 모멘텀 (Wrecking Ball)
//    GET /dxy
//    Yahoo Finance DX-Y.NYB 1년치 일봉 → 50/200 DMA 계산
//    응답: currentPrice, dma50, dma200, spread50, spread200,
//          crossSignal ("GOLDEN"|"DEAD"|null), dmaGap, dates, closes
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function dxyAnalysis() {
  const TICKER = 'DX-Y.NYB';
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(TICKER)}`
    + `?interval=1d&range=1y`;

  const resp = await yahooFetch(url, 3600);

  if (!resp.ok) return json({ error: `Yahoo DXY HTTP ${resp.status}` }, 502);

  const data      = await resp.json();
  const result    = data?.chart?.result?.[0];
  const meta      = result?.meta;
  const tss       = result?.timestamp || [];
  const rawCloses = result?.indicators?.quote?.[0]?.close || [];

  if (!meta || !tss.length) return json({ error: 'DXY 데이터 없음' }, 404);

  // null 제거 + 날짜 정렬
  const pairs = tss
    .map((ts, i) => ({
      date:  new Date(ts * 1000).toISOString().slice(0, 10),
      close: rawCloses[i],
    }))
    .filter(p => p.close != null);

  const closes = pairs.map(p => p.close);
  const dates  = pairs.map(p => p.date);
  const n      = closes.length;

  // 이동평균 계산 헬퍼
  const sma = (arr, period) => {
    if (arr.length < period) return null;
    const slice = arr.slice(arr.length - period);
    return slice.reduce((a, b) => a + b, 0) / period;
  };

  const currentPrice = closes[n - 1];
  const dma50  = n >= 50  ? sma(closes, 50)  : null;
  const dma200 = n >= 200 ? sma(closes, 200) : null;

  // 이격도 (현재가 기준 %)
  const spread50  = (currentPrice != null && dma50  != null) ? ((currentPrice - dma50)  / dma50  * 100) : null;
  const spread200 = (currentPrice != null && dma200 != null) ? ((currentPrice - dma200) / dma200 * 100) : null;

  // 50/200 DMA 간 이격도 (크로스 선행 지표)
  // 양수: 50DMA > 200DMA (골든 방향), 음수: 50DMA < 200DMA (데드 방향)
  const dmaGap = (dma50 != null && dma200 != null) ? ((dma50 - dma200) / dma200 * 100) : null;

  // 크로스 신호: 전일 50DMA vs 200DMA로 전환 여부 판별
  let crossSignal = null;
  if (n >= 201 && dma200 != null) {
    const dma50prev  = sma(closes.slice(0, n - 1), 50);
    const dma200prev = sma(closes.slice(0, n - 1), 200);
    if (dma50prev != null && dma200prev != null) {
      const prevAbove = dma50prev > dma200prev;
      const currAbove = dma50  > dma200;
      if (!prevAbove && currAbove) crossSignal = 'GOLDEN';
      else if (prevAbove && !currAbove) crossSignal = 'DEAD';
    }
  }

  const fmt2 = v => v != null ? +v.toFixed(4) : null;

  return json({
    source:       'Yahoo Finance (DX-Y.NYB)',
    asOf:         dates[n - 1],
    currentPrice: fmt2(currentPrice),
    dma50:        fmt2(dma50),
    dma200:       fmt2(dma200),
    spread50:     dma50  != null ? +spread50.toFixed(3)  : null,  // % (현재가 vs 50DMA)
    spread200:    dma200 != null ? +spread200.toFixed(3) : null,  // % (현재가 vs 200DMA)
    dmaGap:       dmaGap != null ? +dmaGap.toFixed(3)   : null,  // % (50DMA vs 200DMA, 크로스 선행)
    crossSignal,          // "GOLDEN" | "DEAD" | null
    dataPoints:   n,
    // 최근 30일 시계열 (차트용)
    recent: {
      dates:  dates.slice(-30),
      closes: closes.slice(-30).map(v => +v.toFixed(4)),
    },
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PIPE 2-B: 엔캐리 청산 리스크
//    GET /jpy
//    Yahoo Finance JPY=X 단일 티커 (^JYVIX 제거 — Yahoo 데이터 중단)
//
//    산출 지표:
//    ① 5일 변화율 (5d RoC): 방향성 마진콜 신호
//    ② 10일 역사적 변동성 (10d HV, 연율화): JYVIX 대체 지표
//       - 최근 10영업일 일일 수익률(ln 수익률) 표준편차 × sqrt(252) × 100
//       - 임계값: ≥20% → 마진콜 구간 / ≥15% → 감시 구간 (구 JYVIX 기준 그대로 유지)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function jpyCarryRisk() {
  // JPY=X 단독 fetch — 1개월치로 10d HV 계산에 충분한 데이터 확보
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent('JPY=X')}`
    + `?interval=1d&range=1mo`;

  const r = await yahooFetch(url, 3600);
  if (!r.ok) return json({ error: `Yahoo JPY=X HTTP ${r.status}` }, 502);

  const d      = await r.json();
  const result = d?.chart?.result?.[0];
  if (!result)  return json({ error: 'JPY=X 데이터 없음' }, 502);

  const tss    = result.timestamp || [];
  const closes = result.indicators?.quote?.[0]?.close || [];
  const jpyPairs = tss
    .map((ts, i) => ({ date: new Date(ts * 1000).toISOString().slice(0, 10), close: closes[i] }))
    .filter(p => p.close != null);

  if (jpyPairs.length < 6) return json({ error: 'JPY=X 데이터 부족 (최소 6일 필요)' }, 502);

  const jpyCloses = jpyPairs.map(p => p.close);
  const jpyN      = jpyCloses.length;

  // ── ① 5일 변화율 (방향성 신호)
  const jpyCurrent = jpyCloses[jpyN - 1];
  const jpy5dAgo   = jpyCloses[Math.max(0, jpyN - 6)];
  const jpyRoC5d   = jpy5dAgo != null ? (jpyCurrent - jpy5dAgo) / jpy5dAgo * 100 : null;

  // ── ② 10일 역사적 변동성 (HV) — JYVIX 대체
  // 수식: HV_10d = σ(ln_returns_10d) × sqrt(252) × 100  [%]
  // ln 수익률이 정규분포에 더 가깝고 복리 효과 반영
  const HV_WINDOW = 10;
  let jpyHV10d = null;
  if (jpyN >= HV_WINDOW + 1) {
    // 최근 10개의 ln 일일 수익률 계산 (10영업일 = 11개 종가 필요)
    const recentCloses = jpyCloses.slice(-(HV_WINDOW + 1));
    const lnReturns = [];
    for (let i = 1; i < recentCloses.length; i++) {
      if (recentCloses[i] > 0 && recentCloses[i - 1] > 0) {
        lnReturns.push(Math.log(recentCloses[i] / recentCloses[i - 1]));
      }
    }
    if (lnReturns.length >= 4) {
      const mu  = lnReturns.reduce((a, b) => a + b, 0) / lnReturns.length;
      const va  = lnReturns.map(x => (x - mu) ** 2).reduce((a, b) => a + b, 0) / (lnReturns.length - 1);
      const std = Math.sqrt(va);
      jpyHV10d  = +(std * Math.sqrt(252) * 100).toFixed(3); // 연율화, %
    }
  }

  // ── 마진콜 경보 판정
  // 기존 JYVIX 임계값을 HV로 대체: JYVIX 20 → HV 20%, JYVIX 15 → HV 15%
  const marginCallWarning = jpyRoC5d != null && jpyRoC5d <= -3.0;  // RoC 기준 (HV 독립)

  // signal: RoC와 HV 중 더 심각한 쪽 기준
  const rocSignal  = jpyRoC5d != null
    ? (jpyRoC5d <= -3.0 ? 'WARNING' : jpyRoC5d <= -1.5 ? 'WATCH' : 'NORMAL')
    : 'NORMAL';
  const hvSignal   = jpyHV10d != null
    ? (jpyHV10d >= 20 ? 'WARNING' : jpyHV10d >= 15 ? 'WATCH' : 'NORMAL')
    : 'NORMAL';
  const signalPri  = { WARNING: 2, WATCH: 1, NORMAL: 0 };
  const signal     = signalPri[rocSignal] >= signalPri[hvSignal] ? rocSignal : hvSignal;

  const fmt3 = v => v != null ? +v.toFixed(3) : null;

  return json({
    source:  'Yahoo Finance (JPY=X 단독 — ^JYVIX 데이터 중단으로 10d HV로 대체)',
    asOf:    jpyPairs[jpyN - 1]?.date,
    jpy: {
      current:     fmt3(jpyCurrent),
      fiveDayAgo:  fmt3(jpy5dAgo),
      roc5d:       jpyRoC5d != null ? +jpyRoC5d.toFixed(3) : null,
      note:        'USD/JPY. 하락(음수 RoC) = 엔 강세 = 마진콜 방향',
    },
    jpyHV: {
      hv10d:       jpyHV10d,           // 연율화 역사적 변동성 %, 10영업일
      window:      HV_WINDOW,
      method:      'σ(ln returns) × √252 × 100',
      signal:      hvSignal,
      note:        'JPY 10d HV (JYVIX 대체). ≥20% → 마진콜 / ≥15% → 감시',
    },
    marginCallWarning,
    signal,   // WARNING | WATCH | NORMAL (RoC와 HV 중 더 심각한 쪽)
    recent: {
      dates:     jpyPairs.slice(-20).map(p => p.date),
      jpyCloses: jpyPairs.slice(-20).map(p => fmt3(p.close)),
    },
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PIPE 2-C: 금융 스트레스 지수 — FRED API 기반 (OFR FSI 2021년 폐지로 대체)
//    GET /ofr-fsi
//    데이터 소스: FRED API (기존 Worker FRED 프록시 재활용)
//      ① STLFSI4  — St. Louis Fed Financial Stress Index (주간)
//                   전체 시스템 스트레스의 가장 신뢰도 높은 공개 대리 지표
//      ② NFCI     — Chicago Fed National Financial Conditions Index (주간)
//                   자금 조달 포함 전반적 금융여건
//      ③ NFCICREDIT — NFCI Credit Subindex (주간)
//                     신용/자금 조달 스트레스 서브인덱스 (NFCI 구성요소)
//    신호 기준 (3개 동일): > 1.5 위험 / 0~1.5 경계 / < 0 안정
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function ofrFsi(env) {
  const apiKey  = env?.FRED_API_KEY;
  if (!apiKey) return json({ error: 'FRED_API_KEY 환경변수 없음' }, 500);

  const FRED_BASE = 'https://api.stlouisfed.org/fred/series/observations';

  // FRED 시리즈 3개 병렬 fetch (limit=5: 주간 → 최근 5주치)
  const fetchFred = async seriesId => {
    const url = `${FRED_BASE}?series_id=${seriesId}&api_key=${apiKey}`
      + `&file_type=json&limit=5&sort_order=desc`;
    try {
      const r = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return null;
      const d = await r.json();
      return (d.observations || []).filter(o => o.value !== '.');
    } catch(e) { return null; }
  };

  const [stlObs, nfciObs, creditObs] = await Promise.all([
    fetchFred('STLFSI4'),    // St. Louis Fed FSI (주간)
    fetchFred('NFCI'),       // Chicago Fed NFCI (주간)
    fetchFred('NFCICREDIT'), // NFCI Credit Sub-index (주간)
  ]);

  // 최신값 / 전주 대비 Δ 추출
  const cur  = obs => obs?.length > 0 ? +parseFloat(obs[0].value).toFixed(4) : null;
  const prv  = obs => obs?.length > 1 ? +parseFloat(obs[1].value).toFixed(4) : null;
  const dlt  = (c, p) => (c != null && p != null) ? +(c - p).toFixed(4) : null;
  const asOf = stlObs?.[0]?.date ?? null;

  const stlCur   = cur(stlObs),   stlPrv   = prv(stlObs);
  const nfciCur  = cur(nfciObs),  nfciPrv  = prv(nfciObs);
  const crdtCur  = cur(creditObs),crdtPrv  = prv(creditObs);

  // ── 시리즈별 임계값 판별 ──
  // STLFSI4: 0 = 장기 평균. 양수 = 스트레스 발생
  //   정상: ≤ 0 / 주의: 0~1.0 / 위험: > 1.0
  const sigStl = v => v == null ? 'g' : v > 1.0  ? 'r' : v > 0    ? 'a' : 'g';
  const lblStl = v => sigStl(v) === 'r' ? '위험' : sigStl(v) === 'a' ? '주의' : '정상';

  // NFCI / NFCICREDIT: 양수 = 긴축(위험), 음수 = 완화(정상)
  //   정상: ≤ -0.3 / 주의: -0.3~0 / 위험: > 0
  const sigNfc = v => v == null ? 'g' : v > 0    ? 'r' : v > -0.3  ? 'a' : 'g';
  const lblNfc = v => sigNfc(v) === 'r' ? '위험' : sigNfc(v) === 'a' ? '주의' : '정상';

  // 전체 최악 신호
  const sigPri  = {r:2, a:1, g:0};
  const sigs    = [sigStl(stlCur), sigNfc(nfciCur), sigNfc(crdtCur)];
  const worstSig = sigs.reduce((w, s) => sigPri[s] > sigPri[w] ? s : w, 'g');

  // 입체 해석
  const interpretation =
    (sigStl(stlCur) === 'r' && sigNfc(nfciCur) === 'r') ? 'SYSTEMIC_CRISIS'   // STLFSI4 + NFCI 동시 위험
    : (sigStl(stlCur) === 'r')                          ? 'STL_STRESS'         // 전체 스트레스 단독
    : (sigNfc(nfciCur) === 'r' || sigNfc(crdtCur) === 'r') ? 'FUNDING_STRESS'  // 자금조달/신용 경색
    : (worstSig === 'a')                                ? 'WATCH'
    : 'NORMAL';

  return json({
    source:     'FRED API (STLFSI4 + NFCI + NFCICREDIT) — OFR FSI 2021년 폐지로 대체',
    asOf,
    dataPoints: stlObs?.length ?? 0,
    note:       '주간 발표. STLFSI4: >1.0위험/>0주의/≤0정상 | NFCI/Credit: >0위험/>-0.3주의/≤-0.3정상',
    total: {
      label:   '전체 금융 스트레스 (STLFSI4)',
      current: stlCur,
      deltaWoW: dlt(stlCur, stlPrv),
      signal:  sigStl(stlCur),
      level:   lblStl(stlCur),
    },
    funding: {
      label:   '국가 금융 여건 (NFCI)',
      current: nfciCur,
      deltaWoW: dlt(nfciCur, nfciPrv),
      signal:  sigNfc(nfciCur),
      level:   lblNfc(nfciCur),
    },
    otherAdv: {
      label:   '신용 경색 (NFCI Credit)',
      current: crdtCur,
      deltaWoW: dlt(crdtCur, crdtPrv),
      signal:  sigNfc(crdtCur),
      level:   lblNfc(crdtCur),
    },
    worstSignal: worstSig,
    interpretation,
    recent: { dates: [], total: [], funding: [], otherAdv: [] },
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 국채 수익률 히스토리 (DGS2/5/10/30)
//    yieldsHistCached → yieldsHistory
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function yieldsHistory(env) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return json({ error: 'FRED_API_KEY 없음' }, 500);

  const fredSeries = async (id) => {
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&sort_order=desc&limit=252`;
    try {
      const r = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return {};
      const d = await r.json();
      const map = {};
      for (const o of (d.observations || [])) {
        if (o.value !== '.') map[o.date] = parseFloat(o.value);
      }
      return map;
    } catch(e) { return {}; }
  };

  // 4개 시리즈 병렬 fetch
  const [m2, m5, m10, m30] = await Promise.all([
    fredSeries('DGS2'),
    fredSeries('DGS5'),
    fredSeries('DGS10'),
    fredSeries('DGS30'),
  ]);

  // DGS10 날짜를 앵커로 정렬 (가장 완전한 시리즈)
  const labels = Object.keys(m10).sort();

  return json({
    labels,
    dgs2:  labels.map(d => m2[d]  ?? null),
    dgs5:  labels.map(d => m5[d]  ?? null),
    dgs10: labels.map(d => m10[d] ?? null),
    dgs30: labels.map(d => m30[d] ?? null),
    count: labels.length,
    asOf:  new Date().toISOString().slice(0, 10),
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 유동성 & 시장 탭 — FRED 통합 데이터
//    GET /liq
//    16개 FRED 시리즈 병렬 호출 후 구조화 JSON 반환
//    금리·스프레드·물가·MMF·예금 포함
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function liqDataEndpoint(env) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return json({ error: 'FRED_API_KEY 없음' }, 500);

  // FRED 관측치 배열 fetch 헬퍼 (. 결측 필터링)
  const fredArr = async (id, limit) => {
    try {
      const u = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&limit=${limit}&sort_order=desc`;
      const r = await fetch(u, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return [];
      const d = await r.json();
      return (d.observations || [])
        .filter(o => o.value !== '.')
        .map(o => ({ date: o.date, value: parseFloat(o.value), released: o.realtime_start ?? null }));
    } catch(e) { return []; }
  };

  // 병렬 fetch — FRED 시리즈 + H.4.1 + DTS TGA
  // ※ MMF Institutional(WRMFSL): Federal Reserve가 2021-02 이후 발표 중단 → ICI 데이터로 대체 (별도 /ici)
  const [
    mmfR, bankDep,                     // 유동성 흐름 (Institutional은 /ici에서)
    dgs2, dgs5, dgs10, dgs30,          // 국채 수익률 (일별)
    sp2_10, sp3m_10,                   // 스프레드 5년 히스토리
    cpi, coreCpi, ppi, pce,            // 물가 지수 15개월
    corePce, wages,                    // Core PCE + 임금 (CES0500000003)
    mich, bei10, t5y5y, tips10,        // 기대인플레이션 + 실질금리
    rrpSeries, sofrVolSeries,          // Repo Regime Matrix
    h41Data, tgaDtsData,               // NLM: H.4.1 총자산 + DTS 일별 TGA
  ] = await Promise.all([
    fredArr('WRMFNS',          12),  // 소매 MMF 주간 (기관은 ICI 사용)
    fredArr('DPSACBW027SBOG',  12),  // 상업은행 총 예금 주간
    fredArr('DGS2',            30),  // 30일치 → 20영업일 전(1M ago) 확보
    fredArr('DGS5',            30),
    fredArr('DGS10',           30),
    fredArr('DGS30',           30),
    fredArr('T10Y2Y',        1300),  // 2s10s 스프레드 5년치 (스파크라인용)
    fredArr('T10Y3M',        1300),  // 3m10y 스프레드 5년치
    fredArr('CPIAUCSL',        15),  // CPI 월별 (prevYoY 위해 15개)
    fredArr('CPILFESL',        15),  // Core CPI
    fredArr('PPIFIS',          15),  // PPI (Final Demand — 헤드라인 PPI, SA)
    fredArr('PCEPI',           15),  // PCE
    fredArr('PCEPILFE',        15),  // Core PCE (연준 공식 목표 지표)
    fredArr('CES0500000003',   15),  // 시간당 평균 임금 (민간 전체, 현행 시리즈)
    fredArr('MICH',             3),  // 미시간 1Y 기대인플레이션
    fredArr('T10YIE',          25),  // BEI 10Y — 25일치 (1M Δ 계산용)
    fredArr('T5YIFR',          25),  // 5Y5Y Forward — 25일치
    fredArr('DFII10',           3),  // TIPS 10Y 실질금리
    fredArr('RRPONTSYD',       30),  // ON RRP 잔고 일별 (NLM + Regime 공용, 30일치)
    fredArr('SOFRVOL',         10),  // SOFR 거래량 일별 ($B)
    // H.4.1 reserve_credit (총자산) — KV 캐시 → 없으면 직접 파싱
    (async () => {
      try {
        // 1) KV 캐시 확인
        let cached = await kvGet(env, KV_KEYS.h41Html);
        // 2) 캐시 없거나 데이터 없으면 h41HtmlParser 호출로 갱신
        if (!cached?.data?.reserve_credit?.val) {
          const resp = await h41HtmlParser(env, null, true);
          cached = await resp.json();
        }
        if (!cached?.data?.reserve_credit?.val) return null;
        const valB = +(cached.data.reserve_credit.val / 1000).toFixed(2);
        const date = cached.releaseDate
          ? new Date(cached.releaseDate).toISOString().slice(0, 10)
          : cached._savedAt?.slice(0, 10) ?? null;
        const tgaVal = cached.data.tga?.val ?? cached.data.tga_t2?.val;   // DTS 결측 시 대체용
        return { value: valB, date, tgaB: tgaVal != null ? +(tgaVal / 1000).toFixed(2) : null };
      } catch { return null; }
    })(),
    // DTS 일별 TGA — KV에서 읽기 (GitHub Actions가 저장)
    kvGet(env, KV_KEYS.tgaDts).catch(() => null),
  ]);

  // ── MMF 시계열 (Retail만 — Institutional은 ICI에서 별도 조회) ──
  // ※ WRMFSL은 Fed가 2021-02 이후 발표 중단. 인덱스 기반 합산은 ICI 사용.
  const mmfSeries = mmfR
    .slice(0, 12)
    .map(d => ({ date: d.date, retail: d.value, total: d.value }));

  // ── 단순 요약 헬퍼 ──
  const cur  = a => a[0]?.value ?? null;
  const prv  = a => a[1]?.value ?? null;
  const dt   = a => cur(a) != null && prv(a) != null ? +(cur(a) - prv(a)).toFixed(4) : null;
  const asOf = a => a[0]?.date ?? null;
  const summ = (a, decPct=false) => ({
    current: cur(a), prev: prv(a), delta: dt(a), asOf: asOf(a),
    // 1개월 Δ: 20영업일 전 (일별 시리즈) — bp 단위 (* 100)
    delta1m: (a[0]?.value != null && a[20]?.value != null)
      ? +((a[0].value - a[20].value) * 100).toFixed(1) : null,
    ago1m: a[20]?.value ?? null,
    // YoY: 12개월 전 대비 (index 기반)
    yoy: (a[0]?.value != null && a[12]?.value != null && a[12].value > 0)
      ? +((a[0].value - a[12].value) / a[12].value * 100).toFixed(2)
      : null,
  });

  // ── Net Liquidity Momentum 계산 ──
  // NL = 연준 총자산(H.4.1 reserve_credit, 주간) − TGA(DTS 일별) − ON RRP(FRED 일별)
  // 기준점: 최신 RRP 관측일부터 7일 간격 6개 → current = 0주, 4주 평균 = 1~4주 전 (실제 주 단위)
  // TGA: 기준일 이전 가장 가까운 DTS 값. DTS 키가 비었으면 H.4.1 TGA로 대체 (전체 null 방지)
  const walclB = h41Data?.value ?? null;          // H.4.1 단일값 $B
  const dtsSeries = tgaDtsData?.series || [];     // [{date, close}] 오름차순, $B
  const tgaDailyDesc = dtsSeries.slice().reverse().map(d => ({ date: d.date, value: d.close }));
  const shiftDays = (iso, n) => new Date(Date.parse(iso) - n * 86400000).toISOString().slice(0, 10);

  const nlTimeSeries = [];
  let tgaFallbackUsed = false;
  if (walclB != null && rrpSeries.length) {
    for (let w = 0; w < 6; w++) {
      const anchorDate = shiftDays(rrpSeries[0].date, w * 7);
      const rrpEntry = rrpSeries.find(r => r.date <= anchorDate && r.value != null);
      if (!rrpEntry) continue;
      let tgaB = tgaDailyDesc.find(r => r.date <= anchorDate)?.value ?? null;
      if (tgaB == null && w === 0 && h41Data?.tgaB != null) { tgaB = h41Data.tgaB; tgaFallbackUsed = true; }
      if (tgaB == null) continue;
      nlTimeSeries.push({
        date:   anchorDate,
        walcl:  +walclB.toFixed(2),
        tga:    +tgaB.toFixed(2),
        rrp:    +rrpEntry.value.toFixed(2),
        nl:     +(walclB - tgaB - rrpEntry.value).toFixed(2),
      });
    }
  }

  // 현재 NL + 직전 4주 평균 + Momentum Score
  const nlCurrent = nlTimeSeries[0]?.nl ?? null;
  const prior = nlTimeSeries.slice(1, 5);
  const nl4wAvg   = prior.length >= 3
    ? +(prior.reduce((s, d) => s + d.nl, 0) / prior.length).toFixed(2)
    : null;
  const nlScore   = (nlCurrent != null && nl4wAvg != null && Math.abs(nl4wAvg) > 1)
    ? +((nlCurrent - nl4wAvg) / Math.abs(nl4wAvg) * 100).toFixed(2)
    : null;
  const nlRegime  = nlScore == null ? null : nlScore > 0 ? 'risk_on' : 'risk_off';

  // ── Repo Regime Matrix 계산 ──
  // 각 시리즈 독립적으로 결측 제거 후 5영업일 Δ 계산
  // (날짜 불일치 가능 → asOf를 각각 별도 명시)
  const rrpDelta5d     = rrpSeries.length >= 5
    ? +(rrpSeries[0].value - rrpSeries[4].value).toFixed(2) : null;
  const sofrVolDelta5d = sofrVolSeries.length >= 5
    ? +(sofrVolSeries[0].value - sofrVolSeries[4].value).toFixed(2) : null;

  // 국면 판별 (서버사이드)
  let regime = null;
  if (rrpDelta5d != null) {
    if (rrpDelta5d > 0) {
      regime = 'contraction';   // 🔴 유동성 파킹
    } else if (sofrVolDelta5d != null && sofrVolDelta5d > 0) {
      regime = 'expansion';     // 🟢 정상 유입
    } else {
      regime = 'divergence';    // 🟡 분산 및 대기
    }
  }

    // 국채 수익률 — ago1m: 20영업일 전 값 (1개월 전 곡선 오버레이용)
    const yieldSumm = (a) => ({
      current: cur(a), prev: prv(a), delta: dt(a), asOf: asOf(a),
      ago1m:     a[19]?.value ?? null,   // 20번째 = 약 20영업일 전
      ago1mDate: a[19]?.date  ?? null,
    });
    // YoY 계산 헬퍼 (월간 지표: index 0=최신, index 12=1년 전)
    const yoy = (arr) => {
      const cur    = arr[0]?.value  ?? null;
      const yr     = arr[12]?.value ?? null;
      const prev   = arr[1]?.value  ?? null;
      const yrPrev = arr[13]?.value ?? null;
      const curYoY  = (cur  != null && yr     != null && yr     > 0) ? +((cur  - yr)     / yr     * 100).toFixed(2) : null;
      const prevYoY = (prev != null && yrPrev != null && yrPrev > 0) ? +((prev - yrPrev) / yrPrev * 100).toFixed(2) : null;
      return { current: cur, prev, curYoY, prevYoY, asOf: arr[0]?.date ?? null, released: arr[0]?.released ?? null };
    };

    return json({
    asOf: new Date().toISOString().slice(0, 10),
    // 유동성 흐름
    mmf: {
      // ※ Institutional은 FRED WRMFSL 폐지(2021-02). ICI(/ici) 데이터 사용 권장.
      series: mmfSeries,
      retail: summ(mmfR),
      total: {
        current: cur(mmfR),
        prev:    prv(mmfR),
        delta:   dt(mmfR),
        asOf:    asOf(mmfR),
        note:    'retail only (institutional은 /ici 사용)',
      },
    },
    bankDeposits: { series: bankDep, ...summ(bankDep) },
    // Net Liquidity Momentum
    nlMomentum: {
      currentNL:  nlCurrent,
      avg4wNL:    nl4wAvg,
      score:      nlScore,
      regime:     nlRegime,
      walcl:      nlTimeSeries[0]?.walcl  ?? null,
      tga:        nlTimeSeries[0]?.tga    ?? null,
      rrp:        nlTimeSeries[0]?.rrp    ?? null,
      series:     nlTimeSeries,
      asOf:       nlTimeSeries[0]?.date   ?? null,
      sources: {
        walcl: h41Data?.value ? `H.4.1 reserve_credit (${h41Data.date ?? '?'})` : 'unavailable',
        tga:   tgaFallbackUsed ? `H.4.1 (DTS 결측 대체)` : tgaDailyDesc.length ? `DTS daily (${tgaDailyDesc[0]?.date ?? '?'})` : 'unavailable',
        rrp:   'FRED RRPONTSYD daily',
      },
    },
    // Repo Regime Matrix
    repoRegime: {
      regime,
      rrpDelta5d,
      sofrVolDelta5d,
      rrpCurrent:     rrpSeries[0]?.value    ?? null,
      sofrVolCurrent: sofrVolSeries[0]?.value ?? null,
      rrpAsOf:        rrpSeries[0]?.date     ?? null,
      sofrVolAsOf:    sofrVolSeries[0]?.date ?? null,
    },
    // 국채 수익률 (ago1m 포함)
    yields: {
      dgs2:  yieldSumm(dgs2),
      dgs5:  yieldSumm(dgs5),
      dgs10: yieldSumm(dgs10),
      dgs30: yieldSumm(dgs30),
    },
    // 스프레드 (역전 판단용 전체 히스토리 포함)
    spreads: {
      s2_10:  { ...summ(sp2_10),  history: sp2_10 },
      s3m_10: { ...summ(sp3m_10), history: sp3m_10 },
    },
    // 물가 & 실질금리
    inflation: {
      cpi:     yoy(cpi),
      coreCpi: yoy(coreCpi),
      ppi:     yoy(ppi),
      pce:     yoy(pce),
      corePce: yoy(corePce),
      wages:   yoy(wages),
      mich:    summ(mich),
      bei10:   summ(bei10),
      t5y5y:   summ(t5y5y),
      tips10:  summ(tips10),
    },
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PIPE 3: 신용·실물 리스크
//    GET /pipe3
//    4개 지표 병렬 수집:
//    ① SLOOS (DRTSCILM)           — 은행 대출태도, 실물 자금 밸브 (분기)
//    ② IG OAS (BAMLC0A0CM)        — 우량 신용 스프레드, CRE 리스크 메인 (일별)
//    ③ CRE 연체율 (DRCRELEXFACBS) — 실제 부실 확인 보조 지표 (분기)
//    ④ CCC HY (BAMLH0A3HYC)       — 좀비기업 꼬리 리스크 (일별)
//    ⑤ 한국 CDS 스크래핑 (Track1) → EM HY (BAMLEMHBHYCRPIOAS) 백업 (Track2)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function pipe3CreditRisk(env, ctx) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return json({ error: 'FRED_API_KEY 없음' }, 500);

  // ── FRED 공통 fetch 헬퍼 ──
  const fetchFred = async (seriesId, limit = 2) => {
    const url = `https://api.stlouisfed.org/fred/series/observations`
      + `?series_id=${seriesId}&api_key=${apiKey}&file_type=json`
      + `&limit=${limit}&sort_order=desc`;
    try {
      const r = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return null;
      const d = await r.json();
      const obs = (d.observations || []).filter(o => o.value !== '.');
      if (!obs.length) return null;
      const cur  = +parseFloat(obs[0].value).toFixed(4);
      const prev = obs.length > 1 ? +parseFloat(obs[1].value).toFixed(4) : null;
      return { current: cur, prev, delta: prev != null ? +(cur - prev).toFixed(4) : null, asOf: obs[0].date };
    } catch(e) { return null; }
  };

  // SLOOS 실제 발표일 fetch (release_id=74)
  const fetchSloosReleaseDate = async () => {
    try {
      const url = `https://api.stlouisfed.org/fred/release/dates`
        + `?release_id=74&api_key=${apiKey}&file_type=json`
        + `&sort_order=desc&limit=4&include_release_dates_with_no_data=false`;
      const r = await fetch(url, { cf: { cacheTtl: 86400 } });
      if (!r.ok) return null;
      const d = await r.json();
      // 가장 최근 발표일 반환
      return d.release_dates?.[0]?.date || null;
    } catch(e) { return null; }
  };

  // ── 한국 CDS 스크래핑 (Track 1) ──
  const fetchKoreaCds = async () => {
    try {
      // CDS 전용 엔드포인트: POST /wp-json/cds/v1/main
      // 응답: { success:true, chart: "...{\"code\":\"KR\",\"value\":33.10,...}..." }
      const r = await fetch('https://www.worldgovernmentbonds.com/wp-json/cds/v1/main/', {
        method: 'POST',
        headers: {
          'User-Agent':       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Content-Type':     'application/json',
          'Accept':           'application/json, */*',
          'Referer':          'https://www.worldgovernmentbonds.com/cds-historical-data/south-korea/5-years/',
          'Origin':           'https://www.worldgovernmentbonds.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
        body: JSON.stringify({
          FUNCTION: 'CDS', DOMESTIC: true, DATE_RIF: '2099-12-31',
          OBJ:      { UNIT:'', DECIMAL:2, UNIT_DELTA:'%', DECIMAL_DELTA:2 },
          COUNTRY1: { SYMBOL:'29', PAESE:'29', PAESE_UPPERCASE:'SOUTH KOREA', BANDIERA:'kr', URL_PAGE:'south-korea' },
          COUNTRY2: null,
          OBJ1:     { DURATA_STRING:'5 Years', DURATA:60 },
          OBJ2:     null,
        }),
        cf: { cacheTtl: 3600 },
      });
      if (!r.ok) return null;
      const data = await r.json();
      if (!data?.success) return null;

      // chart HTML 안의 지도 데이터에서 KR(한국) 값 추출
      // 패턴: {"code":"KR","value":33.10,...}
      const chart = data.chart ?? '';
      const m = chart.match(/"code"\s*:\s*"KR"\s*,\s*"value"\s*:\s*([\d]+\.[\d]+)/);
      if (!m) return null;
      const v = parseFloat(m[1]);
      return (v >= 10 && v <= 500) ? v : null;
    } catch(e) { return null; }
  };

  // 5개 병렬 실행
  const [sloos, sloosReleaseDate, igOas, creDelin, cccHy, emHy, koreaCds] = await Promise.all([
    fetchFred('DRTSCILM',          2),  // ① SLOOS (분기)
    fetchSloosReleaseDate(),            // ① SLOOS 실제 발표일
    fetchFred('BAMLC0A0CM',        2),  // ② IG OAS (일별)
    fetchFred('DRCRELEXFACBS',     2),  // ③ CRE 연체율 (분기)
    fetchFred('BAMLH0A3HYC',       2),  // ④ CCC HY (일별)
    fetchFred('BAMLEMHBHYCRPIOAS', 2),  // ⑤ EM HY 백업 (일별)
    fetchKoreaCds(),                    // ⑤ 한국 CDS Track1
  ]);

  // 한국 CDS: Track1 성공 시 사용, 실패 시 Track2(EM HY) 사용
  const sovereignTrack  = koreaCds != null ? 1 : 2;
  const sovereignVal    = koreaCds ?? emHy?.current ?? null;

  return json({
    source:   'FRED API + worldgovernmentbonds.com (CDS 스크래핑)',
    asOf:     new Date().toISOString().slice(0, 10),
    // ① SLOOS
    sloos: sloos ? { ...sloos, label: '실물 자금 밸브 (SLOOS · DRTSCILM)', freq: '분기', releaseDate: sloosReleaseDate } : null,
    // ② IG OAS (메인 신호) + CRE 연체율 (보조)
    igOas: igOas ? { ...igOas, label: 'IG 회사채 OAS (BAMLC0A0CM)', freq: '일별' } : null,
    creDelinquency: creDelin ? { ...creDelin, label: 'CRE 대출 연체율 (DRCRELEXFACBS)', freq: '분기', unit: '%' } : null,
    // ③ CCC HY
    cccHy: cccHy ? { ...cccHy, label: 'CCC 이하 HY OAS (BAMLH0A3HYC)', freq: '일별' } : null,
    // ④ Sovereign
    sovereign: {
      track:   sovereignTrack,          // 1 = 한국 CDS, 2 = EM HY 백업
      value:   sovereignVal,
      koreaCds,                         // null이면 스크래핑 실패
      emHy:    emHy ?? null,
      label:   sovereignTrack === 1
        ? '한국 5Y CDS (worldgovernmentbonds.com)'
        : 'EM HY OAS 백업 (BAMLEMHBHYCRPIOAS)',
      unit:    sovereignTrack === 1 ? 'bp' : '%',
    },
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 유틸
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: CORS,
  });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 유동성 관제탑 — 통합 데이터 (H.4.1 + FRED + QRA + Auctions)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function liqTowerCached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.liqTower, force);
  if (cached) return json(cached);
  const data = await fetchLiqTowerData(env);
  const putPromise = kvPut(env, KV_KEYS.liqTower, data, KV_TTL.liqTower);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return json(data);
}

async function refreshLiqTower(env) {
  try {
    const data = await fetchLiqTowerData(env);
    await kvPut(env, KV_KEYS.liqTower, data, KV_TTL.liqTower);
  } catch(e) { console.error('refreshLiqTower:', e.message); }
}

async function fetchLiqTowerData(env) {
  const apiKey = env?.FRED_API_KEY;

  const fredVal = async (id) => {
    if (!apiKey) return null;
    try {
      const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&sort_order=desc&limit=10`;
      const r = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return null;
      const d = await r.json();
      const obs = (d.observations || []).filter(o => o.value !== '.');
      return obs.length ? { value: parseFloat(obs[0].value), date: obs[0].date } : null;
    } catch(e) { return null; }
  };

  const fredSeries = async (id, limit = 52) => {
    if (!apiKey) return [];
    try {
      const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&sort_order=desc&limit=${limit}`;
      const r = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return [];
      const d = await r.json();
      return (d.observations || []).filter(o => o.value !== '.').map(o => ({
        date: o.date, value: parseFloat(o.value),
      })).reverse();
    } catch(e) { return []; }
  };

  // GitHub Actions가 4시간마다 갱신 → raw URL로 fetch (Akamai 우회)
  const auctionsFetch = async () => {
    const RAW = 'https://raw.githubusercontent.com/z66g/macromonitor/main/data/auctions.json';
    try {
      const r = await fetch(RAW, { cf: { cacheTtl: 3600 } });
      if (!r.ok) return [];
      const d = await r.json();

      const today = new Date().toISOString().slice(0, 10);

      // upcoming: 아직 경매 미실시
      const upcoming = d.upcoming || [];

      // auctioned: 경매 완료됐지만 결제일(issueDate)이 아직 안 된 것
      const unsettled = (d.auctioned || []).filter(a =>
        a.issueDate && a.issueDate >= today
      );

      // 합산 후 issueDate 기준 정렬, 중복 cusip 제거
      const seen = new Set();
      const combined = [...upcoming, ...unsettled]
        .filter(a => {
          if (!a.cusip || seen.has(a.cusip)) return !a.cusip;
          seen.add(a.cusip);
          return true;
        })
        .sort((a, b) => (a.issueDate || '').localeCompare(b.issueDate || ''));

      return combined;
    } catch(e) {
      console.error('[auctions] GitHub fetch error:', e.message);
      return [];
    }
  };


  const [walcl, rrp, rrpSeries, tga, walclSeries, tgaSeries, auctions, qraActive, tgaDtsData] = await Promise.all([
    fredVal('WALCL'),
    fredVal('RRPONTSYD'),
    fredSeries('RRPONTSYD', 52),
    fredVal('WTREGEN'),
    fredSeries('WALCL', 15),
    fredSeries('WTREGEN', 26),   // TGA 6개월 시계열 (Millions) — fallback
    auctionsFetch(),
    kvGet(env, KV_KEYS.qraActive),
    kvGet(env, KV_KEYS.tgaDts).catch(() => null),  // DTS 일별 TGA ($B)
  ]);

  // RRP 기반 T-Bill 동적 가중치
  const rrpBn = rrp?.value ?? 0;
  let billWeight, billWeightLabel;
  if (rrpBn > 500)      { billWeight = 0.2; billWeightLabel = '0.2 (충분한 RRP 완충)'; }
  else if (rrpBn > 100) { billWeight = 0.5; billWeightLabel = '0.5 (마찰적 흡수 시작)'; }
  else                  { billWeight = 1.0; billWeightLabel = '1.0 (RRP 소진, 장기채 동등 충격)'; }

  const h41Tower = await fetchH41ForTower(env, { rrpSeries, walclSeries, tgaSeries });

  // DTS TGA 우선, fallback H.4.1
  const dtsTgaLatest = tgaDtsData?.latest ?? null;  // {date, close} $B
  const tgaCurrentBn = dtsTgaLatest?.close ?? h41Tower?.tga?.cur ?? null;

  // DTS 데이터가 있으면 h41Tower.tga를 DTS 기반으로 오버라이드
  if (dtsTgaLatest && tgaDtsData?.series?.length >= 2) {
    const dtsSorted = tgaDtsData.series.slice().sort((a, b) => b.date.localeCompare(a.date));
    h41Tower.tga = {
      cur:   dtsSorted[0].close,
      prev:  dtsSorted[1].close,
      delta: +(dtsSorted[0].close - dtsSorted[1].close).toFixed(1),
      date:  dtsSorted[0].date,
      source: 'DTS',
    };
    // liabilities도 갱신
    if (h41Tower.liabilities) {
      h41Tower.liabilities.tga = dtsSorted[0].close;
      const { res = 0, rrp: rrpL = 0, curr = 0, other = 0 } = h41Tower.liabilities;
      h41Tower.liabilities.total = +(res + rrpL + dtsSorted[0].close + curr + other).toFixed(1);
    }
  }

  const vampire4w = buildVampireModel(auctions, rrpBn, billWeight, qraActive, h41Tower?.maturity ?? null, tgaCurrentBn,
                                     h41Tower?.walcl_anomaly?.avg_13w_delta ?? null);

  return {
    _savedAt: new Date().toISOString(),
    fed: {
      walcl:       walcl,
      rrp:         rrp,
      rrpSeries:   rrpSeries.slice(-26),
      walclSeries: walclSeries,
      tgaSeries:   tgaSeries,   // WTREGEN 26주 (Millions) — fallback
      tgaDts:      tgaDtsData,  // DTS 일별 TGA ($B) — 프론트 차트용
      h41:         h41Tower,
    },
    auctions,
    vampire: {
      billWeight,
      billWeightLabel,
      rrpBn,
      weeks: vampire4w,
    },
    qraActive: normalizeQra(qraActive),
  };
}

// ── H.4.1 관제탑용 KPI + 부채구조 + WALCL 이상징후 ─────────
async function fetchH41ForTower(env, preloadedSeries = {}) {
  const apiKey = env?.FRED_API_KEY;
  if (!apiKey) return null;

  const IDS_STD = {
    // WDTGAL은 TGA 주평균이라 '기타'로 더하면 TGA가 이중 계산됨 → 제거
  };

  const fetchFredObs = async (id, limit, unitM) => {
    try {
      const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${apiKey}&file_type=json&limit=${limit}&sort_order=desc`;
      const r = await fetch(url, { cf: { cacheTtl: 21600 } });
      if (!r.ok) return null;
      const d = await r.json();
      const obs = (d.observations||[]).filter(o => o.value !== '.');
      if (!obs.length) return null;
      const scale = unitM ? 1000 : 1;
      return obs.map(o => ({ date: o.date, value: +(parseFloat(o.value)/scale).toFixed(1) }));
    } catch(e) { return null; }
  };

  const toKv = (obs) => {
    if (!obs?.length) return null;
    const cur  = obs[0].value;
    const prev = obs[1]?.value ?? null;
    return { cur, prev, delta: prev != null ? +(cur-prev).toFixed(1) : null, date: obs[0].date };
  };

  // preloadedSeries: 이미 fetchLiqTowerData에서 가져온 52주 시계열 재사용
  const rrpSeriesP   = preloadedSeries.rrpSeries   || [];  // Billions, desc→reversed
  const walclSeriesP = preloadedSeries.walclSeries  || [];  // Billions, reversed
  const tgaSeriesP   = preloadedSeries.tgaSeries    || [];  // Millions, reversed

  const [stdResults, walclObs, h41HtmlResult] = await Promise.all([
    Promise.all(Object.entries(IDS_STD).map(async ([id, {key, unitM}]) =>
      [key, toKv(await fetchFredObs(id, 3, unitM))]
    )),
    fetchFredObs('WALCL', 15, true),
    fetchH41HtmlData(env),
  ]);

  const kv = Object.fromEntries(stdResults.filter(([,v]) => v));

  // reserve_balances / rrp / tga: h41HtmlResult에서 직접 사용 (FRED API 호출 불필요)
  if (h41HtmlResult?.reserve_balances) kv['reserve_balances'] = h41HtmlResult.reserve_balances;
  if (h41HtmlResult?.rrp)              kv['rrp']              = h41HtmlResult.rrp;
  if (h41HtmlResult?.tga)              kv['tga']              = h41HtmlResult.tga;

  // WALCL 13주 국면 인식
  let walcl_anomaly = { status:'NORMAL', regime:'QT', avg_13w_delta:null, threshold:10, walcl_delta:null, loans_delta:null, walcl_cur:null, loans_cur:null };
  if (walclObs && walclObs.length >= 2) {
    const deltas = [];
    for (let i = 0; i < Math.min(13, walclObs.length-1); i++)
      deltas.push(+(walclObs[i].value - walclObs[i+1].value).toFixed(1));
    const walcl_delta   = deltas[0] ?? 0;
    const avg_13w_delta = deltas.length ? +(deltas.reduce((a,b)=>a+b,0)/deltas.length).toFixed(1) : 0;
    const regime    = avg_13w_delta < 0 ? 'QT' : 'QE';
    const threshold = regime === 'QT' ? 10 : +(avg_13w_delta+20).toFixed(1);
    const loans_delta = h41HtmlResult?.loans?.delta ?? 0;
    const loans_cur   = h41HtmlResult?.loans?.cur   ?? null;
    let status = 'NORMAL';
    if (loans_delta > 2 || walcl_delta > threshold)  status = 'SPIKE_WARNING';
    else if (walcl_delta > 0 && regime === 'QT')     status = 'WATCH';
    walcl_anomaly = { status, regime, avg_13w_delta, threshold, walcl_delta, walcl_cur: walclObs[0].value, loans_delta, loans_cur };
  }

  kv.total_assets = toKv(walclObs);

  const res   = kv.reserve_balances?.cur ?? 0;
  const rrpV  = kv.rrp?.cur              ?? 0;
  const tgaV  = kv.tga?.cur             ?? 0;
  const curr  = h41HtmlResult?.currency ?? 0;
  const otherV= 0;   // (구) WDTGAL = TGA 중복 → 제외
  const totL  = +(res + rrpV + tgaV + curr + otherV).toFixed(1);
  const bufCur = +(res + rrpV).toFixed(1);
  const bufPrev = (kv.reserve_balances?.prev!=null && kv.rrp?.prev!=null)
    ? +(kv.reserve_balances.prev + kv.rrp.prev).toFixed(1) : null;

  return {
    ...kv,
    loans: h41HtmlResult?.loans ?? null,
    buffer: { cur: bufCur, prev: bufPrev, delta: bufPrev!=null ? +(bufCur-bufPrev).toFixed(1):null },
    liabilities: { res, rrp: rrpV, tga: tgaV, curr, other: otherV, total: totL },
    data_date: h41HtmlResult?.loans?.date ?? kv.reserve_balances?.date ?? null,
    walcl_anomaly,
    maturity:       h41HtmlResult?.maturity       ?? null,
    treasury_total: h41HtmlResult?.treasury_total ?? null,
  };
}

// ── H.4.1 HTML 통합 파싱 (loans + maturity + currency) ─────
async function fetchH41HtmlData(env) {
  // h41HistoryFetcher(weeks=2)로 H.4.1 HTML 파싱 데이터 전체 활용
  // reserve_balances / rrp / tga / loans / currency / treasury_total 모두 포함
  try {
    // h41HistoryFetcher 직접 호출 (3주치: cur/prev/delta 확보)
    const resp = await h41HistoryFetcher(new URL('https://dummy'), 3, env);
    const data = await resp.json();
    const s    = data?.series;
    if (!s) return { loans:null, maturity:null, currency:null, treasury_total:null,
                     reserve_balances:null, rrp:null, tga:null };

    const toKv2 = (arr, dates) => {
      const cur  = arr?.[0] ?? null;
      const prev = arr?.[1] ?? null;
      return cur != null ? {
        cur, prev,
        delta: prev != null ? +(cur - prev).toFixed(1) : null,
        date:  dates?.[0] ?? null,
      } : null;
    };

    // 은행 준비금 (Table1 최하단: Reserve balances with F.R. Banks)
    const reserve_balances = toKv2(s.reserve_balances, s.labels);

    // ON RRP domestic (외국계정 제외)
    const rrp = toKv2(s.on_rrp_domestic, s.labels);

    // TGA
    const tga = toKv2(s.tga, s.labels);

    // 긴급대출
    const loans = toKv2(s.loans, s.labels);

    // 만기 절벽
    const maturity = (s.treasury_within_15d?.[0] != null) ? {
      treasury_within_15d: s.treasury_within_15d?.[0] ?? null,
      treasury_d16_90d:    s.treasury_d16_90d?.[0]    ?? null,
      treasury_d91_1y:     s.treasury_d91_1y?.[0]     ?? null,
      treasury_y1_5y:      s.treasury_y1_5y?.[0]      ?? null,
      treasury_y5_10y:     s.treasury_y5_10y?.[0]     ?? null,
      treasury_over_10y:   s.treasury_over_10y?.[0]   ?? null,
    } : null;

    // 화폐 유통량 (Currency in circulation, Table1 최상단)
    // currency: series.currency_circ[0] (fed_notes_net, Billions)
    const currency = s.currency_circ?.[0] ?? null;

    // 연준 보유 국채 WoW (순발행 프록시)
    const treasury_total = toKv2(s.treasury_total, s.labels);

    return { reserve_balances, rrp, tga, loans, maturity, currency, treasury_total };
  } catch(e) {
    console.error('[fetchH41HtmlData]', e.message);
    return { reserve_balances:null, rrp:null, tga:null, loans:null,
             maturity:null, currency:null, treasury_total:null };
  }
}

// ── TGA 뱀파이어 4주 추정 모델 ──────────────────────────
// ── QRA 정규화 — 재무부 QRA는 '이번 분기 + 다음 분기' 두 추정치를 발표한다 ──
// 새 형식(quarters 배열)이면 날짜 기준 해당 분기 항목을 고르고, 구 형식(단일 값)이면 분기 일치 여부만 표시.
function quarterLabel(d) {
  return `${d.getUTCFullYear()} Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
}
function qraForQuarter(raw, label) {
  if (!raw) return null;
  if (Array.isArray(raw.quarters)) {
    const e = raw.quarters.find(x => x.quarter === label);
    return e ? { net: e.net_borrowing_billions, cash: e.end_cash_balance_billions, quarter: e.quarter } : null;
  }
  return raw.target_quarter === label
    ? { net: raw.net_borrowing_billions, cash: raw.tga_target_balance_billions, quarter: raw.target_quarter } : null;
}
function normalizeQra(raw, now = new Date()) {
  if (!raw) return null;
  const cur = qraForQuarter(raw, quarterLabel(now));
  const first = Array.isArray(raw.quarters) ? raw.quarters[0] : null;
  const shown = cur ?? (first ? { net: first.net_borrowing_billions, cash: first.end_cash_balance_billions, quarter: first.quarter }
                              : { net: raw.net_borrowing_billions, cash: raw.tga_target_balance_billions, quarter: raw.target_quarter });
  return {
    ...raw,
    target_quarter:              shown.quarter,
    net_borrowing_billions:      shown.net,
    tga_target_balance_billions: shown.cash,
    quarter_match:               !!cur,          // false면 현재 분기용 QRA가 아님(다음 발표 대기)
  };
}

function buildVampireModel(auctions, rrpBn, billWeight, qraActive, maturityData, tgaCurrentBn = null, qtWeeklyBn = null) {
  const now = new Date();
  const weeks = [];

  // ── 분기 남은 주수 계산 ──────────────────────────────
  const currentQ = Math.floor(now.getUTCMonth() / 3);       // 0,1,2,3
  const quarterEnd = new Date(Date.UTC(now.getUTCFullYear(), (currentQ + 1) * 3, 0)); // 분기 말일
  const remainingDays = Math.max(1, (quarterEnd - now) / 86400000);
  const remainingWeeks = Math.max(2, Math.ceil(remainingDays / 7));   // 분기말 1주 남았을 때 갭 전체가 한 주로 몰리지 않도록

  // ── weeklyNet: TGA 갭 기반 동적 계산 ──
  // 현재 분기용 QRA(분기말 현금 목표)가 있을 때만 갭 사용. 다른 분기 QRA를 쓰지 않는다.
  const curQ  = qraForQuarter(qraActive, quarterLabel(now));
  const nextQ = qraForQuarter(qraActive, quarterLabel(new Date(Date.UTC(now.getUTCFullYear(), (currentQ + 1) * 3, 1))));
  const qraFlat = curQ?.net != null ? +(curQ.net / 13).toFixed(1) : 30;

  let weeklyNet, weeklyNetSource;
  if (curQ?.cash != null && tgaCurrentBn != null) {
    const tgaGap = curQ.cash - tgaCurrentBn;
    const gapBased = +(Math.max(0, tgaGap) / remainingWeeks).toFixed(1);
    weeklyNet = Math.min(Math.max(gapBased, +(qraFlat * 0.5).toFixed(1)), +(qraFlat * 3).toFixed(1));  // 0.5~3배 범위
    weeklyNetSource = 'tga_gap';
  } else {
    weeklyNet = qraFlat;
    weeklyNetSource = curQ ? 'qra_flat' : (qraActive ? 'qra_stale' : 'fallback');
  }
  const nextWeeklyNet = nextQ?.net != null ? +(nextQ.net / 13).toFixed(1) : qraFlat;
  const quarterEndStr = quarterEnd.toISOString().slice(0, 10);

  // ── QT 롤오프: 연준 총자산의 최근 13주 평균 주간 감소분 (만기분은 대부분 재투자되므로 만기액 ≠ QT) ──
  // 대차대조표가 늘고 있으면(QT 종료·준비금 관리 매입) 0
  const qtWeekly = qtWeeklyBn != null ? +Math.max(0, -qtWeeklyBn).toFixed(1) : 0;
  const qtByWeek = [qtWeekly, qtWeekly, qtWeekly, qtWeekly];

  for (let w = 0; w < 4; w++) {
    const weekStart = new Date(now);
    weekStart.setDate(now.getDate() + w * 7);
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 6);

    const wsStr = weekStart.toISOString().slice(0,10);
    const weStr = weekEnd.toISOString().slice(0,10);
    const weekNet = wsStr > quarterEndStr ? nextWeeklyNet : weeklyNet;   // 분기 경계 넘으면 다음 분기 기준

    // 해당 주 경매 결제 필터링 (Gross 비율 계산용)
    const weekAuctions = auctions.filter(a => {
      const issue = a.issueDate?.slice(0,10);
      return issue >= wsStr && issue <= weStr;
    });

    // ── 총발행 구성 비율 산출 (Gross) ────────────────────
    let billGross   = 0;
    let couponGross = 0;
    const auctionDetail = [];

    for (const a of weekAuctions) {
      const amt = a.offeringAmt / 1e9;
      if (a.type === 'Bill') {
        billGross += amt;
        auctionDetail.push({ label: `T-Bill ${a.term}`, grossBn: +amt.toFixed(1) });
      } else {
        couponGross += amt;
        auctionDetail.push({ label: `${a.type} ${a.term}`, grossBn: +amt.toFixed(1) });
      }
    }

    const totalGross = billGross + couponGross;

    // ── QRA 탑다운 순발행 할당 ───────────────────────────
    let netBillAbsorb  = 0;
    let netCouponDrain = 0;

    if (totalGross > 0) {
      const billRatio   = billGross   / totalGross;
      const couponRatio = couponGross / totalGross;
      netBillAbsorb  = +(weekNet * billRatio).toFixed(1);
      netCouponDrain = +(weekNet * couponRatio).toFixed(1);
    } else {
      // 경매 없는 주: 전체를 50/50 추정
      netBillAbsorb  = +(weekNet * 0.5).toFixed(1);
      netCouponDrain = +(weekNet * 0.5).toFixed(1);
    }

    // ── QT (H.4.1 만기 동적) ─────────────────────────────
    const qtDrain = qtByWeek[w];

    // ── 정부 지출 (월초/월말 추정) ───────────────────────
    const dayOfMonth = weekStart.getDate();
    const govOutflow = (dayOfMonth <= 5 || dayOfMonth >= 26) ? -50 : -20;

    // ── 최종 순 유동성 흡수 ──────────────────────────────
    const netDrain = +(netCouponDrain + (netBillAbsorb * billWeight) + qtDrain + govOutflow).toFixed(1);
    const isDangerZone = netDrain > 50;

    weeks.push({
      label:     `Week ${w+1}`,
      dateRange: `${wsStr} ~ ${weStr}`,
      netDrain,
      breakdown: {
        couponDrain:  +netCouponDrain.toFixed(1),       // Net 쿠폰 흡수
        billAbsorb:   +(netBillAbsorb * billWeight).toFixed(1), // Net Bill (가중)
        qtDrain:      +qtDrain.toFixed(1),               // QT 롤오프
        govOutflow:   +govOutflow.toFixed(1),            // 정부 지출
      },
      meta: {
        weeklyNet: weekNet,
        weeklyNetSource,
        tgaGap:      curQ?.cash != null && tgaCurrentBn != null ? +(curQ.cash - tgaCurrentBn).toFixed(1) : null,
        tgaTarget: curQ?.cash ?? null,
        tgaCurrent:  tgaCurrentBn,
        remainingWeeks,
        qraFlat,
        billRatio:    totalGross > 0 ? +(billGross/totalGross*100).toFixed(0) : 50,
        couponRatio:  totalGross > 0 ? +(couponGross/totalGross*100).toFixed(0) : 50,
        maturitySource: qtWeeklyBn != null ? 'H.4.1 13주 평균' : 'unavailable',
        qraSource:      qraActive ? 'QRA' : 'fallback',
      },
      auctionDetail,
      isDangerZone,
    });
  }
  return weeks;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// QRA — 상태 (읽기 전용 · 갱신은 data/qra.json → qra-apply.yml → apply_qra.py)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function qraStatus(env) {
  return json({ active: normalizeQra(await kvGet(env, KV_KEYS.qraActive)) });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 온체인 매크로 타워 (Stablecoin + RWA + BTC ETF)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function onchainMacroCached(env, force = false, ctx) {
  const cached = await cacheFirst(env, KV_KEYS.onchainMacro, force);
  if (cached) return json(cached);
  const data = await refreshOnchain(env, ctx);
  return json(data);
}

// 새로 받은 결과에서 비어 있는 패널은 직전 정상 값으로 채움 (stale 표시) → 그 뒤 저장
async function refreshOnchain(env, ctx) {
  const [data, prev] = await Promise.all([fetchOnchainMacro(), kvGet(env, KV_KEYS.onchainMacro)]);
  const ok = {
    stablecoin: d => (d?.totalB ?? 0) > 0,
    rwa:        d => (d?.protocols?.length ?? 0) > 0,
    etf:        d => (d?.dailyFlows?.length ?? 0) > 0,
  };
  for (const [k, valid] of Object.entries(ok)) {
    if (!valid(data[k]) && valid(prev?.[k])) data[k] = { ...prev[k], stale: true };
  }
  const putPromise = kvPut(env, KV_KEYS.onchainMacro, data, KV_TTL.onchainMacro);
  if (ctx?.waitUntil) ctx.waitUntil(putPromise); else await putPromise;
  return data;
}

async function fetchOnchainMacro() {
  const get = async (url) => {
    try {
      const r = await fetch(url, {
        headers: { Accept: 'application/json', 'User-Agent': 'Mozilla/5.0' },
        cf: { cacheTtl: 300 },
      });
      if (!r.ok) return null;
      return await r.json();
    } catch(e) { return null; }
  };

  // ── 패널 1: 스테이블코인 — DefiLlama 우선, CoinGecko 대체 ─────────
  // CoinGecko 무료 API는 Cloudflare Worker IP를 자주 거절(429/403) → 0으로 표시되던 문제.
  // DefiLlama stablecoincharts: [{date(초), totalCirculatingUSD:{peggedUSD}}, ...] 일별 (USDT id 1, USDC id 2)
  const llamaSeries = async (id) => {
    const d = await get(`https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=${id}`);
    if (!Array.isArray(d)) return [];
    return d.slice(-31).map(e => [
      Number(e.date) * 1000,
      e.totalCirculatingUSD?.peggedUSD ?? e.totalCirculating?.peggedUSD ?? 0,
    ]).filter(([, v]) => v > 0);
  };
  let [usdtHistory, usdcHistory] = await Promise.all([llamaSeries(1), llamaSeries(2)]);
  let stableSource = 'DefiLlama';
  if (!usdtHistory.length || !usdcHistory.length) {
    const [t, c] = await Promise.all([
      get('https://api.coingecko.com/api/v3/coins/tether/market_chart?vs_currency=usd&days=30&interval=daily'),
      get('https://api.coingecko.com/api/v3/coins/usd-coin/market_chart?vs_currency=usd&days=30&interval=daily'),
    ]);
    if (t?.market_caps?.length && c?.market_caps?.length) {
      usdtHistory = t.market_caps; usdcHistory = c.market_caps; stableSource = 'CoinGecko';
    }
  }

  // 날짜 기준 병합 (두 시계열 길이·시각이 다를 수 있음)
  const usdcByDate = new Map(usdcHistory.map(([ts, v]) => [new Date(ts).toISOString().slice(0, 10), v]));
  const stableHistory = usdtHistory.map(([ts, usdt]) => {
    const date = new Date(ts).toISOString().slice(0, 10);
    const usdc = usdcByDate.get(date) || 0;
    const tot  = (usdt || 0) + usdc;
    return {
      date,
      totalB:  +(tot  / 1e9).toFixed(1),
      usdtB:   +((usdt || 0) / 1e9).toFixed(1),
      usdcB:   +(usdc / 1e9).toFixed(1),
      usdcPct: tot > 0 ? +(usdc / tot * 100).toFixed(2) : 0,
    };
  }).filter(h => h.usdcB > 0);

  const lastStable = stableHistory[stableHistory.length - 1];
  const stablecoin = {
    totalB:   lastStable?.totalB  ?? 0,
    usdtB:    lastStable?.usdtB   ?? 0,
    usdcB:    lastStable?.usdcB   ?? 0,
    usdcPct:  lastStable?.usdcPct ?? 0,
    asOf:     lastStable?.date    ?? null,
    source:   lastStable ? stableSource : null,
    history:  stableHistory,
  };

  // ── 패널 2: RWA (DefiLlama) ───────────────────────────
  const allProtocols = await get('https://api.llama.fi/protocols');

  // RWA 카테고리 필터링 — 상위 10개
  const rwaProtos = (allProtocols || [])
    .filter(p => (p.category || '').toLowerCase() === 'rwa')
    .sort((a, b) => (b.tvl || 0) - (a.tvl || 0));

  const top10 = rwaProtos.slice(0, 10);
  const rwaTotal = rwaProtos.reduce((s, p) => s + (p.tvl || 0), 0);

  // 주요 RWA 개별 TVL (상위 10개)
  const rwaList = top10.map(p => ({
    name:   p.name,
    symbol: p.symbol || '',
    slug:   p.slug   || '',
    tvlB:   +((p.tvl || 0) / 1e9).toFixed(3),
    pct:    rwaTotal > 0 ? +(( p.tvl || 0) / rwaTotal * 100).toFixed(1) : 0,
    chain:  p.chains?.[0] || '',
    logo:   p.logo || '',
  }));

  // ── RWA 히스토리 — 동적 슬러그 병렬 fetch ────────────────
  // 상위 10개의 slug를 실시간으로 추출 → 하드코딩 없이 자동 갱신
  const rwaHistory = await (async () => {
    const slugs = top10.map(p => p.slug).filter(Boolean);
    if (!slugs.length) return [];

    // 각 프로토콜 히스토리 병렬 fetch (최대 10개)
    const results = await Promise.allSettled(
      slugs.map(slug =>
        get(`https://api.llama.fi/protocol/${slug}`)
          .then(d => ({ slug, tvl: d?.tvl || [] }))
      )
    );

    // 날짜별 합산 맵 — 키를 YYYY-MM-DD 문자열로 정규화
    // (프로토콜마다 timestamp가 조금씩 달라 raw timestamp 키를 쓰면 부분합산 오류 발생)
    const dayMap = new Map();
    const cutoffDate = new Date(Date.now() - 730 * 86400 * 1000)
                         .toISOString().slice(0, 10);  // 2년 전 날짜

    for (const r of results) {
      if (r.status !== 'fulfilled') continue;
      for (const entry of (r.value.tvl || [])) {
        const dateStr = new Date(entry.date * 1000).toISOString().slice(0, 10);
        if (dateStr < cutoffDate) continue;            // 2년 이전 스킵
        const val = entry.totalLiquidityUSD || 0;
        dayMap.set(dateStr, (dayMap.get(dateStr) || 0) + val);
      }
    }

    // 오늘 날짜 기준 미래 날짜 제거 (일부 프로토콜 데이터 오류 방어)
    const todayStr = new Date().toISOString().slice(0, 10);
    for (const k of dayMap.keys()) {
      if (k > todayStr) dayMap.delete(k);
    }

    // 날짜 오름차순 정렬
    return [...dayMap.entries()]
      .sort(([a], [b]) => a < b ? -1 : 1)
      .map(([date, val]) => ({
        date,
        tvlB: +( val / 1e9).toFixed(3),
      }));
  })();

  // T-Bill 특화 RWA (토큰화 국채) 필터
  const tbillKeywords = ['buidl','fobxx','ousg','benji','stbt','usyc','tbill','t-bill','treasury'];
  const tbillList = rwaList.filter(p =>
    tbillKeywords.some(k => p.name.toLowerCase().includes(k) || p.symbol.toLowerCase().includes(k))
  );

  const rwa = {
    totalB:    +( rwaTotal / 1e9).toFixed(3),
    protocols:  rwaList,
    tbills:     tbillList,
    history:    rwaHistory,   // [{date, tvlB}] — 상위 10개 합산, 2년치
  };

  // ── 패널 3: BTC ETF (GitHub Actions 릴레이) ─────────────
  const RAW_ETF = 'https://raw.githubusercontent.com/z66g/macromonitor/main/data/etf_flows.json';
  const etfRaw  = await get(RAW_ETF);

  const etf = etfRaw ? {
    updated:    etfRaw.updated || null,
    dailyFlows: etfRaw.dailyFlows || [],   // [{date, netFlow, cumFlow}]
    latestDate: etfRaw.dailyFlows?.[0]?.date || null,
    latestNet:  etfRaw.dailyFlows?.[0]?.netFlow || 0,
    cumTotal:   etfRaw.cumTotal || 0,
    // 연속 유출일 계산
    consecutiveOutflows: (() => {
      let cnt = 0;
      for (const d of (etfRaw.dailyFlows || [])) {
        if (d.netFlow < 0) cnt++; else break;
      }
      return cnt;
    })(),
  } : { updated: null, dailyFlows: [], latestNet: 0, cumTotal: 0, consecutiveOutflows: 0 };

  // 경고 플래그
  const alert = etf.consecutiveOutflows >= 3 ? 'ETF_OUTFLOW_WARNING' : null;

  return {
    _savedAt: new Date().toISOString(),
    alert,
    stablecoin,
    rwa,
    etf,
  };
}


// ══════════════════════════════════════════════════════════
// 뉴스 다이제스트 조회·발송 — 생성은 GitHub Actions(Gemini)가 KV에 저장
// ══════════════════════════════════════════════════════════
async function newsDigestEndpoint(env) {
  const digest = await kvGet(env, KV_KEYS.newsDigest);
  return json(digest || { items: [], generatedAt: null, type: null });
}
// cron-job.org가 KST 06:00 / 21:00에 호출. 누구나 호출할 수 있는 공개 URL이므로
// 그 시각 ±20분 안에서만, 시간대마다 한 번만 실제로 발송한다.
const DIGEST_SLOTS_KST = [6, 21];
async function newsDigestSend(env) {
  const kst = new Date(Date.now() + 9 * 3600000);
  const mins = kst.getUTCHours() * 60 + kst.getUTCMinutes();
  const slot = DIGEST_SLOTS_KST.find(h => Math.abs(mins - h * 60) <= 20);
  if (slot === undefined) return json({ ok: false, skipped: 'outside send window' }, 403);
  const slotKey = `digest_sent_${kst.toISOString().slice(0, 10)}_${slot}`;
  if (await env.MMF_KV.get(slotKey)) return json({ ok: true, skipped: 'already sent for this slot' });
  try {
    await env.MMF_KV.put(slotKey, '1', { expirationTtl: 86400 });   // 먼저 표시 → 동시 호출 중복 방지
    await sendBatchedTelegramDigest(env);
    return json({ ok: true, sentAt: new Date().toISOString() });
  } catch(e) {
    await env.MMF_KV.delete(slotKey).catch(() => {});   // 실패 시 같은 시간대 재시도 허용
    console.error('[NEWS DIGEST SEND ERROR]', e.message);
    return json({ ok: false, error: e.message }, 500);
  }
}

async function newsScheduledRefresh(env) {
  try {
    // 1. RSS 파싱 (LLM 호출 없음)
    const data = await newsFetchAll(env);
    if (!data?.items?.length) {   // RSS 전체 실패 시 기존 캐시 유지
      console.error('[News Cron] RSS 0건 — 기존 캐시 유지');
      return;
    }

    // 2. 기존 번역맵 적용 (GH Actions가 채워놓은 번역본을 items에 merge)
    const transMap = await newsLoadTransMap(env);
    applyTransMapToItems(data.items, transMap);

    // 3. 뉴스캐시 저장
    await env.MMF_KV.put(
      KV_KEYS.newsCache,
      JSON.stringify({ ...data, _translatedAt: new Date().toISOString() }),
      { expirationTtl: KV_TTL.newsCache }
    );

    console.log(`[News Cron] RSS 완료: 전체 ${data.items.length}건 (번역은 GH Actions가 담당)`);
  } catch(e) {
    console.error('[News Cron] 실패:', e.message);
  }
}

// ═══════════════════════════════════════════════════════════════
//  NEWS MODULE
//  /news-test  → 항상 fresh fetch (캐시 무시, 테스트용)
//  /news       → KV 캐시 사용 (TTL 1시간, 실서비스용)
// ═══════════════════════════════════════════════════════════════

// ── 카테고리 정의 ──────────────────────────────────────────────
// fed      : 연준·중앙은행 통화정책
// treasury : 미 재무부·국채·재정
// macro    : 글로벌 거시·유동성
// hardtech : AI반도체·에너지·바이오 하드테크
// crypto   : 온체인·RWA·기관자금이동

const NEWS_SOURCES = [

  // ── 1. MACRO & PLUMBING ───────────────────────────────────────
  {
    id: 'fed_press',
    name: 'Federal Reserve Press',
    url: 'https://www.federalreserve.gov/feeds/press_all.xml',
    cat: 'fed',
    tier: 1,  // 1차 원문 소스
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0', 'Accept': 'application/rss+xml,text/xml,*/*' },
  },
  {
    id: 'us_treasury',
    name: 'US Treasury Press',
    url: 'https://home.treasury.gov/news/press-releases/rss.xml',
    cat: 'treasury',
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
    // 실패 시 fallback
    fallbackUrl: 'https://home.treasury.gov/rss.xml',
  },
  {
    id: 'bis_press',
    name: 'BIS Press Releases',
    url: 'https://www.bis.org/doclist/all_pressrels.rss',
    cat: 'treasury',  // 국채 탭으로 이동 (국제결제은행 시장 분석)
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    // WSJ Markets — 국채/채권 시장 전문 피드 (WSJ Economy와 같은 도메인, 검증됨)
    id: 'wsj_markets',
    name: 'WSJ Markets',
    url: 'https://feeds.a.dj.com/rss/RSSMarketsMain.xml',
    cat: 'treasury',
    tier: 2,
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)',
      'Accept': 'application/rss+xml,text/xml,*/*',
    },
  },
  {
    // ECB Press — 유럽중앙은행 공식 발표 (국채/채권 시장 영향)
    id: 'ecb_press',
    name: 'ECB Press Releases',
    url: 'https://www.ecb.europa.eu/rss/press.html',
    cat: 'treasury',
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0', 'Accept': 'application/rss+xml,text/xml,*/*' },
  },
  {
    id: 'bis_speeches',
    name: 'BIS Central Bank Speeches',
    url: 'https://www.bis.org/doclist/cbspeeches.rss',
    cat: 'fed',
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    id: 'zerohedge',
    name: 'ZeroHedge',
    url: 'https://feeds.feedburner.com/zerohedge/feed',
    cat: 'macro',
    tier: 2,
    // ⚠️ ZeroHedge는 Cloudflare 보호 → CF Worker에서 530 가능성 있음
    // feedburner 경유로 우회 시도; 실패 시 rsshub fallback
    fallbackUrl: 'https://rsshub.app/zerohedge/news',
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
      'Accept': 'application/rss+xml,text/xml,*/*',
      'Referer': 'https://www.google.com/',
    },
  },
  {
    id: 'wsj_economy',
    name: 'WSJ Economy',
    url: 'https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml',
    cat: 'macro',
    tier: 2,
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)',
      'Accept': 'application/rss+xml,text/xml,*/*',
    },
  },

  // ── 2. HARD TECH & INFRA ──────────────────────────────────────
  {
    id: 'semianalysis',
    name: 'SemiAnalysis',
    url: 'https://www.semianalysis.com/feed',
    cat: 'hardtech',
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0', 'Accept': 'application/rss+xml,text/xml,*/*' },
  },
  {
    id: 'ars_science',
    name: 'Ars Technica Science',
    url: 'http://feeds.arstechnica.com/arstechnica/science',
    cat: 'hardtech',
    tier: 2,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    id: 'ars_tech',
    name: 'Ars Technica Tech-Lab',
    url: 'http://feeds.arstechnica.com/arstechnica/technology-lab',
    cat: 'hardtech',
    tier: 2,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    id: 'utility_dive',
    name: 'Utility Dive',
    url: 'https://www.utilitydive.com/feeds/news/',
    cat: 'hardtech',
    tier: 2,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    id: 'fierce_biotech',
    name: 'FierceBiotech',
    url: 'https://www.fiercebiotech.com/rss/xml',
    cat: 'hardtech',
    tier: 2,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },

  // ── 3. ON-CHAIN & RWA ─────────────────────────────────────────
  {
    id: 'blockworks',
    name: 'Blockworks',
    url: 'https://blockworks.co/feed',
    cat: 'crypto',
    tier: 1,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
  {
    id: 'coindesk',
    name: 'CoinDesk',
    url: 'https://www.coindesk.com/arc/outboundfeeds/rss/',
    cat: 'crypto',
    tier: 2,
    headers: { 'User-Agent': 'Mozilla/5.0 MacroMonitor/1.0' },
  },
];

const NEWS_CLASSIFY_RULES = [
  // 1순위: Fed / 통화정책
  { cat: 'fed', keywords: [
    'federal reserve','fomc','powell','waller','jefferson','barr','kugler',
    'rate hike','rate cut','interest rate','quantitative tightening','quantitative easing',
    'balance sheet','reverse repurchase','sofr','iorb','ioer',
    'monetary policy','taper','fed tightening','fed easing',
    'boe rate','boj rate','ecb rate','lagarde','ueda','bailey',
  ]},
  // 2순위: 국채 / 재정
  { cat: 'treasury', keywords: [
    'treasury','t-bill','t-note','t-bond','treasury yield','yield curve',
    'treasury auction','debt issuance','tga balance','debt ceiling',
    'fiscal policy','federal deficit','federal budget',
    'quarterly refunding','qra','bessent','yellen','mnuchin',
  ]},
  // 3순위: 온체인 / 크립토 — 짧은 티커는 word 배열로 분리 관리
  { cat: 'crypto', keywords: [
    'bitcoin','cryptocurrency','crypto market','crypto exchange',
    'blockchain','defi','decentralized finance',
    'stablecoin','usdt','usdc','tether','circle',
    'real world asset','rwa tokeniz','tokenized treasury','tokenized fund',
    'spot bitcoin etf','spot ethereum etf','crypto etf','bitcoin halving',
    'coinbase','binance','kraken','bybit',
    'blockworks','on-chain','onchain',
    'solana network','ripple','cardano','avalanche avax',
    'layer-2 blockchain','lightning network',
    'crypto wallet','nft marketplace','web3','decentralized exchange','dao governance','airdrop',
    'bitcoin mining','crypto mining','hash rate','mempool',
    'grayscale','microstrategy','strategy bitcoin',
    'proof of stake','proof of work',
    'crypto regulation','crypto bill','digital asset',
    'altcoin','crypto token',
  ],
  // 짧은 티커: 단어 경계 필요 (word 키로 분리)
  word: ['btc','eth','xrp','sol','ada','bnb','avax'],
  },
  // 4순위: 하드테크 / 인프라 — macro보다 먼저 (Utility Dive 선점)
  { cat: 'hardtech', keywords: [
    // 반도체·AI
    'semiconductor','chipmaker','wafer fabrication',
    'nvidia','tsmc','amd','intel','arm holdings','qualcomm','broadcom','asml',
    'ai infrastructure','ai chip','gpu cluster','ai accelerator','hpc cluster',
    // 데이터센터
    'data center','datacenter','hyperscaler','colocation',
    // 전력·에너지 (Utility Dive 핵심)
    'power grid','electricity demand','grid operator','grid modernization',
    'ferc','pjm','miso','caiso','ercot','iso-ne','nyiso',
    'solar','wind power','offshore wind','onshore wind',
    'megawatt','gigawatt','kilowatt','mwh','gwh','kwh',
    'transmission line','distribution grid','interconnection',
    'battery storage','energy storage','long duration',
    'renewable energy','clean energy','decarbonization',
    'natural gas plant','coal plant','power plant retirement',
    'capacity market','demand response','net metering',
    'electric vehicle charging','ev charging','microgrid','smart grid',
    'smr','small modular reactor','nuclear energy',
    'doe loan','ira credit','clean energy tax',
    // 바이오·헬스
    'biotech','biopharma','fda approval','fda clearance',
    'clinical trial','phase 3','drug approval','mrna','gene therapy','gene editing','crispr',
    // 양자
    'quantum computing','quantum computer',
  ]},
  // 5순위: 거시 / 유동성
  { cat: 'macro', keywords: [
    'gdp','inflation','cpi','pce','core inflation',
    'tariff','trade war','trade deficit','sanction',
    'geopolit','recession','stagflation',
    'nfp','payroll','jobless claims','unemployment',
    'pmi','ism index',
    'dollar index','dxy','fx market','carry trade',
    'liquidity','credit spread','delinquency',
    'zerohedge','wall street journal',
  ]},
];

function newsClassify(title, summary, fallbackCat = 'macro') {
  const text = (' ' + (title + ' ' + (summary || '')).toLowerCase() + ' ');

  for (const rule of NEWS_CLASSIFY_RULES) {
    // 일반 키워드: substring 포함 여부
    const phraseHit = (rule.keywords || []).some(kw => text.includes(kw));
    if (phraseHit) return rule.cat;

    // 단어 경계 키워드 (짧은 티커 등)
    if (rule.word) {
      const wordHit = rule.word.some(kw => {
        const re = new RegExp('(?<![a-z])' + kw + '(?![a-z])', 'i');
        return re.test(text);
      });
      if (wordHit) return rule.cat;
    }
  }
  return fallbackCat;
}

function newsParseRSS(xmlText) {
  const items = [];
  const blockRe = /<(?:item|entry)[\s>]([\s\S]*?)<\/(?:item|entry)>/gi;
  let match;
  while ((match = blockRe.exec(xmlText)) !== null) {
    const block = match[1];
    const getTag = (tag) => {
      const cdataM = new RegExp(`<${tag}[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>`, 'i').exec(block);
      if (cdataM) return cdataM[1].trim();
      const plainM = new RegExp(`<${tag}[^>]*>([^<]*)`, 'i').exec(block);
      if (plainM) return plainM[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#39;/g,"'").replace(/&quot;/g,'"').trim();
      return '';
    };
    const getLinkAtom = () => {
      const m = /<link[^>]+href="([^"]+)"/i.exec(block);
      return m ? m[1] : '';
    };
    const title   = getTag('title');
    const link    = getTag('link') || getLinkAtom();
    const pubDate = getTag('pubDate') || getTag('updated') || getTag('published') || getTag('dc:date');
    const desc    = getTag('description') || getTag('summary') || getTag('content');
    const summary = desc.replace(/<[^>]*>/g,'').replace(/\s+/g,' ').trim().slice(0, 200);
    if (!title && !link) continue;
    items.push({ title, link, pubDate, summary });
  }
  return items;
}

async function newsFetchSource(src) {
  const t0 = Date.now();

  const tryFetch = async (url) => {
    const resp = await fetch(url, {
      headers: src.headers || {},
      signal: AbortSignal.timeout(10000),
      cf: { cacheTtl: 300 },
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const xml = await resp.text();
    if (!xml || xml.length < 100) throw new Error('빈 응답');
    return xml;
  };

  let xml = null;
  let usedUrl = src.url;
  let fetchErr = null;

  try {
    xml = await tryFetch(src.url);
  } catch(e) {
    fetchErr = e.message;
    if (src.fallbackUrl) {
      try {
        xml = await tryFetch(src.fallbackUrl);
        usedUrl = src.fallbackUrl;
        fetchErr = null;
      } catch(e2) {
        fetchErr = `${fetchErr} / fallback: ${e2.message}`;
      }
    }
  }

  if (!xml) {
    return { id: src.id, name: src.name, cat: src.cat, tier: src.tier||2,
             ok: false, error: fetchErr?.slice(0,120)||'오류', ms: Date.now()-t0, items: [] };
  }

  const raw = newsParseRSS(xml);
  if (!raw.length) {
    return { id: src.id, name: src.name, cat: src.cat, tier: src.tier||2,
             ok: false, error: 'XML 파싱 0건', usedUrl, ms: Date.now()-t0, items: [] };
  }

  const items = raw.slice(0, 20).map(item => ({
    ...item,
    sourceId:   src.id,
    sourceName: src.name,
    tier:       src.tier || 2,
    cat: newsClassify(item.title, item.summary, src.cat),
  }));

  return { id: src.id, name: src.name, cat: src.cat, tier: src.tier||2,
           ok: true, count: items.length, usedUrl, ms: Date.now()-t0, items };
}

async function newsFetchAll(env) {
  const results = await Promise.all(NEWS_SOURCES.map(newsFetchSource));
  const allItems = [];
  const sourceStats = [];
  for (const r of results) {
    sourceStats.push({ id: r.id, name: r.name, cat: r.cat, ok: r.ok, count: r.count||0, ms: r.ms, error: r.error||null });
    if (r.ok) allItems.push(...r.items);
  }
  allItems.sort((a, b) => {
    const da = a.pubDate ? new Date(a.pubDate).getTime() : 0;
    const db = b.pubDate ? new Date(b.pubDate).getTime() : 0;
    return db - da;
  });
  const catCount = {};
  for (const item of allItems) { catCount[item.cat] = (catCount[item.cat]||0) + 1; }
  return { fetchedAt: new Date().toISOString(), totalItems: allItems.length, sourceStats, catCount, items: allItems };
}

// isFresh=true → 캐시 무시, false → 캐시 사용
async function newsEndpoint(env, isFresh, ctx) {
  // 번역 맵 로드 (있는 것만 적용)
  const transMap = await newsLoadTransMap(env);

  if (!isFresh) {
    const cached = await kvGet(env, KV_KEYS.newsCache);
    if (cached) {
      applyTransMapToItems(cached.items || [], transMap);
      return new Response(JSON.stringify({ ...cached, _fromCache: true }), { headers: CORS });
    }
  }

  // RSS 파싱 후 번역 적용 (캐시에 있는 것만) → 즉시 반환
  const data = await newsFetchAll(env);
  applyTransMapToItems(data.items, transMap);

  const putP = kvPut(env, KV_KEYS.newsCache, data, KV_TTL.newsCache);
  if (ctx?.waitUntil) ctx.waitUntil(putP); else await putP;

  return new Response(JSON.stringify(data), { headers: CORS });
}
function newsTransKey(item) {
  return (item.link || item.title || '').slice(0, 200);
}

// 번역 캐시 맵 로드 (KV → JS 객체)
async function newsLoadTransMap(env) {
  try {
    const raw = await env.MMF_KV.get(KV_KEYS.newsTransMap, { type: 'text' });
    return raw ? JSON.parse(raw) : {};
  } catch(e) {
    return {};
  }
}

// 번역 맵을 기사 배열에 in-place 적용
function applyTransMapToItems(items, transMap) {
  for (const item of items) {
    const key = newsTransKey(item);
    if (key && transMap[key]) {
      item.titleKo   = transMap[key].titleKo   || null;
      item.summaryKo = transMap[key].summaryKo || null;
    }
  }
}

async function auctionResults(url) {
  // fiscaldata.treasury.gov가 CF Worker IP 차단(525) →
  // GitHub Actions가 4시간마다 수집한 data/auction_results.json 사용
  const RAW = 'https://raw.githubusercontent.com/z66g/macromonitor/main/data/auction_results.json';

  const type  = url.searchParams.get('type');   // 필터: Note/Bond/Bill 등
  const limit = parseInt(url.searchParams.get('limit') || '10');
  const date  = url.searchParams.get('date');   // 특정 경매일 이후

  try {
    const r = await fetch(RAW, { cf: { cacheTtl: 300 } });
    if (!r.ok) throw new Error(`GitHub raw ${r.status}`);
    const d = await r.json();
    let results = d.results || [];

    // 필터 적용
    if (type) results = results.filter(a => a.security_type === type);
    if (date) results = results.filter(a => (a.auction_date || '') >= date);
    results = results.slice(0, limit);

    return new Response(JSON.stringify({
      updated: d.updated,
      count: results.length,
      results,
    }), { headers: { ...CORS } });
  } catch(e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { ...CORS } });
  }
}

// ══════════════════════════════════════════════════════
//  Telegram 알림 헬퍼
// ══════════════════════════════════════════════════════

/**
 * Telegram Bot API로 메시지 발송
 * @param {object} env - Cloudflare Worker env (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
 * @param {string} text - HTML 파싱 모드 메시지
 */
async function sendTelegram(env, text) {
  const token  = env?.TELEGRAM_BOT_TOKEN;
  const chatId = env?.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    console.warn('[TELEGRAM] secrets 없음, 스킵');
    return;
  }
  const resp = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
    }),
  });
  if (!resp.ok) {
    const err = await resp.text();
    throw new Error(`Telegram API ${resp.status}: ${err}`);
  }
  const result = await resp.json();
  if (!result.ok) throw new Error(`Telegram error: ${JSON.stringify(result)}`);
  console.log('[TELEGRAM] 발송 완료');
}

/**
 * 뉴스 다이제스트 Telegram 포맷 조립 후 발송
 */
async function sendTelegramDigest(env, digest) {
  if (!digest?.items?.length) return;

  // KST 시간
  const now    = new Date();
  const kst    = new Date(now.getTime() + 9 * 3600_000);
  const mm     = String(kst.getUTCMonth() + 1).padStart(2, '0');
  const dd     = String(kst.getUTCDate()).padStart(2, '0');
  const hh     = String(kst.getUTCHours()).padStart(2, '0');
  const min    = String(kst.getUTCMinutes()).padStart(2, '0');
  const dateStr = `${mm}-${dd}`;
  const timeStr = `${hh}:${min} KST`;

  const lines = digest.items.map((item, i) =>
    `${i + 1}. ${item.summary || item.title}`
  ).join('\n');

  const text =
    `📰 <b>MacroMonitor 뉴스</b> | ${dateStr} ${timeStr}\n\n` +
    `${lines}\n\n` +
    `🔗 macromonitor.zbbg.kr`;

  await sendTelegram(env, text);
}

/**
 * 누적된 다이제스트를 한 번에 발송 (중복 제거)
 * KST 06:00(9건) / KST 21:00(15건) 배치 발송용
 */
async function sendBatchedTelegramDigest(env) {
  const hist = await kvGet(env, KV_KEYS.newsDigestHistory);
  if (!hist?.digests?.length) {
    console.log('[BATCHED DIGEST] 누적 히스토리 없음, 스킵');
    return;
  }

  // 전체 아이템 수집 + 중복 제거 (title 기준 소문자 정규화)
  const seen = new Set();
  const allItems = [];
  for (const d of hist.digests) {
    for (const item of (d.items || [])) {
      const key = (item.title || item.summary || '').toLowerCase().trim().slice(0, 80);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      allItems.push(item);
    }
  }

  console.log(`[BATCHED DIGEST] ${hist.digests.length}개 다이제스트 → ${allItems.length}건 (중복 제거 후)`);

  if (allItems.length === 0) return;

  await sendTelegramDigest(env, {
    items: allItems,
    generatedAt: new Date().toISOString(),
  });

  // 발송 완료 → 히스토리 초기화
  await kvPut(env, KV_KEYS.newsDigestHistory, { digests: [] }, KV_TTL.newsDigestHistory);
}
