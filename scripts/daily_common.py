"""데일리 리포트 파이프라인 공용 — '지금 있어야 할 세션'과 단계별 상태.

GitHub Actions의 schedule은 30분~3시간 늦거나 아예 건너뛴다(2026-11-16 daily-facts 미실행 사례).
그래서 팩트시트는 cron-job.org dispatch가 주 트리거이고, 이 모듈로 각 단계가 '오늘 세션이 처리됐나'를 판단한다.

사용:
  python3 scripts/daily_common.py expected               # 리포트 에이전트용: 지금 기준 마지막 완료 세션 (장 마감 21:15 UTC 이후)
  python3 scripts/daily_common.py status [--cutoff HH:MM] # JSON: {expected, facts, draft, report, missing}
"""
import datetime as dt
import json
import os
import sys

BASE = 'data/daily'

# NYSE 휴장일 (조기 폐장일은 세션으로 친다). 2028년분은 2027년 말까지 추가할 것 — 헬스체크가 소진 전 경고.
NYSE_HOLIDAYS = {
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19',
    '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
    '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18',
    '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24',
}
HOLIDAY_HORIZON = max(NYSE_HOLIDAYS)[:4]


def is_session(d):
    return d.weekday() < 5 and d.isoformat() not in NYSE_HOLIDAYS


def expected_session(now=None, cutoff=dt.time(21, 15)):
    """now(UTC) 기준으로 '처리돼 있어야 할' 가장 최근 세션일.
    세션 D는 D일 cutoff(UTC) 이후부터 센다. 21:15 UTC = 16:15 ET(겨울) — 여름엔 한 시간 보수적."""
    now = now or dt.datetime.now(dt.timezone.utc)
    d = now.date() if now.time() >= cutoff else now.date() - dt.timedelta(days=1)
    while not is_session(d):
        d -= dt.timedelta(days=1)
    return d.isoformat()


def status(now=None, cutoff=dt.time(21, 15), base=BASE):
    sess = expected_session(now, cutoff)
    has = {k: os.path.exists(os.path.join(base, sess, f'{k}.json')) for k in ('facts', 'draft', 'report')}
    missing = None
    if not has['facts']:
        missing = 'facts'
    elif not (has['draft'] or has['report']):
        missing = 'draft'
    elif not has['report']:
        missing = 'report'
    return {'expected': sess, **has, 'missing': missing}


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'expected'
    cut = dt.time(21, 15)
    if '--cutoff' in sys.argv:
        h, m = sys.argv[sys.argv.index('--cutoff') + 1].split(':')
        cut = dt.time(int(h), int(m))
    if cmd == 'expected':
        print(expected_session(cutoff=cut))
    elif cmd == 'status':
        print(json.dumps(status(cutoff=cut), ensure_ascii=False))
    else:
        sys.exit(f'알 수 없는 명령: {cmd}')
