"""data/health.json → GitHub 이슈 생성/갱신/종료 (health-check.yml, health-alert.yml 공용)."""
import json, subprocess

r = json.load(open('data/health.json', encoding='utf-8'))
status = r.get('status', 'fail')
icon = {'ok': '✅', 'warn': '⚠️', 'fail': '❌'}
bad = [c for c in r.get('checks', []) if c['status'] != 'ok']

def gh(*args):
    return subprocess.run(['gh', *args], capture_output=True, text=True, check=True).stdout.strip()

gh('label', 'create', 'health', '--color', 'D93F0B', '--description', 'MacroMonitor 자동 헬스체크', '--force')
open_issues = json.loads(gh('issue', 'list', '--label', 'health', '--state', 'open', '--json', 'number,body'))

if status == 'ok':
    for i in open_issues:
        gh('issue', 'close', str(i['number']), '--comment', f"✅ {r['checked_at']} 헬스체크 정상 — 자동 종료")
    print('정상 — 열린 이슈', len(open_issues), '건 종료')
    raise SystemExit(0)

lines = [f"**종합: {icon[status]} {status}** · 점검 {r['checked_at']}", '', '| 상태 | 항목 | 내용 |', '|---|---|---|']
lines += [f"| {icon[c['status']]} | {c['name']} | {str(c['detail'])[:300]} |" for c in r.get('checks', [])]
diag = r.get('diagnosis') or []
if diag:
    lines += ['', '### 에이전트 진단']
    lines += [f"- **{d.get('check')}** — {d.get('likely_cause')} → {d.get('suggested_fix')} (확신도 {d.get('confidence')})" for d in diag]
body = '\n'.join(lines)
names = ', '.join(c['name'] for c in bad)

if open_issues:
    num = str(open_issues[0]['number'])
    prev_body = open_issues[0]['body'] or ''
    gh('issue', 'edit', num, '--body', body)
    new = [c['name'] for c in bad if f"| {c['name']} |" not in prev_body or f"| ✅ | {c['name']} |" in prev_body]
    if new or status == 'fail':
        gh('issue', 'comment', num, '--body', f"{icon[status]} 새로 발생/악화: {', '.join(new) or names}")
    print('이슈 갱신 #' + num)
else:
    url = gh('issue', 'create', '--label', 'health', '--title', f"{icon[status]} MacroMonitor 헬스체크: {names}"[:120], '--body', body)
    print('이슈 생성', url)
