"""국채경매 결과 텔레그램 발송 — 요약이 완성됐고 아직 발송하지 않은 이표채만.

- 클라우드 에이전트가 summary를 커밋하면 auction-telegram.yml이 이 스크립트를 실행
- 발송 성공한 레코드에 telegram_sent=True 를 기록 → 중복 발송 방지 (워크플로가 커밋)
- Gemini/에이전트가 쓴 텍스트는 html.escape 후 삽입 (parse_mode=HTML 400 오류 방지)
- 4096자 제한을 넘으면 경매 단위로 나눠 여러 메시지로 발송
"""
import datetime
import html
import json
import os
import sys
import urllib.request

PATH = 'data/auction_results.json'
MAX_AGE_DAYS = 4          # 오래된 경매를 뒤늦게 요약해도 알림 폭탄이 되지 않도록
LIMIT = 3900

token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
chat = os.environ.get('TELEGRAM_CHAT_ID', '')
if not token or not chat:
    sys.exit('TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 없음')

with open(PATH, encoding='utf-8') as f:
    data = json.load(f)

today = datetime.date.today()
targets = [r for r in data['results']
           if r.get('security_type') in ('Note', 'Bond', 'TIPS')
           and r.get('summary') and not r.get('telegram_sent')
           and (today - datetime.date.fromisoformat(r['auction_date'])).days <= MAX_AGE_DAYS]
if not targets:
    print('발송 대상 없음')
    sys.exit(0)


def fmt(v, dec=2):
    return f'{v:.{dec}f}' if isinstance(v, (int, float)) else 'N/A'


def mark(g):
    return {'strong': '✅', 'neutral': '🟡', 'weak': '⚠️'}.get(g or '', '⚪')


def overall_emoji(summary):
    t = summary.lower()
    for word, e in (('disastrous', '🚨'), ('weak', '⚠️'), ('average', '😐'), ('strong', '💪')):
        if word in t:
            return e
    return '📊'


def block(r):
    an, gr = r['_analysis'], r['_analysis'].get('grade', {})
    lines = r['summary'].strip().splitlines()
    head = html.escape(lines[0]) if lines else ''
    out = [f"\n━━━ {html.escape(r['security_term'])} {r['security_type']}  ${fmt((r.get('offering_amt') or 0) / 1e9, 1)}B ━━━",
           f"{overall_emoji(r['summary'])} {head}"]
    if isinstance(an.get('tail_bps'), (int, float)):
        out.append(f"• Tail: {an['tail_bps']:+.1f}bp {mark(gr.get('tail'))}")
    out.append(f"• BTC: {fmt(an.get('btc'))}x (6avg {fmt(an.get('btc_avg6'))}x) {mark(gr.get('btc'))}")
    out.append(f"• Indirect: {fmt(an.get('in_pct'), 1)}% (6avg {fmt(an.get('in_avg6'), 1)}%)")
    out.append(f"• PD: {fmt(an.get('pd_pct'), 1)}% (6avg {fmt(an.get('pd_avg6'), 1)}%) {mark(gr.get('pd'))}")
    detail = html.escape('\n'.join(lines[1:]).strip())
    if detail:
        out.append(detail)
    return '\n'.join(out) + '\n'


def send(text):
    body = json.dumps({'chat_id': chat, 'text': text, 'parse_mode': 'HTML',
                       'disable_web_page_preview': True}).encode('utf-8')
    req = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=body,
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=20) as resp:
        res = json.loads(resp.read())
    if not res.get('ok'):
        raise RuntimeError(f'Telegram 오류: {res}')


targets.sort(key=lambda r: (r['auction_date'], r['security_term']))
dates = sorted({r['auction_date'] for r in targets})
header = f"🏦 <b>국채경매 결과</b> | {' · '.join(dates)}\n"
footer = '\n🔗 macromonitor.zbbg.kr'

# 경매 단위로 메시지를 채우되 한도를 넘으면 다음 메시지로
messages, cur, members, groups = [], header, [], []
for r in targets:
    b = block(r)
    if len(cur) + len(b) + len(footer) > LIMIT and members:
        messages.append(cur + footer)
        groups.append(members)
        cur, members = header, []
    cur += b
    members.append(r)
messages.append(cur + footer)
groups.append(members)

sent = 0
for text, members in zip(messages, groups):
    send(text)                       # 실패 시 예외 → 이 그룹부터 미표시로 남아 다음 실행에서 재시도
    for r in members:
        r['telegram_sent'] = True
        sent += 1
    with open(PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

print(f'✅ 발송 완료: {sent}건 / 메시지 {len(messages)}개')
