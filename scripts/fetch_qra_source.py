"""재무부 최신 'Treasury Announces Marketable Borrowing Estimates' 원문 → data/qra_source.json.

클라우드 에이전트 세션은 네트워크 정책상 treasury.gov에 접근할 수 없으므로,
GitHub Actions가 원문을 받아 레포에 저장하고 에이전트는 이 파일만 읽어 수치를 추출한다.
apply_qra.py는 에이전트의 evidence_quote가 이 원문에 글자 그대로 있는지 확인한다.
"""
import datetime
import html
import json
import os
import re
import sys
import urllib.request

PATH = 'data/qra_source.json'
BASE = 'https://home.treasury.gov'
TITLE = 'Treasury Announces Marketable Borrowing Estimates'
UA = {'User-Agent': 'Mozilla/5.0 (compatible; MacroMonitor/1.0)'}


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
        return r.read().decode('utf-8', errors='ignore')


def text_of(page):
    t = re.sub(r'<script.*?</script>|<style.*?</style>', '', page, flags=re.S)
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', t))).strip()


# 1. '최근 분기 리펀딩 문서' 페이지의 Financing Estimates 링크 (보도자료 목록은 최근 10건만 보여줘 부적합)
DOCS = f'{BASE}/policy-issues/financing-the-government/quarterly-refunding/most-recent-quarterly-refunding-documents'
m = re.search(r'<a href="?(/news/press-releases/[a-z]{2}\d+)/?"?[^>]*>\s*Financing Estimates[^<]*</a>', get(DOCS))
if not m:
    sys.exit('최근 리펀딩 문서 페이지에서 Financing Estimates 링크를 찾지 못함 — 기존 파일 유지')
link = BASE + m.group(1)

# 2. 원문 본문 추출 (제목 ~ 마지막 수치 문단)
full = text_of(get(link))
start = full.find(TITLE + ' ', full.find('Press Releases'))
start = full.find(TITLE, start if start >= 0 else 0)
body = full[start:start + 6000]
end = body.find('###')
body = body[:end] if end > 0 else body
date_m = re.search(TITLE + r' ([A-Z][a-z]+ \d{1,2}, \d{4})', body)
if 'WASHINGTON' not in body or 'billion' not in body or not date_m:
    sys.exit(f'원문 구조가 예상과 다름 ({link}) — 기존 파일 유지')
ann = datetime.datetime.strptime(date_m.group(1), '%B %d, %Y').date().isoformat()

prev = json.load(open(PATH, encoding='utf-8')) if os.path.exists(PATH) else {}
if prev.get('url') == link and prev.get('text') == body:
    print(f'변경 없음: {link} ({ann})')
    sys.exit(0)
with open(PATH, 'w', encoding='utf-8') as f:
    json.dump({'url': link, 'title': TITLE, 'announcement_date': ann, 'text': body,
               'fetched_at': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')},
              f, indent=2, ensure_ascii=False)
print(f'저장: {link} ({ann}) {len(body)}자')
