#!/usr/bin/env bash
# 데이터 파일 커밋 — 여러 워크플로가 동시에 main에 push해도 깨지지 않도록 rebase + 재시도.
# 사용: scripts/commit_data.sh "커밋 메시지" data/a.json [data/b.json ...]
set -euo pipefail
msg="$1"; shift
git config user.name  "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add "$@"
if git diff --staged --quiet; then
  echo "변경 없음 — 커밋 생략"
  exit 0
fi
git commit -m "$msg"
for i in 1 2 3 4 5; do
  if git pull --rebase --autostash origin main && git push origin HEAD:main; then
    echo "push 완료 (시도 $i)"
    exit 0
  fi
  echo "push 실패 — $((i * 5))초 후 재시도"
  sleep $((i * 5))
done
echo "push 5회 실패" >&2
exit 1
