#!/usr/bin/env bash
# 样例 4：交付切片 —— 阻断项不保存切片、补传后同号重试、幂等重放、
#         观测号集合不同 409、固化内容不随后续修订变化。
set -euo pipefail

API_A="${API_A:-http://localhost:8001}"
API_B="${API_B:-http://localhost:8002}"

pretty() {
  if command -v jq >/dev/null 2>&1; then jq .;
  elif command -v python3 >/dev/null 2>&1; then python3 -m json.tool;
  else cat; fi
}

# 发请求并美化输出响应体，最后单独标注 HTTP 状态码
post() {
  local out code
  out=$(mktemp)
  code=$(curl -sS -o "$out" -w '%{http_code}' "$@")
  pretty < "$out"
  echo "HTTP $code"
  rm -f "$out"
}

echo "== 1) 准备观测：OBS-D1、OBS-D2 有效；OBS-D3 待补齐；OBS-D4 冲突 =="
curl -sS -X POST "$API_A/packages" -H 'Content-Type: application/json' -d '{
  "revisions": [
    {"observation_id": "OBS-D1", "revision_id": "rev-d101", "parents": [],
     "species": "Cervus elaphus", "count": 3},
    {"observation_id": "OBS-D2", "revision_id": "rev-d201", "parents": [],
     "species": "Lynx lynx", "count": 2},
    {"observation_id": "OBS-D3", "revision_id": "rev-d302", "parents": ["rev-d301"],
     "species": "Vulpes vulpes", "count": 4},
    {"observation_id": "OBS-D4", "revision_id": "rev-d401a", "parents": [],
     "species": "Bubo bubo", "count": 1},
    {"observation_id": "OBS-D4", "revision_id": "rev-d401b", "parents": [],
     "species": "Bubo bubo", "count": 2}
  ]
}' | pretty

echo
echo "== 2) 发布交付 BATCH-D：OBS-D1/OBS-D2 可用，OBS-D3 待补齐、OBS-D4 冲突、OBS-D9 不存在 =="
echo "   → 422 列出全部阻断项，不保存切片"
post -X POST "$API_A/deliveries" -H 'Content-Type: application/json' -d '{
  "delivery_id": "BATCH-D",
  "observation_ids": ["OBS-D1", "OBS-D2", "OBS-D3", "OBS-D4", "OBS-D9"]
}'

echo
echo "== 3) 阻断切片未被保存：从 api-b 查询 → 404 =="
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$API_B/deliveries/BATCH-D"

echo
echo "== 4) 补传缺失祖先 OBS-D3 的父修订，并人工裁决 OBS-D4 =="
curl -sS -X POST "$API_B/packages" -H 'Content-Type: application/json' -d '{
  "revisions": [
    {"observation_id": "OBS-D3", "revision_id": "rev-d301", "parents": [],
     "species": "Vulpes vulpes", "count": 4}
  ]
}' >/dev/null
curl -sS -X POST "$API_A/observations/OBS-D4/resolutions" -H 'Content-Type: application/json' -d '{
  "revision_id": "rev-d402", "parents": ["rev-d401a", "rev-d401b"],
  "species": "Bubo bubo", "count": 1
}' >/dev/null

echo
echo "== 5) 同一交付号去掉不存在的 OBS-D9 后重试 → 201，一次性固化四项 =="
post -X POST "$API_A/deliveries" -H 'Content-Type: application/json' -d '{
  "delivery_id": "BATCH-D",
  "observation_ids": ["OBS-D1", "OBS-D2", "OBS-D3", "OBS-D4"]
}'

echo
echo "== 6) 从 api-b 查询同一切片：两进程结论一致（含物种、数量、删除标记、修订号） =="
curl -sS "$API_B/deliveries/BATCH-D" | pretty

echo
echo "== 7) 同号同集合重试（顺序不同也算同集合）→ 200 原切片，existing=true，内容不变 =="
post -X POST "$API_B/deliveries" -H 'Content-Type: application/json' -d '{
  "delivery_id": "BATCH-D",
  "observation_ids": ["OBS-D4", "OBS-D2", "OBS-D1", "OBS-D3"]
}'

echo
echo "== 8) 后续汇交新修订 OBS-D1：观测当前值改变，但已交付切片不变 =="
curl -sS -X POST "$API_B/packages" -H 'Content-Type: application/json' -d '{
  "revisions": [
    {"observation_id": "OBS-D1", "revision_id": "rev-d102", "parents": ["rev-d101"],
     "species": "Cervus elaphus", "count": 8}
  ]
}' >/dev/null
echo "-- 观测当前有效值（已改为 8）："
curl -sS "$API_A/observations/OBS-D1" | pretty
echo "-- 已交付切片仍为固化时的 rev-d101 / count=3："
curl -sS "$API_B/deliveries/BATCH-D" | pretty

echo
echo "== 9) 同号但观测号集合不同 → 409，不覆盖已交付内容 =="
post -X POST "$API_A/deliveries" -H 'Content-Type: application/json' -d '{
  "delivery_id": "BATCH-D",
  "observation_ids": ["OBS-D1", "OBS-D2", "OBS-D3"]
}'

echo
echo "== 10) 交付切片列表（供核对批次） =="
curl -sS "$API_B/deliveries" | pretty
