#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -f infra/compose.yaml)
endpoints="http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379"

"${compose[@]}" exec -T etcd-1 etcdctl --endpoints="$endpoints" endpoint health --cluster
member_count=$("${compose[@]}" exec -T etcd-1 etcdctl --endpoints="$endpoints" member list -w json \
  | python -c 'import json,sys; print(len(json.load(sys.stdin)["members"]))')
if [[ "$member_count" != "3" ]]; then
  echo "expected exactly 3 etcd members; found $member_count" >&2
  exit 1
fi
echo "etcd smoke test passed: 3 healthy members"
