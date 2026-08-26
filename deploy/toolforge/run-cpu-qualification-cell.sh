#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: run-cpu-qualification-cell.sh <nlwiki|ptwiki|frwiki> <cpu> <threads> <weekly-workers>" >&2
  exit 2
fi

wiki=$1
cpu=$2
threads=$3
weekly_workers=$4
case "$wiki" in nlwiki|ptwiki|frwiki) ;; *) echo "Unsupported qualification wiki: $wiki" >&2; exit 2 ;; esac
case "$cpu:$threads:$weekly_workers" in
  1:1:1|2:2:1|4:3:1|4:3:2) ;;
  *) echo "Unsupported qualification profile: $cpu:$threads:$weekly_workers" >&2; exit 2 ;;
esac

export WIKI_ECON_REQUESTED_CPU="$cpu"
export WIKI_ECON_QUALIFICATION_THREADS="$threads"
export WIKI_ECON_WEEKLY_WORKERS="$weekly_workers"
exec "$(dirname "$0")/run-capacity-benchmark.sh" "$wiki" 256
