#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo "Installing side-by-side Python 3.11 tooling..."
sudo apt update
sudo apt install -y python3.11 python3.11-venv

echo "Creating .venv with Python 3.11..."
rm -rf .venv
python3.11 -m venv .venv
. .venv/bin/activate

python --version
pip install --upgrade pip
pip install -r requirements.txt

echo "Ensuring hostname 'kafka' resolves for tester data_generator.py..."
if ! getent hosts kafka >/dev/null; then
  echo "127.0.0.1 kafka" | sudo tee -a /etc/hosts >/dev/null
fi

echo
echo "Ready. Start stack, then run generator like interviewer environment:"
echo "  docker compose up -d zookeeper kafka kafka-ui postgres"
echo "  source .venv/bin/activate"
echo "  python data_generator.py"
