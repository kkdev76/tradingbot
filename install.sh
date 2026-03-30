#!/bin/bash
set -e

echo "=== Updating system packages ==="
sudo apt update

echo "=== Installing Python and pip ==="
sudo apt install -y python3 python3-pip python3-venv

echo "=== Creating virtual environment ==="
python3 -m venv venv
source venv/bin/activate

echo "=== Installing dependencies ==="
pip install --upgrade pip
pip install -r requirements.txt

echo "=== Setting up git config ==="
git config --global user.name "Ramnath"
git config --global user.email "dgramnath@gmail.com"

echo ""
echo "=== Setup complete ==="
echo "To activate the virtual environment, run:"
echo "  source venv/bin/activate"
echo ""
echo "Make sure crypto.env exists with your API keys and config before running."
echo "To start the bot:"
echo "  python3 multi_symbol_macd_stream.py"
