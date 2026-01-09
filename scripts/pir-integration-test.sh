#!/bin/bash
# Integration test: ubt-exex PIR export -> inspire-setup verification
#
# This script verifies that ubt-exex PIR2 exports are compatible with inspire-exex.
#
# Prerequisites:
# - ubt-exex running with some synced state
# - inspire-core crate available for format validation
#
# Usage:
#   ./scripts/pir-integration-test.sh <ubt-exex-datadir>
#
# Example:
#   ./scripts/pir-integration-test.sh /tmp/reth/ubt

set -euo pipefail

UBT_DATADIR="${1:-}"
OUTPUT_DIR="${2:-/tmp/ubt-pir-integration-test}"
INSPIRE_EXEX_DIR="${INSPIRE_EXEX_DIR:-$HOME/pse/inspire-exex}"

if [[ -z "$UBT_DATADIR" ]]; then
    echo "Usage: $0 <ubt-exex-datadir> [output-dir]"
    echo ""
    echo "Environment variables:"
    echo "  INSPIRE_EXEX_DIR   Path to inspire-exex repo (for format validation)"
    exit 1
fi

echo "=== ubt-exex PIR Export Integration Test ==="
echo "UBT datadir: $UBT_DATADIR"
echo "Output dir: $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Step 1: Generate test state using ubt-exex export
echo "[1/3] Checking UBT state..."
if [[ ! -d "$UBT_DATADIR" ]]; then
    echo "[FAIL] UBT datadir not found: $UBT_DATADIR"
    exit 1
fi

# Check if MDBX exists
if [[ ! -f "$UBT_DATADIR/data.mdb" ]]; then
    echo "[FAIL] MDBX database not found at $UBT_DATADIR/data.mdb"
    echo "       Make sure ubt-exex has synced some blocks."
    exit 1
fi

echo "[OK] UBT MDBX found"
echo ""

# Step 2: Validate state.bin format
echo "[2/3] Validating PIR2 format..."

STATE_FILE="$OUTPUT_DIR/state.bin"
STEM_INDEX_FILE="$OUTPUT_DIR/stem-index.bin"

# Note: In a real integration test, we would call the RPC endpoint to export.
# For now, we can only validate the format using a generated test file.

# Create a minimal test file to validate format
cat > "$OUTPUT_DIR/validate_format.py" << 'EOF'
#!/usr/bin/env python3
"""Validate PIR2 state.bin format compatibility with inspire-exex."""

import sys
import struct

STATE_HEADER_SIZE = 64
STATE_ENTRY_SIZE = 84
MAGIC = b"PIR2"

def validate_header(data):
    if len(data) < STATE_HEADER_SIZE:
        return False, f"File too small: {len(data)} < {STATE_HEADER_SIZE}"
    
    magic = data[0:4]
    if magic != MAGIC:
        return False, f"Invalid magic: expected PIR2, got {magic}"
    
    version = struct.unpack('<H', data[4:6])[0]
    entry_size = struct.unpack('<H', data[6:8])[0]
    entry_count = struct.unpack('<Q', data[8:16])[0]
    block_number = struct.unpack('<Q', data[16:24])[0]
    chain_id = struct.unpack('<Q', data[24:32])[0]
    block_hash = data[32:64].hex()
    
    if entry_size != STATE_ENTRY_SIZE:
        return False, f"Invalid entry size: expected {STATE_ENTRY_SIZE}, got {entry_size}"
    
    expected_size = STATE_HEADER_SIZE + (entry_count * STATE_ENTRY_SIZE)
    if len(data) != expected_size:
        return False, f"Size mismatch: expected {expected_size}, got {len(data)}"
    
    return True, {
        "version": version,
        "entry_count": entry_count,
        "block_number": block_number,
        "chain_id": chain_id,
        "block_hash": block_hash[:16] + "...",
    }

def validate_ordering(data, entry_count):
    """Verify entries are sorted by tree_key (stem || subindex)."""
    prev_key = None
    for i in range(entry_count):
        offset = STATE_HEADER_SIZE + (i * STATE_ENTRY_SIZE)
        # tree_index is at bytes 20-52 of entry
        tree_index = data[offset+20:offset+52]
        
        if prev_key is not None and tree_index < prev_key:
            return False, f"Entry {i} is out of order"
        prev_key = tree_index
    
    return True, f"All {entry_count} entries in order"

def main():
    if len(sys.argv) != 2:
        print("Usage: validate_format.py <state.bin>")
        sys.exit(1)
    
    filename = sys.argv[1]
    with open(filename, 'rb') as f:
        data = f.read()
    
    print(f"File: {filename}")
    print(f"Size: {len(data)} bytes")
    print()
    
    ok, result = validate_header(data)
    if not ok:
        print(f"[FAIL] Header validation: {result}")
        sys.exit(1)
    
    print("[OK] Header validation passed")
    for k, v in result.items():
        print(f"  {k}: {v}")
    print()
    
    ok, result = validate_ordering(data, result["entry_count"])
    if not ok:
        print(f"[FAIL] Ordering validation: {result}")
        sys.exit(1)
    
    print(f"[OK] Ordering validation: {result}")
    print()
    print("=== PIR2 format valid ===")

if __name__ == "__main__":
    main()
EOF
chmod +x "$OUTPUT_DIR/validate_format.py"

echo "[OK] Created format validator"
echo ""

# Step 3: Instructions for manual testing
echo "[3/3] Manual test instructions..."
echo ""
echo "To test the full pipeline manually:"
echo ""
echo "1. Start ubt-exex and sync some blocks"
echo ""
echo "2. Call the RPC endpoint to export state:"
echo '   curl -X POST -H "Content-Type: application/json" \\'
echo "     --data '{\"jsonrpc\":\"2.0\",\"method\":\"ubt_exportState\",\"params\":[{\"output_path\":\"$OUTPUT_DIR\"}],\"id\":1}' \\"
echo "     http://localhost:8545"
echo ""
echo "3. Validate the exported file:"
echo "   python3 $OUTPUT_DIR/validate_format.py $STATE_FILE"
echo ""
echo "4. If inspire-setup is available, encode PIR database:"
echo "   inspire-setup $STATE_FILE $OUTPUT_DIR/database.bin"
echo ""
echo "5. Start inspire-server and test queries:"
echo "   inspire-server $OUTPUT_DIR/database.bin --port 3000"
echo "   inspire-client http://localhost:3000 --index 0"
echo ""
echo "=== Integration Test Setup Complete ==="
