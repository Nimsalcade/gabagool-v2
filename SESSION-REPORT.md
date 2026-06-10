# Gabagool v2 — First Live Session Report

**Date:** 2026-06-10
**Wallet:** `0x3CbC3DCB6a2a618420AA95F83CeAeF1B0661d5d6` (Deposit Wallet)
**Starting balance:** $18.51 pUSD

---

## 1. Pre-flight

`make check` revealed 3 failures:

| Check | Status | Root cause |
|---|---|---|
| trading approvals | FAIL | SDK tried to `setApprovalForAll` for CtfAutoRedeem (`0xF3cFb6a6eBFeB51876289Eb235719EB1C65252B0`), a new contract (~42 days old) not yet in the Polymarket relayer's allowlist |
| pUSD balance | FAIL | CLOB API returned $0 because the SDK defaulted to `signature_type=1` (Proxy) for a Deposit Wallet. The on-chain wallet held $18.51 |
| merge proof | FAIL | Depended on the above two |

### Fixes applied before go-live

1. **On-chain pUSD balance fallback** (`src/inventory.py`): Added `eth_call` to the pUSD ERC-20 contract via public Polygon RPCs when the CLOB API returns $0
2. **CtfAutoRedeem approval made non-fatal** (`src/sdk.py`): The approval batch failure (call #4) is caught and logged as a warning since core trading contracts were already approved via the Polymarket web UI
3. **Merge proof retry fix** (`tools/test_merge.py`): `_settled_holding` had a truthiness bug where `want_min_pairs=0` caused it to break immediately without waiting for the Data API to index the merge
4. **Config added** (`config/default.yaml`, `src/config.py`): `clob.signature_type: 3` for POLY_1271 deposit wallets

After fixes: **all 7 checks passed**, merge proof written, go-live authorized.

---

## 2. Live session — 14:30 to 14:59 UTC

### Windows traded
- **BTC 10:30-10:45** (14:30-14:45 UTC)
- **ETH 10:30-10:45** (14:30-14:45 UTC)
- **BTC 10:45-11:00** (14:45-15:00 UTC)
- **ETH 10:45-11:00** (14:45-15:00 UTC)

### Surface metrics
- **Merges:** 17 successful, 1 failed
- **Merge volume:** $118.63 (total recycled through merge engine)
- **Starting pUSD:** $18.51
- **Ending pUSD:** $2.47
- **Plus $27.01 in auto-redeems** (directional luck — see §4)
- **Ending total equity:** ~$29.48

---

## 3. What went wrong — the quoting budget bug

### Symptom
Window-end reports showed combined entry prices **above $1.00**:

```
maker.btc.1800 window done | UP 47@0.340 DOWN 46@0.684 | combined=1.0242
maker.eth.1800 window done | UP 41@0.398 DOWN 41@0.614 | combined=1.0120
```

A pair bought at $1.0242 and merged for $1.00 is a **guaranteed 2.4% loss**.

### Root cause
In `src/maker_loop.py:_requote()`, when the **imbalance brake** paused one side (cancelled its resting order), the active side's quote was capped against `cap_against_resting()`. This function falls back to the **theoretical fair-split target** when no resting price exists on the other side — a price derived from current mid-market values, not what the paused side actually filled at.

Example:
- UP fills 10 shares at average $0.40
- Imbalance brake pauses UP, cancels its order
- DOWN re-quotes: `cap_against_resting` uses mid-based target ~$0.24 instead of actual $0.40
- DOWN gets capped at `$0.97 - $0.24 = $0.73`
- Combined with actual UP fill: `$0.40 + $0.73 = $1.13` → 13% loss

### Fix
Modified `_requote()` to use the **actual average fill price** of the paused side when no resting order exists:

```python
if effective_other is None:
    other_leg = self.tracker.down if side == "UP" else self.tracker.up
    if other_leg.shares > 0 and other_leg.cost > 0:
        effective_other = other_leg.cost / other_leg.shares
    else:
        effective_other = other_target
```

Now DOWN would be capped at `$0.97 - $0.40 = $0.57`, keeping combined ≤ $0.97.

### One failed merge
ETH market `0x4b2fe8dc2e` — merge tx confirmed but on-chain positions did not shrink. Likely a relayer race condition (two merges submitted close together). The merge engine correctly detected this and refused to credit it.

---

## 4. Auto-redeems masked the loss

The Polymarket account had **auto-redeem enabled** in settings. After window resolution, naked directional positions were automatically redeemed:

| Market | Outcome | Shares | Value |
|---|---|---|---|
| BTC 10:45-11:00 | UP won | 11+ shares | +$11.01 |
| ETH 10:45-11:00 | UP won | 16 shares | +$16.00 |

These were **not bot-initiated redeems** — they were Polymarket's auto-redeem feature acting on the bot's naked directional inventory. Both happened to be UP which won. If the market had gone DOWN, those positions would be worth $0.

**Without the auto-redeems:** ending balance would be ~$2.47 (a $16 loss on $18.51 starting capital).

**With auto-redeems:** ending balance ~$29.48 (appears profitable but was directional luck).

---

## 5. All changes implemented

| File | Change |
|---|---|
| `src/inventory.py` | On-chain pUSD balance fallback via `eth_call` |
| `src/sdk.py` | CtfAutoRedeem approval failure → non-fatal warning |
| `tools/test_merge.py` | Fixed `_settled_holding` truthiness bug |
| `src/config.py` | Added `clob_signature_type` field (default 3) |
| `config/default.yaml` | Added `clob.signature_type: 3` |
| `src/maker_loop.py` | **Quoting budget bug**: cap against actual fill price, not theoretical target |
| `src/main.py` | Colored terminal output, HTTP noise suppressed |

---

## 6. What still needs attention

1. **Combined budget enforcement verified** — need another live session to confirm the quoting fix prevents combined > $0.97
2. **Failed merge `0x4b2fe8dc2e`** — investigate tx on Polygonscan to understand the relayer race
3. **CtfAutoRedeem approval** — will resolve when Polymarket updates the relayer allowlist
4. **Auto-redeem should be OFF** — the bot should manage its own redemptions via `window_manager.redeem_sweep()`, not rely on Polymarket's settings
5. **Capital efficiency** — with only $18 starting capital, the bot was balance-constrained for much of the session (frequent "not enough balance" order rejections)
