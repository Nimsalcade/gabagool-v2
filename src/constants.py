"""
Polymarket V2 platform constants.

Single source of truth. Every address below was verified against
https://docs.polymarket.com/resources/contracts (June 2026) and against the
`polymarket-client` SDK's bundled production Environment. Do NOT hardcode
addresses anywhere else in this codebase — import from here.

WHY THIS FILE EXISTS
--------------------
The previous bot died on stale platform facts:
  * It merged against the NegRiskAdapter for non-neg-risk markets.
  * It used USDC.e as collateral after the April 2026 pUSD migration.
  * It used V1 exchange domains after the April 22, 2026 V2 cutover.
Pinning the current facts in one reviewed file prevents that class of bug.
"""

from __future__ import annotations

# --- Chain -------------------------------------------------------------------
CHAIN_ID = 137  # Polygon mainnet

# --- Core trading contracts (V2, post April 22 2026 migration) ---------------
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"            # Conditional Tokens (unchanged)
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# --- Collateral (pUSD replaced USDC.e as the trading collateral) --------------
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"            # CollateralToken proxy
COLLATERAL_ONRAMP = "0x93070a847efEf7F70739046A929D47a521F5B8ee"   # wrap USDC.e -> pUSD
COLLATERAL_OFFRAMP = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"  # unwrap pUSD -> USDC.e
CTF_COLLATERAL_ADAPTER = "0xAdA100Db00Ca00073811820692005400218FcE1f"        # pUSD-native split/merge/redeem
NEG_RISK_CTF_COLLATERAL_ADAPTER = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"

USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"          # legacy; kept for offramp tooling only

# --- API hosts ----------------------------------------------------------------
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
DATA_HOST = "https://data-api.polymarket.com"
RELAYER_HOST = "https://relayer-v2.polymarket.com"

# --- Unit scaling --------------------------------------------------------------
USDC_DECIMALS = 6
MICRO = 10 ** USDC_DECIMALS  # 1 share / 1 pUSD == 1_000_000 base units

# --- Exchange-enforced order constraints ---------------------------------------
# The book endpoint returns the authoritative per-market values
# (OrderBook.min_order_size / OrderBook.tick_size). These are conservative
# fallbacks used only when the book hasn't been fetched yet.
FALLBACK_MIN_ORDER_SHARES = 5.0     # "Size (4) lower than the minimum: 5" — seen live
FALLBACK_TICK = 0.01
MIN_ORDER_NOTIONAL_USD = 1.00       # historical $1 floor; cheap insurance to keep

# --- 15-minute Up/Down market slugs --------------------------------------------
# Verified against real data: "eth-updown-15m-1761728400" where 1761728400 is
# the unix epoch of the window START, always a multiple of WINDOW_SECONDS.
UPDOWN_SLUG_TEMPLATE = "{asset}-updown-15m-{window_start}"
WINDOW_SECONDS = 900

# --- Heartbeat (V2 dead-man's switch) -------------------------------------------
# CLOB cancels all open orders if a started heartbeat session goes silent
# for >10s (5s grace). We send every HEARTBEAT_INTERVAL_S so that a crashed
# bot leaves no orphan orders on the book.
HEARTBEAT_INTERVAL_S = 5.0
