"""Run the tightly-capped MetaMask Predictions BTC 5m strategy across 10 markets.

Unlike the one-shot runner, this orchestrator does not abort the whole experiment when
one market finishes with an unmatched first leg. It records the residual exposure and
continues to the next fresh 5-minute session so the user can observe up to ten market
outcomes in one run.

Important: cash deltas do not include the eventual settlement value of any unresolved
unmatched positions until those positions are redeemed/settled. The runner prints those
residual UP/DOWN balances explicitly in the session summary.

No private key is printed, written to disk, or accepted as a command-line argument.
"""
from __future__ import annotations

import argparse
import asyncio
import time
from collections import Counter
from decimal import Decimal

from eth_account import Account
from eth_utils import to_checksum_address
from polymarket import AsyncSecureClient

from tools import metamask_tiny_live_test as live
from tools import metamask_tiny_order as tiny

DEFAULT_OWNER = tiny.DEFAULT_OWNER
MARKET_COUNT = 10
FIRST_MAX = Decimal("0.25")
PAIR_MAX = Decimal("0.90")
MAX_TOTAL_SPEND = Decimal("4.75")
TEST_SHARES = Decimal("5")
MIN_CASH_TO_START_WINDOW = Decimal("1.40")


async def amain(owner: str) -> int:
    owner = to_checksum_address(owner)
    await tiny._geo_preflight()  # pyright: ignore[reportPrivateUsage]

    wallet, deployed = tiny._resolve_wallet(owner)  # pyright: ignore[reportPrivateUsage]
    print(f"OWNER     {owner}")
    print(f"PREDICT   {wallet} deployed={deployed}")
    if not deployed:
        raise RuntimeError(
            "resolved MetaMask Predictions deposit wallet is not deployed; refusing live run"
        )

    private_key = live._private_key()  # pyright: ignore[reportPrivateUsage]
    signer = to_checksum_address(Account.from_key(private_key).address)
    if signer.lower() != owner.lower():
        raise RuntimeError(
            f"private key belongs to {signer}, expected MetaMask owner {owner}; refusing live run"
        )

    client = await AsyncSecureClient.create(private_key=private_key, wallet=wallet)
    try:
        print(f"WALLET    {client.wallet} ({client.wallet_type})")
        if str(client.wallet).lower() != wallet.lower():
            raise RuntimeError(f"SDK bound {client.wallet}, expected {wallet}")
        if "DEPOSIT" not in str(client.wallet_type).upper():
            raise RuntimeError(f"expected Deposit Wallet, got {client.wallet_type}")

        collateral = str(client._ctx.environment_config.collateral_token)  # pyright: ignore[reportPrivateUsage]
        ctf = str(client._ctx.environment_config.conditional_tokens)  # pyright: ignore[reportPrivateUsage]

        initial_cash = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
        print(f"START     pUSD=${initial_cash:.6f}")
        print(
            f"LIMITS    {MARKET_COUNT} markets | trigger<={FIRST_MAX} | "
            f"pair<={PAIR_MAX} | spend<={MAX_TOTAL_SPEND}/window | target={TEST_SHARES}sh"
        )
        print(
            "MODE      continue after no-trade, flatten, merge, or unmatched LEG1; "
            "unresolved exposure is reported, not hidden"
        )

        await live._ensure_standard_trade_approvals(  # pyright: ignore[reportPrivateUsage]
            client,
            max_total_spend=MAX_TOTAL_SPEND,
        )

        start = live.window_start_epoch(300, time.time()) + 300
        rows: list[dict[str, object]] = []

        for index in range(MARKET_COUNT):
            session_no = index + 1
            target = start + index * 300
            wait = target - time.time()
            if wait > 0:
                print(f"WAIT      market {session_no}/{MARKET_COUNT} starts in {wait:.1f}s")
                await asyncio.sleep(wait)

            market = await live._resolve_fresh_market(client, target)  # pyright: ignore[reportPrivateUsage]
            if market is None:
                print(f"SESSION   {session_no}/{MARKET_COUNT} market unavailable start={target}")
                rows.append(
                    {
                        "n": session_no,
                        "market": f"start={target}",
                        "status": "market-unavailable",
                        "cash_before": None,
                        "cash_after": None,
                        "cash_delta": None,
                        "up": Decimal(0),
                        "down": Decimal(0),
                    }
                )
                continue

            print("\n" + "=" * 72)
            print(f"SESSION   {session_no}/{MARKET_COUNT}")
            print(f"MARKET    {market.slug}")
            print(f"CONDITION {market.condition_id}")
            print(f"UP        {market.up_token_id}")
            print(f"DOWN      {market.down_token_id}")

            cash_before = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
            if cash_before < MIN_CASH_TO_START_WINDOW:
                print(
                    f"STOP RUN  pUSD=${cash_before:.6f} below ${MIN_CASH_TO_START_WINDOW} "
                    "minimum reserved for another tiny first leg"
                )
                rows.append(
                    {
                        "n": session_no,
                        "market": market.slug,
                        "status": "low-cash-stop",
                        "cash_before": cash_before,
                        "cash_after": cash_before,
                        "cash_delta": Decimal(0),
                        "up": Decimal(0),
                        "down": Decimal(0),
                    }
                )
                break

            try:
                status = await tiny._robust_trade_window(  # pyright: ignore[reportPrivateUsage]
                    client,
                    market,
                    first_max=FIRST_MAX,
                    pair_max=PAIR_MAX,
                    emergency_pair_max=PAIR_MAX,
                    max_total_spend=MAX_TOTAL_SPEND,
                    test_shares=TEST_SHARES,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"SESSION ERROR {type(exc).__name__}: {exc}")
                status = "error"

            cash_after = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
            up_bal, down_bal = await live._positions(  # pyright: ignore[reportPrivateUsage]
                ctf,
                wallet,
                market.up_token_id,
                market.down_token_id,
            )
            delta = cash_after - cash_before

            print(
                f"SESSION   {session_no}/{MARKET_COUNT} status={status} "
                f"cash=${cash_before:.6f}->${cash_after:.6f} delta=${delta:+.6f} "
                f"residual UP={up_bal:.6f} DOWN={down_bal:.6f}"
            )

            if status == "exposure-unknown" or max(up_bal, down_bal) > Decimal("0.000001"):
                print(
                    "CONTINUE  residual exposure retained for settlement; moving to the next "
                    "5-minute market instead of ending the experiment"
                )

            rows.append(
                {
                    "n": session_no,
                    "market": market.slug,
                    "status": status,
                    "cash_before": cash_before,
                    "cash_after": cash_after,
                    "cash_delta": delta,
                    "up": up_bal,
                    "down": down_bal,
                }
            )

        final_cash = await live._pusd_balance(collateral, wallet)  # pyright: ignore[reportPrivateUsage]
        counts = Counter(str(row["status"]) for row in rows)

        print("\n" + "=" * 72)
        print(f"{MARKET_COUNT}-MARKET PERFORMANCE SUMMARY")
        print(f"START CASH ${initial_cash:.6f}")
        print(f"END CASH   ${final_cash:.6f}")
        print(f"CASH DELTA ${final_cash - initial_cash:+.6f}")
        print(
            "STATUS     "
            + ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
        )
        for row in rows:
            before = row["cash_before"]
            after = row["cash_after"]
            delta = row["cash_delta"]
            cash_text = (
                "n/a"
                if before is None or after is None or delta is None
                else f"${before:.4f}->${after:.4f} ({delta:+.4f})"
            )
            print(
                f"#{int(row['n']):02d} {str(row['status']):18s} "
                f"{cash_text:27s} UP={Decimal(row['up']):.6f} "
                f"DOWN={Decimal(row['down']):.6f} {row['market']}"
            )

        unresolved = [
            row
            for row in rows
            if max(Decimal(row["up"]), Decimal(row["down"])) > Decimal("0.000001")
        ]
        if unresolved:
            print(
                "NOTE       cash delta is not final P&L while unresolved residual positions "
                "remain. Their eventual settlement/redemption value must be added before "
                f"judging the {MARKET_COUNT}-market strategy result."
            )
        else:
            print("NOTE       no residual market positions remain in the recorded sessions.")

        return 0
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the tiny MetaMask BTC 5m strategy across up to 10 markets"
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="MetaMask owner EOA")
    args = parser.parse_args()
    try:
        rc = asyncio.run(amain(args.owner))
    except KeyboardInterrupt:
        rc = 130
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL     {type(exc).__name__}: {exc}")
        rc = 1
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
