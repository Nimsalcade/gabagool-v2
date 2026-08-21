"""Resolve the MetaMask Predictions/Polymarket deposit-wallet address for an EVM owner.

This mirrors MetaMask Mobile's current depositWallet.ts CREATE2 derivation and checks
Polygon for the active factory beacon and deployed bytecode. It is read-only: no
signatures, private keys, transactions, or funds are required.
"""
from __future__ import annotations

import argparse
import json
import urllib.request

from eth_utils import keccak, to_checksum_address

POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"
FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
IMPLEMENTATION = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB"
BEACON_SELECTOR = "0x49493a4d"
ZERO = "0x0000000000000000000000000000000000000000"

ERC1967_CONST1 = bytes.fromhex(
    "cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3"
)
ERC1967_CONST2 = bytes.fromhex(
    "5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076"
)
ERC1967_PREFIX = 0x61003D3D8160233D3973

ERC1967_BEACON_CONST1 = bytes.fromhex(
    "60195155f3363d3d373d3d363d602036600436635c60da"
)
ERC1967_BEACON_CONST2 = bytes.fromhex(
    "1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c"
)
ERC1967_BEACON_CONST3 = bytes.fromhex(
    "b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3"
)
ERC1967_BEACON_PREFIX = 0x6100523D8160233D3973


def b20(address: str) -> bytes:
    raw = address.removeprefix("0x")
    if len(raw) != 40:
        raise ValueError(f"invalid EVM address: {address}")
    return bytes.fromhex(raw)


def pad32(data: bytes) -> bytes:
    if len(data) > 32:
        raise ValueError("value exceeds 32 bytes")
    return b"\x00" * (32 - len(data)) + data


def fixed_hex_int(value: int, size_bytes: int) -> bytes:
    return value.to_bytes(size_bytes, "big")


def create2(factory: bytes, salt: bytes, init_code_hash: bytes) -> str:
    digest = keccak(b"\xff" + factory + salt + init_code_hash)
    return to_checksum_address("0x" + digest[-20:].hex())


def create2_inputs(owner: str):
    factory = b20(FACTORY)
    wallet_id = pad32(b20(owner))
    # abi.encode(address factory, bytes32 walletId)
    args = pad32(factory) + wallet_id
    salt = keccak(args)
    return factory, args, salt


def derive_uups(owner: str) -> str:
    factory, args, salt = create2_inputs(owner)
    prefix = fixed_hex_int(ERC1967_PREFIX + (len(args) << 56), 10)
    init_hash = keccak(
        prefix
        + b20(IMPLEMENTATION)
        + bytes.fromhex("6009")
        + ERC1967_CONST2
        + ERC1967_CONST1
        + args
    )
    return create2(factory, salt, init_hash)


def derive_beacon(owner: str, beacon: str) -> str:
    factory, args, salt = create2_inputs(owner)
    prefix = fixed_hex_int(ERC1967_BEACON_PREFIX + (len(args) << 56), 10)
    init_hash = keccak(
        prefix
        + b20(beacon)
        + ERC1967_BEACON_CONST1
        + ERC1967_BEACON_CONST2
        + ERC1967_BEACON_CONST3
        + args
    )
    return create2(factory, salt, init_hash)


def rpc(method: str, params: list, rpc_url: str):
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    req = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read().decode())
    if "error" in body:
        raise RuntimeError(body["error"])
    return body.get("result")


def read_beacon(rpc_url: str) -> str | None:
    result = rpc(
        "eth_call",
        [{"to": FACTORY, "data": BEACON_SELECTOR}, "latest"],
        rpc_url,
    )
    if not result or result == "0x":
        return None
    raw = result.removeprefix("0x")
    if len(raw) < 64:
        raise RuntimeError(f"malformed beacon response: {result}")
    address = "0x" + raw[-40:]
    if address.lower() == ZERO.lower():
        return None
    return to_checksum_address(address)


def code_exists(address: str, rpc_url: str) -> bool:
    code = rpc("eth_getCode", [address, "latest"], rpc_url)
    return bool(code and code != "0x" and int(code, 16) != 0)


def main():
    parser = argparse.ArgumentParser(
        description="Resolve MetaMask Predictions deposit wallet on Polygon"
    )
    parser.add_argument("--owner", required=True, help="public MetaMask EVM address")
    parser.add_argument("--rpc", default=POLYGON_RPC)
    args = parser.parse_args()

    owner = to_checksum_address(args.owner)
    uups = derive_uups(owner)
    beacon = read_beacon(args.rpc)
    uups_deployed = code_exists(uups, args.rpc)

    if beacon:
        beacon_wallet = derive_beacon(owner, beacon)
        beacon_deployed = code_exists(beacon_wallet, args.rpc)
    else:
        beacon_wallet = None
        beacon_deployed = False

    # MetaMask resolution rule: deployed legacy/UUPS wins; otherwise current
    # beacon-derived address is the active candidate when a beacon is configured.
    resolved = uups if uups_deployed or not beacon_wallet else beacon_wallet
    resolved_deployed = uups_deployed if resolved == uups else beacon_deployed

    print(json.dumps({
        "owner_eoa": owner,
        "factory": to_checksum_address(FACTORY),
        "factory_beacon": beacon,
        "legacy_uups_wallet": uups,
        "legacy_uups_deployed": uups_deployed,
        "beacon_wallet": beacon_wallet,
        "beacon_wallet_deployed": beacon_deployed,
        "resolved_predictions_wallet": resolved,
        "resolved_predictions_wallet_deployed": resolved_deployed,
        "note": "Read-only derivation/check. No private key or signature used.",
    }, indent=2))


if __name__ == "__main__":
    main()
