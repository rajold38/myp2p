# BIEXC per-user deposit setup (v6 — free RPC, no Etherscan paywall)

Permanent per-user EVM, TRON, Bitcoin and Solana deposit addresses. Confirmed
deposits are credited atomically to `users/{uid}/balances/{COIN}` and
recorded in history.

## What changed in v6 and why

Etherscan removed free-tier API access to **BNB Chain, Avalanche, Base and
OP** in November 2025 (paid plans start at $49/mo), and BscScan's own
standalone free API was fully deprecated in December 2025. The old
`deposit-engine.js` used Etherscan for all EVM chains — which meant **BNB
and every BEP-20 token (USDT-BEP20, USDC-BEP20, etc.) silently stopped being
detected**, even with a perfectly valid API key, because BSC itself is no
longer covered by the free plan. There was no error, no crash — it just
never found anything.

v6 removes Etherscan entirely. It talks directly to each chain's own free
public RPC node (no key required), and tries a second/third fallback URL
automatically if the first one is unreachable — with a `[WARN]` log naming
exactly which one failed and which one it switched to:

| Chain | RPCs tried in order |
|---|---|
| ETH | `ethereum-rpc.publicnode.com` → `rpc.ankr.com/eth` → `cloudflare-eth.com` |
| BSC | `bsc-rpc.publicnode.com` → `bsc-dataseed.binance.org` → `rpc.ankr.com/bsc` |
| Polygon | `polygon-bor-rpc.publicnode.com` → `polygon-rpc.com` → `rpc.ankr.com/polygon` |
| Avalanche | `avalanche-c-chain-rpc.publicnode.com` → `api.avax.network/ext/bc/C/rpc` |

(`eth.llamarpc.com` was tried first in an earlier version of this file but
puts server-side/bot traffic behind a Cloudflare "Just a moment..." check
and returns HTTP 403 for non-browser requests — publicnode.com is built for
programmatic access and doesn't have that problem.)

Native coins are found by scanning new blocks once per poll cycle (one scan
covers every user at once). Tokens (USDT, USDC, etc.) are found via a single
`eth_getLogs` call per token per cycle, filtered to your users' addresses —
also one call covers every user, not one call per user.

Every failure now logs a `[WARN]` line instead of silently doing nothing.

## Deploy

1. Replace your `myp2p` repository files with this folder and deploy it on Render.
2. Generate a new 24-word BIP39 mnemonic **offline**. Never put it in GitHub, chat, screenshots or frontend code.
3. Add the variables from `.env.example` in Render's Environment tab. Only `MASTER_MNEMONIC`, `FIREBASE_SERVICE_ACCOUNT`, `FIREBASE_DB_URL`, `TG_TOKEN`, `TG_CHAT` are required — everything else has a working free default.
4. Also set `DEBUG_ADMIN_KEY` to any random string — this unlocks the debugging tools below, which you'll want on day one.
5. Keep exactly one backend instance running while using the free polling design.
6. **Save the env vars, then trigger "Manual Deploy"** on Render — adding an env var alone does not restart a running service.
7. Watch the logs for: `[WALLET] deposit engine on (RPC-only, no Etherscan dependency); polling every 60s`

## Debugging — do this before testing with real money

Once `DEBUG_ADMIN_KEY` is set and deployed:

```bash
# See engine health, which RPCs are connected, and the last poll's result
curl "https://YOUR-BACKEND.onrender.com/api/debug/status?key=YOUR_DEBUG_ADMIN_KEY"

# Force an immediate check right now instead of waiting up to 60s
curl -X POST "https://YOUR-BACKEND.onrender.com/api/debug/poll-now?key=YOUR_DEBUG_ADMIN_KEY"
```

`poll-now` replies with a per-chain summary like:
`["ETH ok (0 new)", "BSC ok (1 new)", "POLYGON ok (0 new)", "AVALANCHE ok (0 new)", "TRON checked 3 (0 errors)", ...]`

If a chain shows `ERROR` here or in the Render logs, the message tells you
exactly what failed (RPC unreachable, bad response, etc.) — nothing fails
silently anymore.

## Frontend request

The signed-in frontend must get a Firebase ID token and call:

```js
const token = await firebase.auth().currentUser.getIdToken();
const uid = firebase.auth().currentUser.uid;
const response = await fetch(`${BACKEND_URL}/api/wallet/${encodeURIComponent(uid)}`, {
  headers: { Authorization: `Bearer ${token}` },
});
const addresses = await response.json();
```

Use `addresses.evm` for EVM/native ERC-20/BEP-20 deposits (this one address
works for ETH, BNB, Polygon and Avalanche — all EVM chains share the same
address format), `addresses.tron` for TRX/TRC-20, `addresses.btc` for native
BTC and `addresses.sol` for native SOL.

## Important production notes

- Test with tiny amounts first. Blockchain transfers cannot be reversed.
- Back up the mnemonic offline in two secure physical locations. Losing it loses access to deposited funds.
- This creates custody wallets; withdrawals/sweeping and gas management are separate operational systems.
- The free public RPC endpoints above are official, chain-run nodes suitable for a small-to-medium launch. If you ever outgrow them, swap in a paid RPC provider (Alchemy, QuickNode, Ankr, etc.) by setting `ETH_RPC_URL` / `BSC_RPC_URL` / `POLYGON_RPC_URL` / `AVALANCHE_RPC_URL` — no code changes needed.
- Do not expose `user_wallets`, `wallet_index`, `deposits_seen`, `meta` or the `/api/debug/*` endpoints through client database rules or a public key.
