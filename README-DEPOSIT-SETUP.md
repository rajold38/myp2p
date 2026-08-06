# BIEXC per-user deposit setup

This package is ready for permanent per-user EVM, TRON, Bitcoin and Solana deposit addresses. Confirmed deposits are credited atomically to `users/{uid}/balances/{COIN}` and recorded in history.

## Deploy

1. Replace your `myp2p` repository files with this folder and deploy it on Render.
2. Generate a new 24-word BIP39 mnemonic **offline**. Never put it in GitHub, chat, screenshots or frontend code.
3. Add the variables from `.env.example` in Render Environment. `MASTER_MNEMONIC` and `ETHERSCAN_API_KEY` are required for all supported networks; `TRONGRID_API_KEY` is strongly recommended.
4. Keep exactly one backend instance running while using the free polling design.
5. Wait for the log: `deposit engine on`.

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

Use `addresses.evm` for EVM/native ERC-20/BEP-20 deposits, `addresses.tron` for TRX/TRC-20, `addresses.btc` for native BTC and `addresses.sol` for native SOL.

## Important production notes

- Test with tiny amounts first. Blockchain transfers cannot be reversed.
- Back up the mnemonic offline in two secure physical locations. Losing it loses access to deposited funds.
- This creates custody wallets; withdrawals/sweeping and gas management are separate operational systems.
- Public free explorer/RPC limits are suitable only for a small launch. Upgrade to provider webhooks/indexing as user count grows.
- Do not expose `user_wallets`, `wallet_index`, `deposits_seen` or `meta` through client database rules.