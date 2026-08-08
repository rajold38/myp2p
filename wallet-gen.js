// ════════════════════════════════════════════════════════════════════
// wallet-gen.js — HD wallet derivation (ESM, myp2p server ke liye)
//
// Ek master BIP39 mnemonic se har user ka apna permanent address:
//   EVM  m/44'/60'/0'/0/i    -> ETH, BNB, POL, AVAX + saare ERC20/BEP20
//   TRON m/44'/195'/0'/0/i   -> TRX, USDT-TRC20
//   BTC  m/84'/0'/0'/0/i     -> bc1... native segwit
//   SOL  m/44'/501'/i'/0'    -> SOL
//
// npm i bip39 @scure/bip32 ethers tronweb bitcoinjs-lib tiny-secp256k1 \
//       @solana/web3.js ed25519-hd-key
// ════════════════════════════════════════════════════════════════════

import * as bip39 from 'bip39';
import { HDKey } from '@scure/bip32';
import { ethers } from 'ethers';
import TronWeb from 'tronweb';
import * as bitcoin from 'bitcoinjs-lib';
import * as ecc from 'tiny-secp256k1';
import { derivePath } from 'ed25519-hd-key';
import solanaWeb3 from '@solana/web3.js';

const { Keypair } = solanaWeb3;
bitcoin.initEccLib(ecc);

// Litecoin mainnet network params (bitcoinjs-lib only ships Bitcoin by
// default — these are Litecoin's actual constants, not a workaround).
// Produces native segwit addresses starting with "ltc1...".
const LITECOIN_NETWORK = {
  messagePrefix: '\x19Litecoin Signed Message:\n',
  bech32: 'ltc',
  bip32: { public: 0x019da462, private: 0x019d9cfe },
  pubKeyHash: 0x30,
  scriptHash: 0x32,
  wif: 0xb0,
};

const MNEMONIC = process.env.MASTER_MNEMONIC || '';
export const WALLETS_ENABLED = bip39.validateMnemonic(MNEMONIC);

if (!WALLETS_ENABLED) {
  console.warn('⚠️  MASTER_MNEMONIC missing/invalid — per-user deposit addresses disabled.');
}

const SEED = WALLETS_ENABLED
  ? bip39.mnemonicToSeedSync(MNEMONIC, process.env.MASTER_PASSPHRASE || '')
  : null;

function evm(i) {
  if (!SEED) throw new Error('MASTER_MNEMONIC not configured');
  const n = ethers.HDNodeWallet.fromSeed(SEED).derivePath(`m/44'/60'/0'/0/${i}`);
  return { address: ethers.getAddress(n.address), privateKey: n.privateKey };
}
function tron(i) {
  if (!SEED) throw new Error('MASTER_MNEMONIC not configured');
  const n = HDKey.fromMasterSeed(SEED).derive(`m/44'/195'/0'/0/${i}`);
  if (!n.privateKey) throw new Error('TRON private key derivation failed');
  const pk = Buffer.from(n.privateKey).toString('hex');
  return { address: TronWeb.address.fromPrivateKey(pk), privateKey: pk };
}
function btc(i) {
  if (!SEED) throw new Error('MASTER_MNEMONIC not configured');
  const n = HDKey.fromMasterSeed(SEED).derive(`m/84'/0'/0'/0/${i}`);
  const { address } = bitcoin.payments.p2wpkh({
    pubkey: Buffer.from(n.publicKey),
    network: bitcoin.networks.bitcoin,
  });
  if (!address || !n.privateKey) throw new Error('BTC key derivation failed');
  return { address, privateKey: Buffer.from(n.privateKey).toString('hex') };
}
function ltc(i) {
  if (!SEED) throw new Error('MASTER_MNEMONIC not configured');
  const n = HDKey.fromMasterSeed(SEED).derive(`m/84'/2'/0'/0/${i}`);
  const { address } = bitcoin.payments.p2wpkh({
    pubkey: Buffer.from(n.publicKey),
    network: LITECOIN_NETWORK,
  });
  if (!address || !n.privateKey) throw new Error('LTC key derivation failed');
  return { address, privateKey: Buffer.from(n.privateKey).toString('hex') };
}
function sol(i) {
  if (!SEED) throw new Error('MASTER_MNEMONIC not configured');
  const { key } = derivePath(`m/44'/501'/${i}'/0'`, SEED.toString('hex'));
  const kp = Keypair.fromSeed(key);
  return { address: kp.publicKey.toBase58(), privateKey: Buffer.from(kp.secretKey).toString('hex') };
}

/** Public addresses — yehi frontend ko bhejna hai. */
export function deriveAddresses(index) {
  if (!WALLETS_ENABLED) throw new Error('MASTER_MNEMONIC not configured');
  if (!Number.isInteger(index) || index < 0) throw new Error('bad derivation index');
  return { evm: evm(index).address, tron: tron(index).address, btc: btc(index).address, ltc: ltc(index).address, sol: sol(index).address };
}

/** Private keys — SIRF server-side sweep ke liye. Kabhi API/log/frontend mein nahi. */
export function derivePrivateKeys(index) {
  if (!WALLETS_ENABLED) throw new Error('MASTER_MNEMONIC not configured');
  return { evm: evm(index).privateKey, tron: tron(index).privateKey, btc: btc(index).privateKey, ltc: ltc(index).privateKey, sol: sol(index).privateKey };
}

/** coin + chain -> konsa address dikhana hai (frontend bhi yehi logic use karega) */
export function bucketFor(coinSymbol, chainName = '') {
  const c = String(coinSymbol || '').toUpperCase();
  const ch = String(chainName || '').toUpperCase();
  if (c === 'BTC' && !ch.includes('BEP')) return 'btc';
  if (c === 'LTC' && !ch.includes('BEP')) return 'ltc';
  if (c === 'SOL' || ch.includes('SOLANA')) return 'sol';
  if (c === 'TRX' || ch.includes('TRC')) return 'tron';
  return 'evm';
}
