import * as bip39 from 'bip39';
import { HDKey } from '@scure/bip32';
import { ethers } from 'ethers';
import TronWeb from 'tronweb';
import * as bitcoin from 'bitcoinjs-lib';
import * as ecc from 'tiny-secp256k1';
import solanaWeb3 from '@solana/web3.js';
import { derivePath } from 'ed25519-hd-key';

const { Keypair } = solanaWeb3;
bitcoin.initEccLib(ecc);

const MNEMONIC = process.env.MASTER_MNEMONIC || '';
export const WALLETS_ENABLED = bip39.validateMnemonic(MNEMONIC);
const SEED = WALLETS_ENABLED ? bip39.mnemonicToSeedSync(MNEMONIC) : null;

export function deriveAddresses(index) {
  const evmWallet = ethers.HDNodeWallet.fromSeed(SEED).derivePath(`m/44'/60'/0'/0/${index}`);
  
  const tronNode = HDKey.fromMasterSeed(SEED).derive(`m/44'/195'/0'/0/${index}`);
  const tronPk = Buffer.from(tronNode.privateKey).toString('hex');
  
  const btcNode = HDKey.fromMasterSeed(SEED).derive(`m/84'/0'/0'/0/${index}`);
  const { address: btcAddr } = bitcoin.payments.p2wpkh({ pubkey: Buffer.from(btcNode.publicKey) });

  const { key } = derivePath(`m/44'/501'/${index}'/0'`, SEED.toString('hex'));
  const solAddr = Keypair.fromSeed(key).publicKey.toBase58();

  return {
    evm: evmWallet.address,
    tron: TronWeb.address.fromPrivateKey(tronPk),
    btc: btcAddr,
    sol: solAddr
  };
}
