import requests
import pandas as pd
import numpy as np
import json
import time
import os
from collections import defaultdict, Counter
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

# --- Configuration ---
HELIUS_API_KEY = "YOUR_HELIUS_KEY_HERE"
CRYPTOCOMPARE_API_KEY = "YOUR_CRYPTOCOMPARE_KEY_HERE"
MORALIS_API_KEY = "YOUR_MORALIS_KEY_HERE"

# --- Constants ---
LAMPORTS_TO_SOL = 10**9
API_DELAY = 0.5  # More conservative for Solana APIs
MAX_RETRIES = 3
HELIUS_MAX_RECORDS = 1000  # Helius pagination limit
MAX_TRANSACTIONS_PER_ADDRESS = 50000
JUPITER_API_DELAY = 1.0  # Jupiter has stricter rate limits

# Known Solana program addresses to filter out from counterparty analysis
KNOWN_PROGRAM_ADDRESSES = {
    '11111111111111111111111111111112',  # System Program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token Program
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',  # Associated Token Program
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',  # Jupiter V6
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium AMM
}

class SolanaPriceConverter:
    """
    Multi-layered price conversion system for Solana tokens with robust fallbacks.
    Priority: Jupiter API -> CoinGecko -> CryptoCompare -> Moralis -> Fail
    """
    
    def __init__(self):
        self.price_cache = {}
        self.token_price_cache = {}
        self.token_info_cache = {}
        self.load_price_cache()

    def load_price_cache(self):
        if os.path.exists('solana_price_cache.json'):
            try:
                with open('solana_price_cache.json', 'r') as f:
                    cache_data = json.load(f)
                    self.price_cache = cache_data.get('sol_btc', {})
                    self.token_price_cache = cache_data.get('token_sol', {})
                    self.token_info_cache = cache_data.get('token_info', {})
                    print(f"Loaded price cache with {len(self.price_cache)} SOL/BTC entries and {len(self.token_price_cache)} token entries.")
            except Exception as e:
                print(f"⚠️ Error loading price cache: {e}. Starting with an empty cache.")
                self.price_cache, self.token_price_cache, self.token_info_cache = {}, {}, {}

    def save_price_cache(self):
        cache_data = {'sol_btc': self.price_cache, 'token_sol': self.token_price_cache, 'token_info': self.token_info_cache}
        try:
            with open('solana_price_cache.json', 'w') as f:
                json.dump(cache_data, f, indent=2)
            print(f"💾 Price cache saved with {len(self.price_cache)} SOL/BTC and {len(self.token_price_cache)} token entries.")
        except Exception as e:
            print(f"❌ Error saving price cache: {e}")

    def get_token_info(self, mint_address: str) -> Dict[str, Any]:
        """Get token metadata with fallbacks to multiple APIs."""
        mint_address = mint_address.strip()
        
        if mint_address in self.token_info_cache:
            return self.token_info_cache[mint_address]

        # Common Solana token addresses
        token_map = {
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': {'symbol': 'USDC', 'decimals': 6, 'name': 'USD Coin'},
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': {'symbol': 'USDT', 'decimals': 6, 'name': 'Tether'},
            'So11111111111111111111111111111111111111112': {'symbol': 'WSOL', 'decimals': 9, 'name': 'Wrapped SOL'},
            '9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E': {'symbol': 'BTC', 'decimals': 6, 'name': 'Bitcoin (Sollet)'},
            'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So': {'symbol': 'mSOL', 'decimals': 9, 'name': 'Marinade staked SOL'},
        }
        
        if mint_address in token_map:
            self.token_info_cache[mint_address] = token_map[mint_address]
            return token_map[mint_address]

        # Try Helius first (fastest for Solana)
        helius_info = self._fetch_token_info_helius(mint_address)
        if helius_info.get('symbol') != 'UNKNOWN':
            self.token_info_cache[mint_address] = helius_info
            return helius_info

        # Fallback to Moralis
        moralis_info = self._fetch_token_info_moralis(mint_address)
        if moralis_info.get('symbol') != 'UNKNOWN':
            self.token_info_cache[mint_address] = moralis_info
            return moralis_info

        # Final fallback
        unknown_info = {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown Token'}
        self.token_info_cache[mint_address] = unknown_info
        return unknown_info

    def _fetch_token_info_helius(self, mint_address: str) -> Dict[str, Any]:
        """Fetch token metadata from Helius API."""
        print(f"   -> Fetching token info from Helius for {mint_address[:20]}...")
        try:
            url = f"https://api.helius.xyz/v0/token-metadata"
            params = {'api-key': HELIUS_API_KEY}
            payload = {'mintAccounts': [mint_address]}

            response = requests.post(url, params=params, json=payload, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list) and len(data) > 0:
                    token_data = data[0]
                    return {
                        'symbol': token_data.get('symbol', 'UNKNOWN').upper(),
                        'decimals': int(token_data.get('decimals', 9)),
                        'name': token_data.get('name', 'Unknown')
                    }
            
            print(f"   -> ❌ Helius metadata failed. Status: {response.status_code}")
            return {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown'}
            
        except Exception as e:
            print(f"   -> ❌ Helius metadata EXCEPTION: {e}")
            return {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown'}

    def _fetch_token_info_moralis(self, mint_address: str) -> Dict[str, Any]:
        """Fallback to Moralis for token metadata."""
        print(f"   -> Fallback to Moralis for token info...")
        try:
            url = f"https://solana-gateway.moralis.io/token/mainnet/{mint_address}/metadata"
            headers = {"accept": "application/json", "X-API-Key": MORALIS_API_KEY}

            response = requests.get(url, headers=headers, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                data = response.json()
                return {
                    'symbol': data.get('symbol', 'UNKNOWN').upper(),
                    'decimals': int(data.get('decimals', 9)),
                    'name': data.get('name', 'Unknown')
                }
            
            return {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown'}
            
        except Exception as e:
            print(f"   -> ❌ Moralis metadata EXCEPTION: {e}")
            return {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown'}

    def get_token_sol_ratio(self, mint_address: str, timestamp: int) -> float:
        """Get token-to-SOL conversion ratio with multi-layer fallback."""
        token_info = self.get_token_info(mint_address)
        token_symbol = token_info['symbol']

        # WSOL is 1:1 with SOL
        if token_symbol == 'WSOL' or mint_address == 'So11111111111111111111111111111111111111112':
            return 1.0

        dt_object = datetime.fromtimestamp(timestamp)
        monthly_key = dt_object.strftime('%Y-%m-01')
        cache_key = f"{token_symbol}_{monthly_key}_{mint_address[:20]}"

        if cache_key in self.token_price_cache:
            return self.token_price_cache[cache_key]

        # Handle stablecoins
        if token_symbol in ['USDC', 'USDT', 'BUSD', 'DAI']:
            ratio = self._get_stablecoin_sol_ratio(timestamp)
            if ratio > 0:
                self.token_price_cache[cache_key] = ratio
            return ratio

        print(f"   -> PRICE FETCH: Getting price for {token_symbol} ({mint_address[:20]}...)")
        
        # Multi-layer price fetching
        price = self._fetch_token_price_jupiter(mint_address, timestamp)
        if price > 0:
            self.token_price_cache[cache_key] = price
            return price

        price = self._fetch_token_price_coingecko(token_symbol, timestamp)
        if price > 0:
            self.token_price_cache[cache_key] = price
            return price

        price = self._fetch_token_price_cryptocompare(token_symbol, timestamp)
        if price > 0:
            self.token_price_cache[cache_key] = price
            return price

        price = self._fetch_token_price_moralis(mint_address, timestamp)
        if price > 0:
            self.token_price_cache[cache_key] = price
            return price

        print(f"      -> ❌ ALL APIs FAILED for {token_symbol}. Returning 0.0")
        self.token_price_cache[cache_key] = 0.0
        return 0.0

    def _fetch_token_price_jupiter(self, mint_address: str, timestamp: int) -> float:
        """Layer 1: Jupiter API (current supported Price API V3)."""
        print(f"      -> [Layer 1] Jupiter API for current price...")
        try:
            # Jupiter only provides recent prices
            days_old = (time.time() - timestamp) / (24 * 3600)
            if days_old > 30:  # Only use Jupiter for recent prices
                print(f"      -> ❌ Jupiter: Transaction too old ({days_old:.1f} days)")
                return 0.0

            url = "https://lite-api.jup.ag/price/v3"
            params = {"ids": mint_address}

            response = requests.get(url, params=params, timeout=15)
            time.sleep(JUPITER_API_DELAY)

            if response.status_code != 200:
                print(f"      -> ❌ Jupiter FAILED: HTTP {response.status_code} - {response.text[:200]}")
                return 0.0

            data = response.json()
            token_info = data.get(mint_address)
            if not token_info:
                print(f"      -> ❌ Jupiter FAILED: No entry for mint {mint_address} in response.")
                return 0.0

            usd_price = token_info.get("usdPrice")
            if usd_price is None:
                print(f"      -> ❌ Jupiter FAILED: 'usdPrice' missing for {mint_address}. Full payload: {token_info}")
                return 0.0

            # Convert USD price of the token to SOL via stablecoin ratio (assuming that returns USD->SOL)
            sol_usd_ratio = self._get_stablecoin_sol_ratio(timestamp)
            if sol_usd_ratio <= 0:
                print(f"      -> ❌ Jupiter FAILED: invalid SOL/USD ratio ({sol_usd_ratio})")
                return 0.0

            sol_ratio = usd_price * sol_usd_ratio
            print(f"      -> ✅ Jupiter SUCCESS: {sol_ratio:.8f} SOL (token ${usd_price:.6f} * ratio {sol_usd_ratio:.6f})")
            return sol_ratio

        except Exception as e:
            print(f"      -> ❌ Jupiter EXCEPTION: {e}")
            return 0.0

    def _fetch_token_price_coingecko(self, token_symbol: str, timestamp: int) -> float:
        """Layer 2: CoinGecko API (good historical data, free tier)."""
        print(f"      -> [Layer 2] CoinGecko API...")
        try:
            # CoinGecko uses dates, not timestamps
            dt = datetime.fromtimestamp(timestamp)
            date_str = dt.strftime('%d-%m-%Y')

            # First, try to find the coin ID by symbol
            search_url = f"https://api.coingecko.com/api/v3/search"
            params = {'query': token_symbol}
            
            response = requests.get(search_url, params=params, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                search_data = response.json()
                coins = search_data.get('coins', [])
                
                # Look for exact symbol match
                coin_id = None
                for coin in coins:
                    if coin.get('symbol', '').upper() == token_symbol.upper():
                        coin_id = coin.get('id')
                        break

                if coin_id:
                    # Get historical price
                    history_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
                    history_params = {'date': date_str}
                    
                    hist_response = requests.get(history_url, params=history_params, timeout=15)
                    time.sleep(API_DELAY)

                    if hist_response.status_code == 200:
                        hist_data = hist_response.json()
                        market_data = hist_data.get('market_data', {})
                        current_price = market_data.get('current_price', {})
                        usd_price = current_price.get('usd')

                        if usd_price:
                            sol_usd_ratio = self._get_stablecoin_sol_ratio(timestamp)
                            if sol_usd_ratio > 0:
                                sol_ratio = usd_price * sol_usd_ratio
                                print(f"      -> ✅ CoinGecko SUCCESS: {sol_ratio:.8f} SOL")
                                return sol_ratio

            print(f"      -> ❌ CoinGecko FAILED: No data found")
            return 0.0

        except Exception as e:
            print(f"      -> ❌ CoinGecko EXCEPTION: {e}")
            return 0.0

    def _fetch_token_price_cryptocompare(self, token_symbol: str, timestamp: int) -> float:
        """Layer 3: CryptoCompare API (excellent historical data)."""
        print(f"      -> [Layer 3] CryptoCompare API...")
        try:
            monthly_dt = datetime.fromtimestamp(timestamp).replace(day=1)
            monthly_timestamp = int(monthly_dt.timestamp())
            params = {
                'fsym': token_symbol,
                'tsyms': 'SOL',
                'ts': monthly_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }

            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                data = response.json()
                token_price = data.get(token_symbol, {}).get('SOL')
                if token_price and token_price > 0:
                    print(f"      -> ✅ CryptoCompare SUCCESS: {token_price:.8f} SOL")
                    return token_price

            print(f"      -> ❌ CryptoCompare FAILED: {response.json().get('Message', 'No data')}")
            return 0.0

        except Exception as e:
            print(f"      -> ❌ CryptoCompare EXCEPTION: {e}")
            return 0.0

    def _fetch_token_price_moralis(self, mint_address: str, timestamp: int) -> float:
        """Layer 4: Moralis API (last resort)."""
        print(f"      -> [Layer 4] Moralis API...")
        try:
            url = f"https://solana-gateway.moralis.io/token/mainnet/{mint_address}/price"
            headers = {"accept": "application/json", "X-API-Key": MORALIS_API_KEY}

            response = requests.get(url, headers=headers, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                data = response.json()
                usd_price = data.get('usdPrice')
                if usd_price:
                    sol_usd_ratio = self._get_stablecoin_sol_ratio(timestamp)
                    if sol_usd_ratio > 0:
                        sol_ratio = usd_price * sol_usd_ratio
                        print(f"      -> ✅ Moralis SUCCESS: {sol_ratio:.8f} SOL")
                        return sol_ratio

            print(f"      -> ❌ Moralis FAILED")
            return 0.0

        except Exception as e:
            print(f"      -> ❌ Moralis EXCEPTION: {e}")
            return 0.0

    def _get_stablecoin_sol_ratio(self, timestamp: int) -> float:
        """Get USD-to-SOL conversion ratio."""
        dt_object = datetime.fromtimestamp(timestamp)
        monthly_key = dt_object.strftime('%Y-%m-01')
        cache_key = f"SOL_USD_{monthly_key}"
        
        if cache_key in self.price_cache:
            sol_usd_price = self.price_cache[cache_key]
            return 1.0 / sol_usd_price if sol_usd_price > 0 else 0.0

        print(f"   -> Fetching SOL/USD price for {monthly_key}...")
        try:
            monthly_timestamp = int(datetime.strptime(monthly_key, '%Y-%m-%d').timestamp())
            params = {
                'fsym': 'SOL',
                'tsyms': 'USD',
                'ts': monthly_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }
            
            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)
            
            if response.status_code == 200:
                data = response.json()
                sol_usd_price = data.get('SOL', {}).get('USD')
                if sol_usd_price and sol_usd_price > 0:
                    self.price_cache[cache_key] = sol_usd_price
                    return 1.0 / sol_usd_price

            print(f"   -> Warning: Could not fetch SOL/USD price")
            return 0.0

        except Exception as e:
            print(f"   -> Error: SOL/USD fetch failed: {e}")
            return 0.0

    def get_sol_btc_ratio(self, timestamp: int) -> float:
        """Get SOL-to-BTC conversion ratio."""
        dt_object = datetime.fromtimestamp(timestamp)
        monthly_key = dt_object.strftime('%Y-%m-01')
        
        if monthly_key in self.price_cache:
            return self.price_cache[monthly_key]

        print(f"-> Fetching SOL/BTC price for {monthly_key}...")
        try:
            monthly_timestamp = int(datetime.strptime(monthly_key, '%Y-%m-%d').timestamp())
            params = {
                'fsym': 'SOL',
                'tsyms': 'BTC',
                'ts': monthly_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }
            
            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)
            
            if response.status_code == 200:
                data = response.json()
                sol_price = data.get('SOL', {}).get('BTC')
                if sol_price:
                    self.price_cache[monthly_key] = sol_price
                    return sol_price

            print(f"   -> Warning: No SOL/BTC price data for {monthly_key}. Using fallback.")
            return self._get_fallback_sol_btc_ratio(timestamp)

        except Exception as e:
            print(f"   -> Error: SOL/BTC price fetch failed: {e}. Using fallback.")
            return self._get_fallback_sol_btc_ratio(timestamp)

    def _get_fallback_sol_btc_ratio(self, timestamp: int) -> float:
        """Fallback SOL/BTC ratios based on historical averages."""
        year = datetime.fromtimestamp(timestamp).year
        if year <= 2021:
            return 0.001  # SOL was very cheap early on
        elif year <= 2022:
            return 0.003  # Bull run peak
        elif year <= 2023:
            return 0.0008  # Bear market
        else:
            return 0.002  # Current era estimate


class SolanaDataExtractor:
    """Extracts Solana transaction data using Helius API with proper parsing."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.price_converter = SolanaPriceConverter()

    def get_all_transactions(self, address: str) -> List[Dict]:
        """Get all transactions for a Solana address with proper parsing."""
        print(f" Fetching Solana transactions for {address}...")
        
        all_transactions = []
        before_signature = None
        page_count = 0

        while len(all_transactions) < MAX_TRANSACTIONS_PER_ADDRESS:
            page_count += 1
            print(f"    Fetching page {page_count}...")
            
            page_transactions = self._fetch_transaction_page(address, before_signature)
            
            if not page_transactions:
                break

            # Parse each transaction
            for raw_tx in page_transactions:
                parsed_txs = self._parse_solana_transaction(raw_tx, address)
                all_transactions.extend(parsed_txs)

            # Set up for next page
            if len(page_transactions) < HELIUS_MAX_RECORDS:
                break
                
            before_signature = page_transactions[-1].get('signature')
            
            if len(all_transactions) >= MAX_TRANSACTIONS_PER_ADDRESS:
                print(f" ⚠️  Limiting to {MAX_TRANSACTIONS_PER_ADDRESS} transactions")
                all_transactions = all_transactions[:MAX_TRANSACTIONS_PER_ADDRESS]
                break

        print(f" Total parsed transactions: {len(all_transactions)}")
        return all_transactions

    def _fetch_transaction_page(self, address: str, before_signature: Optional[str] = None) -> List[Dict]:
        """Fetch a single page of transactions from Helius."""
        url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
        params = {'api-key': self.api_key}
        
        if before_signature:
            params['before'] = before_signature

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    time.sleep(API_DELAY)
                    return data if isinstance(data, list) else []
                elif response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"    Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"    API Error: {response.status_code}")
                    time.sleep(2 ** attempt)
                    
            except requests.exceptions.RequestException as e:
                print(f"    Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                time.sleep(2 ** attempt)

        return []

    def _parse_solana_transaction(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Parse a Solana transaction into standardized format(s)."""
        parsed_transactions = []
        
        # Basic transaction info
        signature = raw_tx.get('signature', '')
        slot = raw_tx.get('slot', 0)
        timestamp = raw_tx.get('timestamp', 0)
        fee_lamports = raw_tx.get('fee', 0)
        
        # Check if transaction succeeded
        meta = raw_tx.get('meta', {})
        tx_error = meta.get('err')
        succeeded = tx_error is None

        # Handle failed transactions (only fees, no transfers)
        if not succeeded:
            parsed_tx = {
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'FAILED',
                'from': target_address,
                'to': target_address,
                'value': 0,
                'fee_lamports': fee_lamports,
                'success': False,
                'mint_address': 'So11111111111111111111111111111111111111112',  # SOL
                'decimals': 9
            }
            parsed_transactions.append(parsed_tx)
            return parsed_transactions

        # Parse successful transactions
        # Look for native SOL transfers first
        sol_transfers = self._extract_sol_transfers(raw_tx, target_address)
        for transfer in sol_transfers:
            transfer.update({
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'fee_lamports': fee_lamports,
                'success': True,
                'tx_type': 'SOL_TRANSFER'
            })
            parsed_transactions.append(transfer)

        # Look for SPL token transfers
        token_transfers = self._extract_token_transfers(raw_tx, target_address)
        for transfer in token_transfers:
            transfer.update({
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'fee_lamports': fee_lamports if not sol_transfers else 0,  # Avoid double-counting fees
                'success': True,
                'tx_type': 'TOKEN_TRANSFER'
            })
            parsed_transactions.append(transfer)

        # If no transfers found but transaction succeeded, create a fee-only entry
        if not parsed_transactions and succeeded:
            parsed_tx = {
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'OTHER',
                'from': target_address,
                'to': target_address,
                'value': 0,
                'fee_lamports': fee_lamports,
                'success': True,
                'mint_address': 'So11111111111111111111111111111111111111112',
                'decimals': 9
            }
            parsed_transactions.append(parsed_tx)

        return parsed_transactions

    def _extract_sol_transfers(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Extract native SOL transfers from transaction."""
        transfers = []
        
        # Method 1: Look in tokenTransfers (Helius sometimes puts SOL here as WSOL)
        token_transfers = raw_tx.get('tokenTransfers', [])
        for transfer in token_transfers:
            if transfer.get('mint') == 'So11111111111111111111111111111111111111112':
                from_addr = transfer.get('fromUserAccount', '')
                to_addr = transfer.get('toUserAccount', '')
                amount = transfer.get('tokenAmount', 0)
                
                if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                    transfers.append({
                        'from': from_addr,
                        'to': to_addr,
                        'value': amount,
                        'mint_address': 'So11111111111111111111111111111111111111112',
                        'decimals': 9
                    })

        # Method 2: Look in nativeTransfers (more common for raw SOL)
        native_transfers = raw_tx.get('nativeTransfers', [])
        for transfer in native_transfers:
            from_addr = transfer.get('fromUserAccount', '')
            to_addr = transfer.get('toUserAccount', '')
            lamports = transfer.get('amount', 0)
            
            if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                transfers.append({
                    'from': from_addr,
                    'to': to_addr,
                    'value': lamports / LAMPORTS_TO_SOL, # Value is already in SOL
                    'mint_address': 'So11111111111111111111111111111111111111112',
                    'decimals': 9
                })

        return transfers

    def _extract_token_transfers(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Extract SPL token transfers from transaction."""
        transfers = []
        
        token_transfers = raw_tx.get('tokenTransfers', [])
        for transfer in token_transfers:
            mint = transfer.get('mint', '')
            
            # Skip SOL (handled in _extract_sol_transfers)
            if mint == 'So11111111111111111111111111111111111111112':
                continue
                
            from_addr = transfer.get('fromUserAccount', '')
            to_addr = transfer.get('toUserAccount', '')

            if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                token_info = self.price_converter.get_token_info(mint)
                amount = transfer.get('tokenAmount', 0)

                transfers.append({
                    'from': from_addr,
                    'to': to_addr,
                    'value': amount,
                    'mint_address': mint,
                    'decimals': token_info['decimals']
                })
        return transfers

    def _parse_balance_changes(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Fallback method to parse SOL transfers from balance changes."""
        transfers = []
        
        try:
            meta = raw_tx.get('meta', {})
            pre_balances = meta.get('preBalances', [])
            post_balances = meta.get('postBalances', [])
            account_keys = raw_tx.get('transaction', {}).get('message', {}).get('accountKeys', [])
            
            if len(pre_balances) == len(post_balances) == len(account_keys):
                target_index = -1
                
                # Find target address in account keys
                for i, account in enumerate(account_keys):
                    if account.lower() == target_address.lower():
                        target_index = i
                        break
                
                if target_index >= 0:
                    pre_balance = pre_balances[target_index]
                    post_balance = post_balances[target_index]
                    balance_change = post_balance - pre_balance
                    
                    if balance_change != 0:
                        # Determine direction
                        if balance_change > 0:
                            # Received SOL
                            transfers.append({
                                'from': '',  # Unknown sender
                                'to': target_address,
                                'value': balance_change / LAMPORTS_TO_SOL,
                                'mint_address': 'So11111111111111111111111111111111111111112',
                                'decimals': 9
                            })
                        else:
                            # Sent SOL (excluding fees)
                            fee = meta.get('fee', 0)
                            actual_sent = abs(balance_change) - fee
                            if actual_sent > 0:
                                transfers.append({
                                    'from': target_address,
                                    'to': '',  # Unknown recipient
                                    'value': actual_sent / LAMPORTS_TO_SOL,
                                    'mint_address': 'So11111111111111111111111111111111111111112',
                                    'decimals': 9
                                })
        
        except Exception as e:
            print(f"    -> Warning: Error parsing balance changes: {e}")
        
        return transfers


class SolanaFeatureCalculator:
    """Calculates Bitcoin-compatible features from Solana transaction data."""
    
    def __init__(self, price_converter: SolanaPriceConverter):
        self.price_converter = price_converter
    
    def calculate_features(self, address: str, transactions: List[Dict]) -> Optional[Dict]:
        """Main feature calculation logic for Solana transactions."""
        if not transactions:
            return None
        
        address = address.lower()
        
        # Separate lists for different value calculations (all in BTC equivalent)
        sent_txs, received_txs, all_values_btc, all_fees_btc = [], [], [], []
        slots, counterparties = [], Counter()
        
        # Solana-specific metrics
        failed_txs = 0
        sol_txs = 0
        token_txs = 0
        unique_tokens = set()

        for tx in transactions:
            timestamp = int(tx.get('timestamp', 0))
            if timestamp == 0:
                continue

            slot = int(tx.get('slot', 0))
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            tx_type = tx.get('tx_type', 'UNKNOWN')
            success = tx.get('success', True)
            
            # Track failed transactions
            if not success:
                failed_txs += 1

            # Calculate value in SOL, then convert to BTC
            value_sol = 0.0
            if tx.get('value', 0) > 0:
                if tx_type == 'SOL_TRANSFER':
                    value_sol = float(tx['value'])
                    sol_txs += 1
                elif tx_type == 'TOKEN_TRANSFER':
                    # Convert token to SOL using our price converter
                    mint_address = tx.get('mint_address', '')
                    token_amount = float(tx['value'])
                    
                    if mint_address and token_amount > 0:
                        token_sol_ratio = self.price_converter.get_token_sol_ratio(mint_address, timestamp)
                        value_sol = token_amount * token_sol_ratio
                        
                        if value_sol > 0:
                            token_info = self.price_converter.get_token_info(mint_address)
                            token_symbol = token_info['symbol']
                            print(f"     -> Conversion: {token_amount:.4f} {token_symbol} * {token_sol_ratio:.8f} = {value_sol:.8f} SOL")
                            unique_tokens.add(mint_address)
                            token_txs += 1

            # Calculate fees (always in SOL)
            fee_lamports = int(tx.get('fee_lamports', 0))
            fee_sol = fee_lamports / LAMPORTS_TO_SOL

            # Convert to BTC
            sol_btc_ratio = self.price_converter.get_sol_btc_ratio(timestamp)
            value_btc = value_sol * sol_btc_ratio
            fee_btc = fee_sol * sol_btc_ratio

            slots.append(slot)

            # Classify transaction direction
            if tx_from == address:  # Outgoing
                all_fees_btc.append(fee_btc)
                if value_btc > 0:
                    sent_txs.append({
                        'value_btc': value_btc,
                        'fee_btc': fee_btc,
                        'slot': slot,
                        'tx_type': tx_type
                    })
                    all_values_btc.append(value_btc)
                    
                    # Add counterparty (filter out known programs)
                    if tx_to and tx_to not in KNOWN_PROGRAM_ADDRESSES:
                        counterparties[tx_to] += 1

            if tx_to == address:  # Incoming
                if value_btc > 0:
                    received_txs.append({
                        'value_btc': value_btc,
                        'slot': slot,
                        'tx_type': tx_type
                    })
                    all_values_btc.append(value_btc)
                    
                    # Add counterparty (filter out known programs)
                    if tx_from and tx_from not in KNOWN_PROGRAM_ADDRESSES:
                        counterparties[tx_from] += 1

        return self._aggregate_features(
            transactions, slots, sent_txs, received_txs, all_values_btc, all_fees_btc,
            counterparties, failed_txs, sol_txs, token_txs, len(unique_tokens)
        )

    def _aggregate_features(self, transactions, slots, sent_txs, received_txs, all_values_btc, 
                          all_fees_btc, counterparties, failed_txs, sol_txs, token_txs, unique_tokens):
        """Aggregate all calculated statistics into feature dictionary."""
        features = {}
        
        # Basic transaction counts
        features['num_txs_as_sender'] = float(len(sent_txs))
        features['num_txs_as_receiver'] = float(len(received_txs))
        features['total_txs'] = float(len(transactions))
        
        # Solana-specific features
        features['failed_txs'] = float(failed_txs)
        features['success_rate'] = float((len(transactions) - failed_txs) / len(transactions)) if transactions else 0.0
        features['sol_txs'] = float(sol_txs)
        features['token_txs'] = float(token_txs)
        features['unique_tokens_transacted'] = float(unique_tokens)
        features['sol_to_token_ratio'] = float(sol_txs / token_txs) if token_txs > 0 else float('inf')

        # Slot-based features (equivalent to block-based features in Bitcoin/Ethereum)
        if slots:
            slots = [s for s in slots if s > 0]
            features['first_slot_appeared_in'] = float(min(slots)) if slots else 0.0
            features['last_slot_appeared_in'] = float(max(slots)) if slots else 0.0
            features['lifetime_in_slots'] = features['last_slot_appeared_in'] - features['first_slot_appeared_in']
            features['num_timesteps_appeared_in'] = float(len(set(slots)))
        else:
            features.update({
                'first_slot_appeared_in': 0.0,
                'last_slot_appeared_in': 0.0,
                'lifetime_in_slots': 0.0,
                'num_timesteps_appeared_in': 0.0
            })

        # First transaction slots by direction
        sent_slots = sorted([tx['slot'] for tx in sent_txs if tx['slot'] > 0])
        received_slots = sorted([tx['slot'] for tx in received_txs if tx['slot'] > 0])
        features['first_sent_slot'] = float(sent_slots[0]) if sent_slots else 0.0
        features['first_received_slot'] = float(received_slots[0]) if received_slots else 0.0

        # BTC-equivalent value statistics
        self._add_stats(features, 'btc_transacted', all_values_btc)
        self._add_stats(features, 'btc_sent', [tx['value_btc'] for tx in sent_txs])
        self._add_stats(features, 'btc_received', [tx['value_btc'] for tx in received_txs])
        self._add_stats(features, 'fees', all_fees_btc)

        # Fee percentage calculations
        fee_shares = [(tx['fee_btc'] / tx['value_btc']) * 100 for tx in sent_txs if tx['value_btc'] > 0]
        self._add_stats(features, 'fees_as_share', fee_shares)

        # Interval statistics
        self._add_interval_stats(features, 'slots_btwn_txs', sorted(set(slots)))
        self._add_interval_stats(features, 'slots_btwn_input_txs', sent_slots)
        self._add_interval_stats(features, 'slots_btwn_output_txs', received_slots)

        # Counterparty analysis
        features['transacted_w_address_total'] = float(len(counterparties))
        features['num_addr_transacted_multiple'] = float(sum(1 for count in counterparties.values() if count > 1))
        self._add_stats(features, 'transacted_w_address', list(counterparties.values()), include_total=False)
        
        return features

    def _add_stats(self, features: Dict, prefix: str, values: List[float], include_total: bool = True):
        """Add statistical aggregations for a list of values."""
        if values:
            if include_total:
                features[f'{prefix}_total'] = float(sum(values))
            features[f'{prefix}_min'] = float(min(values))
            features[f'{prefix}_max'] = float(max(values))
            features[f'{prefix}_mean'] = float(np.mean(values))
            features[f'{prefix}_median'] = float(np.median(values))
        else:
            if include_total:
                features[f'{prefix}_total'] = 0.0
            features.update({
                f'{prefix}_min': 0.0,
                f'{prefix}_max': 0.0,
                f'{prefix}_mean': 0.0,
                f'{prefix}_median': 0.0
            })

    def _add_interval_stats(self, features: Dict, prefix: str, sorted_slots: List[int]):
        """Calculate statistics for intervals between slots."""
        if len(sorted_slots) > 1:
            intervals = [float(sorted_slots[i] - sorted_slots[i-1]) for i in range(1, len(sorted_slots))]
            self._add_stats(features, prefix, intervals)
        else:
            self._add_stats(features, prefix, [])


class SolanaProcessor:
    """Main orchestrator for processing Solana addresses."""
    
    def __init__(self, api_key: str = HELIUS_API_KEY):
        self.extractor = SolanaDataExtractor(api_key)
        self.calculator = SolanaFeatureCalculator(self.extractor.price_converter)
    
    def process_from_csv(self, csv_file_path: str, output_file: str = 'solana_features_output.csv'):
        """Process addresses from CSV with periodic saving and resume capability."""
        try:
            df_source = pd.read_csv(csv_file_path)
            addresses_to_process = [(row['Address'], row['FLAG']) for _, row in df_source.iterrows()]
            print(f"📖 Found {len(addresses_to_process)} total addresses in source file.")
        except Exception as e:
            print(f"❌ Error reading source CSV '{csv_file_path}': {e}")
            return

        all_results, processed_addresses = self._load_previous_results(output_file)
        
        processed_in_session = 0
        for i, (address, flag) in enumerate(addresses_to_process, 1):
            if address in processed_addresses:
                continue

            print(f"\n--- Processing {i}/{len(addresses_to_process)}: {address} ---")
            
            try:
                transactions = self.extractor.get_all_transactions(address)
                if not transactions:
                    print(f"No transactions found for {address}. Skipping.")
                    continue
                
                features = self.calculator.calculate_features(address, transactions)
                if features:
                    features['address'] = address
                    features['class'] = flag
                    all_results.append(features)
                    processed_in_session += 1
                    print(f"✅ {address}: Features calculated successfully.")
                
                # Periodic save every 5 addresses (more frequent due to Solana complexity)
                if processed_in_session > 0 and processed_in_session % 5 == 0:
                    self._save_results(all_results, output_file)

            except Exception as e:
                print(f"❌ Unhandled error processing {address}: {e}")
                import traceback
                traceback.print_exc()
        
        # Final save
        if processed_in_session > 0:
            print("\n✅ Processing complete. Performing final save...")
            self._save_results(all_results, output_file)
            print(f"\n📊 Session Summary:")
            print(f"   - Processed: {processed_in_session} new addresses")
            print(f"   - Total in dataset: {len(all_results)} addresses")
        else:
            print("\nNo new addresses were processed in this session.")

    def _load_previous_results(self, output_file: str) -> tuple[List[Dict], set]:
        """Load previously processed data to allow resuming."""
        if os.path.exists(output_file):
            try:
                df_existing = pd.read_csv(output_file)
                if 'address' in df_existing.columns:
                    results = df_existing.to_dict('records')
                    processed = set(df_existing['address'])
                    print(f"✅ Resuming. Loaded {len(processed)} previously processed addresses from '{output_file}'.")
                    return results, processed
            except Exception as e:
                print(f"⚠️ Could not read existing output file '{output_file}'. Starting fresh. Error: {e}")
        return [], set()

    def _save_results(self, results: List[Dict], output_file: str):
        """Save current results to CSV and update price cache."""
        if not results:
            return
        
        df = pd.DataFrame(results)
        
        # Clean and standardize data types
        for col in df.columns:
            if col not in ['address', 'class']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        df.to_csv(output_file, index=False)
        self.extractor.price_converter.save_price_cache()
        print(f"💾 Progress saved. {len(df)} total addresses now in '{output_file}'.")

    def process_single_address(self, address: str, verbose: bool = True) -> Optional[Dict]:
        """Process a single address for testing purposes."""
        if verbose:
            print(f"\n🔍 Testing single address: {address}")
        
        try:
            transactions = self.extractor.get_all_transactions(address)
            if not transactions:
                if verbose:
                    print("No transactions found.")
                return None
            
            features = self.calculator.calculate_features(address, transactions)
            if features and verbose:
                print(f"✅ Successfully calculated {len(features)} features")
                print(f"   - Total transactions: {features.get('total_txs', 0)}")
                print(f"   - BTC transacted: {features.get('btc_transacted_total', 0):.8f}")
                print(f"   - Unique tokens: {features.get('unique_tokens_transacted', 0)}")
            
            return features
            
        except Exception as e:
            if verbose:
                print(f"❌ Error processing address: {e}")
                import traceback
                traceback.print_exc()
            return None


# --- Main Execution ---
if __name__ == "__main__":
    # Configuration check
    if HELIUS_API_KEY == "your_helius_api_key_here":
        print("❌ Please update your API keys in the configuration section before running!")
        print("Required API keys:")
        print("   - HELIUS_API_KEY (primary data source)")
        print("   - CRYPTOCOMPARE_API_KEY (price data)")
        print("   - MORALIS_API_KEY (backup data source)")
        exit(1)
    
    processor = SolanaProcessor()
    
    # Example usage
    print("🚀 Solana Blockchain Analyzer")
    print("=" * 50)
    
    # Test with a single address first (recommended)
    test_address = input("Enter a Solana address to test (or press Enter to skip): ").strip()
    if test_address:
        print(f"\n🧪 Testing with address: {test_address}")
        features = processor.process_single_address(test_address)
        if features:
            print(f"\n📋 Sample features calculated:")
            for key, value in list(features.items())[:10]:  # Show first 10 features
                print(f"   {key}: {value}")
        
        proceed = input("\nProceed with CSV processing? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Exiting...")
            exit(0)
    
    # CSV processing
    csv_path = input("Enter path to your CSV file with Solana addresses: ").strip()
    
    if not csv_path:
        csv_path = "solana_addresses.csv"  # Default filename
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        print("Please ensure your CSV has columns: 'Address' and 'FLAG'")
        
        # Create sample CSV
        sample_data = {
            'Address': [
                '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            ],
            'FLAG': [1, 0]
        }
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_csv('sample_solana_addresses.csv', index=False)
        print("📝 Created sample file: sample_solana_addresses.csv")
    else:
        print(f"📂 Processing file: {csv_path}")
        processor.process_from_csv(csv_path)