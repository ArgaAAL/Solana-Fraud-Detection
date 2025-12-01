import requests
import pandas as pd
import numpy as np
import json
import time
import os
from collections import defaultdict, Counter
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timezone

# --- Configuration ---
HELIUS_API_KEY = "YOUR_HELIUS_KEY_HERE"
CRYPTOCOMPARE_API_KEY = "YOUR_CRYPTOCOMPARE_KEY_HERE"
MORALIS_API_KEY = "YOUR_MORALIS_KEY_HERE"

# --- Constants ---
LAMPORTS_TO_SOL = 10**9
API_DELAY = 0.5
MAX_RETRIES = 3
HELIUS_MAX_RECORDS = 1000
MAX_TRANSACTIONS_PER_ADDRESS = 50000
JUPITER_API_DELAY = 1.0

# Comprehensive Solana program addresses for filtering
COMPREHENSIVE_PROGRAM_ADDRESSES = {
    # System Programs
    '11111111111111111111111111111112',  # System Program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token Program  
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',  # Token Program 2022
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',  # Associated Token Program
    
    # DEXs
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium AMM
    'CAMMCzo5YL8w4VFF8KVHrK22GGUQpMDdHwMBSPBy4kD',   # Raydium CLMM
    '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  # Orca
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',   # Orca Whirlpools
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',   # Jupiter V6
    'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',   # Jupiter V4
    'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',  # Orca V1
    'EhYXq3ANp5nAerUpbSgd7VK2RRcxK1zNuSQ755G5Mtc1',   # Orca V2
    
    # Serum DEX
    'EUqojwWA2rd19FZrzeBncJsm38Jm1hEhE3zsmX3bRc2o',   # Serum DEX
    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',   # Serum DEX V3
    'BJ3jrUzddfuSrZHXSCxMbUE2yoHqpiUWyypURhoxiFwZ',   # Serum DEX V2
    
    # Lending Protocols
    'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',   # Solend
    '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg',   # Mango V3
    'mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68',    # Mango V4
    'LendZqTs7gn5CTSJU1jWKhKuVpjg9avMpS7FgG7V4CJ',   # Port Finance
    'FC81tbGt6JWRXidaWYFXxGnTk2VgEYrLR9c2YLGgCu8z',   # Francium
    
    # Staking Programs
    'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',   # Marinade Finance
    'StakeSSzfxn391k3LvdKbZP5WVwWd6AsY39qcgwy7f3J',   # Native Staking
    'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',   # JitoSOL
    'SP12tWFxD9oJsVWNavTTBZvMbA6gkAmxtVgxdqvyvhY',    # Stake Pool Program
    'Zap9yosk9j9Jc1GLyYQ9rQHY8oPrBF5iqHoZdFYHoW',     # Socean
    
    # Cross-chain Bridges
    'worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth',   # Wormhole
    'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb',    # Wormhole Token Bridge
    'HDwcJBJXjL9FpJ7UBsYBtaDjsBUhuLCUYoz3zr8SWWaQ',   # Wormhole NFT Bridge
    'A94X2fRy3wydNShU4dRaDyap2UuoeWJGWyATtyp61WVf',   # AllBridge
    
    # NFT Marketplaces
    'CJsLwbP1iu5DuUikHEJnLfANgKy6stB2uFgvBBHoyxwz',   # Solanart
    'hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk',    # Metaplex Auction House
    'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K',    # Magic Eden V1
    'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K',    # Magic Eden V2
    
    # Other DeFi
    'CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR',   # Lifinity
    'AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6',    # Lifinity V2
    'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ',    # Saber
    'CropUGUScj1h4KoGx47n8yXwkzLHFMeGtLhNLrG3TCxs',   # Crop Finance
    'TokenSwapV1M41u6Xd9fgY4wXDrPmKUkKGfTLnNGN',       # Aldrin
    'AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6',    # Mercurial
    'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky',     # Mercurial V2
    
    # Pyth Oracle
    'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH',    # Pyth Oracle
    'gSbePebfvPy7tRqimPoVecS2UsBvYv46ynrzWocc92s',     # Pyth Program
    
    # System/Governance
    'Gov1BBdCNNqVD39vdFm93vVEwX7xEYqR3AwKbyKPP4',      # SPL Governance
    'GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw',     # Realms Governance
}

# Stablecoin addresses for validation
STABLECOIN_ADDRESSES = {
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'USDC',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 'USDT', 
    '4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU': 'USDC',  # USDC on other markets
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So': 'mSOL',   # Marinade SOL
    'So11111111111111111111111111111111111111112': 'WSOL',   # Wrapped SOL
}


class SolanaPriceConverter:
    """Enhanced price conversion with better validation and error handling."""
    
    def __init__(self):
        self.price_cache = {}
        self.token_price_cache = {}
        self.token_info_cache = {}
        self.price_validation_failures = 0
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
        cache_data = {
            'sol_btc': self.price_cache, 
            'token_sol': self.token_price_cache, 
            'token_info': self.token_info_cache
        }
        try:
            with open('solana_price_cache.json', 'w') as f:
                json.dump(cache_data, f, indent=2)
            print(f"💾 Price cache saved. Validation failures: {self.price_validation_failures}")
        except Exception as e:
            print(f"❌ Error saving price cache: {e}")

    def normalize_token_amount(self, raw_amount: int, decimals: int) -> float:
        """Convert raw token amount to human-readable decimal amount."""
        if decimals < 0 or decimals > 18:
            print(f"⚠️ Suspicious decimals value: {decimals}. Using 9.")
            decimals = 9
        return float(raw_amount) / (10 ** decimals)

    def get_token_info(self, mint_address: str) -> Dict[str, Any]:
        """Get token metadata with enhanced validation."""
        mint_address = mint_address.strip()
        
        if mint_address in self.token_info_cache:
            return self.token_info_cache[mint_address]

        # Enhanced token map with proper decimals
        token_map = {
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': {'symbol': 'USDC', 'decimals': 6, 'name': 'USD Coin'},
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': {'symbol': 'USDT', 'decimals': 6, 'name': 'Tether'},
            'So11111111111111111111111111111111111111112': {'symbol': 'WSOL', 'decimals': 9, 'name': 'Wrapped SOL'},
            '9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E': {'symbol': 'BTC', 'decimals': 6, 'name': 'Bitcoin (Sollet)'},
            'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So': {'symbol': 'mSOL', 'decimals': 9, 'name': 'Marinade staked SOL'},
            'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn': {'symbol': 'jitoSOL', 'decimals': 9, 'name': 'Jito Staked SOL'},
            'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1': {'symbol': 'bSOL', 'decimals': 9, 'name': 'BlazeStake Staked SOL'},
        }
        
        if mint_address in token_map:
            self.token_info_cache[mint_address] = token_map[mint_address]
            return token_map[mint_address]

        # Try Helius first
        helius_info = self._fetch_token_info_helius(mint_address)
        if helius_info.get('symbol') != 'UNKNOWN' and self._validate_token_info(helius_info):
            self.token_info_cache[mint_address] = helius_info
            return helius_info

        # Fallback to Moralis
        moralis_info = self._fetch_token_info_moralis(mint_address)
        if moralis_info.get('symbol') != 'UNKNOWN' and self._validate_token_info(moralis_info):
            self.token_info_cache[mint_address] = moralis_info
            return moralis_info

        # Final fallback
        unknown_info = {'symbol': 'UNKNOWN', 'decimals': 9, 'name': 'Unknown Token'}
        self.token_info_cache[mint_address] = unknown_info
        return unknown_info

    def _validate_token_info(self, token_info: Dict[str, Any]) -> bool:
        """Validate token info for obvious errors."""
        decimals = token_info.get('decimals', 0)
        symbol = token_info.get('symbol', '')
        
        # Check decimals range
        if decimals < 0 or decimals > 18:
            return False
            
        # Check symbol sanity
        if not symbol or len(symbol) > 20 or symbol == 'UNKNOWN':
            return False
            
        return True

    def _fetch_token_info_helius(self, mint_address: str) -> Dict[str, Any]:
        """Fetch token metadata from Helius API with better error handling."""
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

    def get_token_sol_ratio(self, mint_address: str, timestamp: int) -> Tuple[float, bool]:
        """Get token-to-SOL conversion ratio with validation. Returns (ratio, success)."""
        token_info = self.get_token_info(mint_address)
        token_symbol = token_info['symbol']

        # WSOL is 1:1 with SOL
        if token_symbol == 'WSOL' or mint_address == 'So11111111111111111111111111111111111111112':
            return 1.0, True

        # Use daily cache for better accuracy
        dt_object = datetime.fromtimestamp(timestamp)
        daily_key = dt_object.strftime('%Y-%m-%d')
        cache_key = f"{token_symbol}_{daily_key}_{mint_address[:20]}"

        if cache_key in self.token_price_cache:
            cached_value = self.token_price_cache[cache_key]
            return cached_value, cached_value > 0

        # Handle stablecoins with validation
        if token_symbol in ['USDC', 'USDT', 'BUSD', 'DAI']:
            ratio, success = self._get_stablecoin_sol_ratio(timestamp)
            if success and self._validate_stablecoin_price(ratio, timestamp):
                self.token_price_cache[cache_key] = ratio
                return ratio, True

        print(f"   -> PRICE FETCH: Getting price for {token_symbol} ({mint_address[:20]}...)")
        
        # Multi-layer price fetching with validation
        price, success = self._fetch_token_price_jupiter(mint_address, timestamp)
        if success and self._validate_price_data(price, token_symbol, timestamp):
            self.token_price_cache[cache_key] = price
            return price, True

        price, success = self._fetch_token_price_coingecko(token_symbol, timestamp)
        if success and self._validate_price_data(price, token_symbol, timestamp):
            self.token_price_cache[cache_key] = price
            return price, True

        price, success = self._fetch_token_price_cryptocompare(token_symbol, timestamp)
        if success and self._validate_price_data(price, token_symbol, timestamp):
            self.token_price_cache[cache_key] = price
            return price, True

        print(f"      -> ❌ ALL APIs FAILED for {token_symbol}")
        self.price_validation_failures += 1
        self.token_price_cache[cache_key] = 0.0
        return 0.0, False

    def _validate_price_data(self, price_sol: float, token_symbol: str, timestamp: int) -> bool:
        """Validate price data for obvious errors."""
        
        # Check for impossible prices
        if price_sol <= 0 or price_sol > 1000000:  # More than 1M SOL per token
            print(f"      -> ❌ Invalid price range: {price_sol}")
            return False
        
        # Check for stablecoin price sanity
        if token_symbol in ['USDC', 'USDT', 'BUSD', 'DAI']:
            sol_usd, _ = self._get_sol_price_usd(timestamp)
            if sol_usd > 0:
                implied_usd_price = price_sol * sol_usd
                if not (0.95 <= implied_usd_price <= 1.05):  # Should be ~$1
                    print(f"      -> ❌ Stablecoin price validation failed: ${implied_usd_price:.4f}")
                    return False
        
        return True

    def _validate_stablecoin_price(self, sol_ratio: float, timestamp: int) -> bool:
        """Special validation for stablecoin prices."""
        sol_usd, success = self._get_sol_price_usd(timestamp)
        if not success or sol_usd <= 0:
            return False
            
        implied_usd_price = sol_ratio * sol_usd
        return 0.95 <= implied_usd_price <= 1.05

    def _fetch_token_price_jupiter(self, mint_address: str, timestamp: int) -> Tuple[float, bool]:
        """Layer 1: Jupiter API with enhanced validation (uses current Price API V3)."""
        print(f"      -> [Layer 1] Jupiter API for current price...")
        try:
            days_old = (time.time() - timestamp) / (24 * 3600)
            if days_old > 7:  # Only use Jupiter for very recent prices
                print(f"      -> ❌ Jupiter: Transaction too old ({days_old:.1f} days)")
                return 0.0, False

            url = "https://lite-api.jup.ag/price/v3"  # current supported endpoint; legacy price.jup.ag/v4 is deprecated. :contentReference[oaicite:3]{index=3} :contentReference[oaicite:4]{index=4}
            params = {"ids": mint_address}

            response = requests.get(url, params=params, timeout=15)
            time.sleep(JUPITER_API_DELAY)

            if response.status_code != 200:
                print(f"      -> ❌ Jupiter FAILED: HTTP {response.status_code} - {response.text[:200]}")
                return 0.0, False

            data = response.json()
            token_info = data.get(mint_address)
            if not token_info:
                print(f"      -> ❌ Jupiter FAILED: no data for mint {mint_address} in response.")
                return 0.0, False

            usd_price = token_info.get("usdPrice")  # V3 uses 'usdPrice'. :contentReference[oaicite:5]{index=5}
            if usd_price is None or usd_price <= 0:
                print(f"      -> ❌ Jupiter FAILED: invalid usdPrice ({usd_price}) for {mint_address}.")
                return 0.0, False

            # Get SOL price in USD to convert token USD price to SOL
            sol_usd, sol_success = self._get_sol_price_usd(timestamp)
            if not sol_success or sol_usd <= 0:
                print(f"      -> ❌ Jupiter FAILED: invalid SOL/USD ratio ({sol_usd})")
                return 0.0, False

            sol_ratio = usd_price / sol_usd
            print(f"      -> ✅ Jupiter SUCCESS: {sol_ratio:.8f} SOL (token ${usd_price:.6f} / SOL ${sol_usd:.6f})")
            return sol_ratio, True

        except Exception as e:
            print(f"      -> ❌ Jupiter EXCEPTION: {e}")
            return 0.0, False

    def _fetch_token_price_coingecko(self, token_symbol: str, timestamp: int) -> Tuple[float, bool]:
        """Layer 2: CoinGecko API with better error handling."""
        print(f"      -> [Layer 2] CoinGecko API...")
        try:
            dt = datetime.fromtimestamp(timestamp)
            date_str = dt.strftime('%d-%m-%Y')

            # Search for coin
            search_url = f"https://api.coingecko.com/api/v3/search"
            params = {'query': token_symbol}
            
            response = requests.get(search_url, params=params, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                search_data = response.json()
                coins = search_data.get('coins', [])
                
                coin_id = None
                for coin in coins:
                    if coin.get('symbol', '').upper() == token_symbol.upper():
                        coin_id = coin.get('id')
                        break

                if coin_id:
                    history_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
                    history_params = {'date': date_str}
                    
                    hist_response = requests.get(history_url, params=history_params, timeout=15)
                    time.sleep(API_DELAY)

                    if hist_response.status_code == 200:
                        hist_data = hist_response.json()
                        market_data = hist_data.get('market_data', {})
                        current_price = market_data.get('current_price', {})
                        usd_price = current_price.get('usd')

                        if usd_price and usd_price > 0:
                            sol_usd, sol_success = self._get_sol_price_usd(timestamp)
                            if sol_success and sol_usd > 0:
                                sol_ratio = usd_price / sol_usd
                                print(f"      -> ✅ CoinGecko SUCCESS: {sol_ratio:.8f} SOL")
                                return sol_ratio, True

            return 0.0, False

        except Exception as e:
            print(f"      -> ❌ CoinGecko EXCEPTION: {e}")
            return 0.0, False

    def _fetch_token_price_cryptocompare(self, token_symbol: str, timestamp: int) -> Tuple[float, bool]:
        """Layer 3: CryptoCompare API."""
        print(f"      -> [Layer 3] CryptoCompare API...")
        try:
            # Use daily precision for better accuracy
            daily_dt = datetime.fromtimestamp(timestamp).replace(hour=0, minute=0, second=0)
            daily_timestamp = int(daily_dt.timestamp())
            
            params = {
                'fsym': token_symbol,
                'tsyms': 'SOL',
                'ts': daily_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }

            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)

            if response.status_code == 200:
                data = response.json()
                token_price = data.get(token_symbol, {}).get('SOL')
                if token_price and token_price > 0:
                    print(f"      -> ✅ CryptoCompare SUCCESS: {token_price:.8f} SOL")
                    return token_price, True

            return 0.0, False

        except Exception as e:
            print(f"      -> ❌ CryptoCompare EXCEPTION: {e}")
            return 0.0, False

    def _get_stablecoin_sol_ratio(self, timestamp: int) -> Tuple[float, bool]:
        """Get USD-to-SOL conversion ratio for stablecoins."""
        sol_usd, success = self._get_sol_price_usd(timestamp)
        if success and sol_usd > 0:
            return 1.0 / sol_usd, True
        return 0.0, False

    def _get_sol_price_usd(self, timestamp: int) -> Tuple[float, bool]:
        """Get SOL price in USD with validation."""
        dt_object = datetime.fromtimestamp(timestamp)
        daily_key = dt_object.strftime('%Y-%m-%d')
        cache_key = f"SOL_USD_{daily_key}"
        
        if cache_key in self.price_cache:
            sol_usd_price = self.price_cache[cache_key]
            return sol_usd_price, sol_usd_price > 0

        print(f"   -> Fetching SOL/USD price for {daily_key}...")
        try:
            daily_timestamp = int(datetime.strptime(daily_key, '%Y-%m-%d').timestamp())
            params = {
                'fsym': 'SOL',
                'tsyms': 'USD',
                'ts': daily_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }
            
            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)
            
            if response.status_code == 200:
                data = response.json()
                sol_usd_price = data.get('SOL', {}).get('USD')
                if sol_usd_price and sol_usd_price > 0:
                    # Validate SOL price is reasonable
                    if 1 <= sol_usd_price <= 1000:  # SOL has never been below $1 or above $1000
                        self.price_cache[cache_key] = sol_usd_price
                        return sol_usd_price, True

            return 0.0, False

        except Exception as e:
            print(f"   -> Error: SOL/USD fetch failed: {e}")
            return 0.0, False

    def get_sol_btc_ratio(self, timestamp: int) -> float:
        """Get SOL-to-BTC conversion ratio with daily precision."""
        dt_object = datetime.fromtimestamp(timestamp)
        daily_key = dt_object.strftime('%Y-%m-%d')
        cache_key = f"SOL_BTC_{daily_key}"
        
        if cache_key in self.price_cache:
            return self.price_cache[cache_key]

        print(f"-> Fetching SOL/BTC price for {daily_key}...")
        try:
            daily_timestamp = int(datetime.strptime(daily_key, '%Y-%m-%d').timestamp())
            params = {
                'fsym': 'SOL',
                'tsyms': 'BTC',
                'ts': daily_timestamp,
                'api_key': CRYPTOCOMPARE_API_KEY
            }
            
            response = requests.get("https://min-api.cryptocompare.com/data/pricehistorical", params=params, timeout=15)
            time.sleep(API_DELAY)
            
            if response.status_code == 200:
                data = response.json()
                sol_price = data.get('SOL', {}).get('BTC')
                if sol_price and 0.0001 <= sol_price <= 0.1:  # Reasonable SOL/BTC range
                    self.price_cache[cache_key] = sol_price
                    return sol_price

            return self._get_fallback_sol_btc_ratio(timestamp)

        except Exception as e:
            print(f"   -> Error: SOL/BTC price fetch failed: {e}")
            return self._get_fallback_sol_btc_ratio(timestamp)

    def _get_fallback_sol_btc_ratio(self, timestamp: int) -> float:
        """Improved fallback SOL/BTC ratios."""
        year = datetime.fromtimestamp(timestamp).year
        month = datetime.fromtimestamp(timestamp).month
        
        if year <= 2021:
            if month <= 6:
                return 0.0005  # Early 2021
            else:
                return 0.002   # SOL bull run
        elif year == 2022:
            if month <= 6:
                return 0.003   # Peak bull market
            else:
                return 0.001   # Bear market begins
        elif year == 2023:
            return 0.0008      # Bear market
        else:
            return 0.002       # Current estimates


class TransactionClassifier:
    """Classifies Solana transactions by their economic context."""
    
    def __init__(self):
        self.dex_programs = {
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium
            '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  # Orca
            'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',   # Jupiter
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',   # Orca Whirlpools
        }
        
        self.lending_programs = {
            'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',   # Solend
            '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg',   # Mango
            'LendZqTs7gn5CTSJU1jWKhKuVpjg9avMpS7FgG7V4CJ',   # Port Finance
        }
        
        self.staking_programs = {
            'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',   # Marinade
            'StakeSSzfxn391k3LvdKbZP5WVwWd6AsY39qcgwy7f3J',   # Native Staking
            'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',   # JitoSOL
        }

    def classify_transaction_context(self, raw_tx: Dict) -> str:
        """Classify transaction by its economic context."""
        try:
            instructions = raw_tx.get('transaction', {}).get('message', {}).get('instructions', [])
            
            # Extract program IDs from instructions
            program_ids = set()
            for instr in instructions:
                if isinstance(instr, dict):
                    program_id = instr.get('programId', '')
                    if program_id:
                        program_ids.add(program_id)
            
            # Check for DEX activity
            if program_ids.intersection(self.dex_programs):
                return 'DEX_SWAP'
            
            # Check for lending
            if program_ids.intersection(self.lending_programs):
                return 'LENDING'
            
            # Check for staking
            if program_ids.intersection(self.staking_programs):
                return 'STAKING'
            
            # Check for pure transfers
            token_transfers = raw_tx.get('tokenTransfers', [])
            native_transfers = raw_tx.get('nativeTransfers', [])
            
            if (token_transfers or native_transfers) and not program_ids.intersection(COMPREHENSIVE_PROGRAM_ADDRESSES):
                return 'PURE_TRANSFER'
            
            return 'OTHER_PROGRAM'
            
        except Exception as e:
            print(f"    -> Warning: Error classifying transaction: {e}")
            return 'UNKNOWN'

    def is_programmatic_transaction(self, raw_tx: Dict) -> bool:
        """Determine if transaction is likely programmatic (bot/contract) vs human."""
        try:
            # High instruction count suggests programmatic
            instructions = raw_tx.get('transaction', {}).get('message', {}).get('instructions', [])
            if len(instructions) > 10:
                return True
            
            # Multiple token transfers in single tx suggests programmatic
            token_transfers = raw_tx.get('tokenTransfers', [])
            if len(token_transfers) > 5:
                return True
            
            # Interaction with known program addresses
            program_ids = set()
            for instr in instructions:
                if isinstance(instr, dict):
                    program_id = instr.get('programId', '')
                    if program_id and program_id in COMPREHENSIVE_PROGRAM_ADDRESSES:
                        program_ids.add(program_id)
            
            return len(program_ids) > 0
            
        except Exception:
            return False


class SolanaDataExtractor:
    """Enhanced Solana data extractor with better parsing and validation."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.price_converter = SolanaPriceConverter()
        self.classifier = TransactionClassifier()

    def get_all_transactions(self, address: str) -> List[Dict]:
        """Get all transactions with enhanced parsing and validation."""
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

            # Parse each transaction with enhanced logic
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
        """Fetch a single page of transactions from Helius with better error handling."""
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
                    wait_time = min(2 ** attempt, 30)  # Cap at 30 seconds
                    print(f"    Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"    API Error: {response.status_code}")
                    if attempt == MAX_RETRIES - 1:
                        return []
                    time.sleep(2 ** attempt)
                    
            except requests.exceptions.RequestException as e:
                print(f"    Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt == MAX_RETRIES - 1:
                    return []
                time.sleep(2 ** attempt)

        return []

    def _parse_solana_transaction(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Enhanced transaction parsing with better validation and context."""
        parsed_transactions = []
        
        # Basic transaction info
        signature = raw_tx.get('signature', '')
        slot = raw_tx.get('slot', 0)
        timestamp = raw_tx.get('timestamp', 0)
        fee_lamports = raw_tx.get('fee', 0)
        
        # Validate basic data
        if not signature or slot <= 0 or timestamp <= 0:
            print(f"    -> Skipping invalid transaction: sig={signature[:20]}, slot={slot}, ts={timestamp}")
            return []
        
        # Check if transaction succeeded
        meta = raw_tx.get('meta', {})
        tx_error = meta.get('err')
        succeeded = tx_error is None
        
        # Get transaction context
        tx_context = self.classifier.classify_transaction_context(raw_tx)
        is_programmatic = self.classifier.is_programmatic_transaction(raw_tx)

        # Handle failed transactions
        if not succeeded:
            parsed_tx = {
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'FAILED',
                'tx_context': tx_context,
                'is_programmatic': is_programmatic,
                'from': target_address,
                'to': target_address,
                'value_normalized': 0.0,
                'value_sol': 0.0,
                'fee_lamports': fee_lamports,
                'success': False,
                'mint_address': 'So11111111111111111111111111111111111111112',
                'decimals': 9,
                'price_fetch_success': True,  # No price needed for failed tx
                'token_symbol': 'SOL'
            }
            parsed_transactions.append(parsed_tx)
            return parsed_transactions

        # Parse successful transactions
        sol_transfers = self._extract_sol_transfers(raw_tx, target_address)
        token_transfers = self._extract_token_transfers(raw_tx, target_address)
        
        # Process SOL transfers
        for transfer in sol_transfers:
            transfer.update({
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'SOL_TRANSFER',
                'tx_context': tx_context,
                'is_programmatic': is_programmatic,
                'fee_lamports': fee_lamports,
                'success': True,
                'price_fetch_success': True,
                'token_symbol': 'SOL',
                'value_sol': transfer['value_normalized']  # Already in SOL
            })
            parsed_transactions.append(transfer)

        # Process token transfers with proper price conversion
        for transfer in token_transfers:
            mint_address = transfer['mint_address']
            raw_value = transfer['value_raw']
            decimals = transfer['decimals']
            
            # Normalize token amount
            normalized_value = self.price_converter.normalize_token_amount(raw_value, decimals)
            
            # Convert to SOL value
            sol_ratio, price_success = self.price_converter.get_token_sol_ratio(mint_address, timestamp)
            value_sol = normalized_value * sol_ratio if price_success else 0.0
            
            transfer.update({
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'TOKEN_TRANSFER',
                'tx_context': tx_context,
                'is_programmatic': is_programmatic,
                'fee_lamports': fee_lamports if not sol_transfers else 0,  # Avoid double-counting
                'success': True,
                'value_normalized': normalized_value,
                'value_sol': value_sol,
                'price_fetch_success': price_success,
                'sol_ratio': sol_ratio
            })
            parsed_transactions.append(transfer)

        # If no transfers found but transaction succeeded, create fee-only entry
        if not parsed_transactions and succeeded:
            parsed_tx = {
                'signature': signature,
                'slot': slot,
                'timestamp': timestamp,
                'tx_type': 'FEE_ONLY',
                'tx_context': tx_context,
                'is_programmatic': is_programmatic,
                'from': target_address,
                'to': target_address,
                'value_normalized': 0.0,
                'value_sol': 0.0,
                'fee_lamports': fee_lamports,
                'success': True,
                'mint_address': 'So11111111111111111111111111111111111111112',
                'decimals': 9,
                'price_fetch_success': True,
                'token_symbol': 'SOL'
            }
            parsed_transactions.append(parsed_tx)

        return parsed_transactions

    def _extract_sol_transfers(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Extract native SOL transfers with better validation."""
        transfers = []
        
        # Method 1: nativeTransfers (most reliable for SOL)
        native_transfers = raw_tx.get('nativeTransfers', [])
        for transfer in native_transfers:
            from_addr = transfer.get('fromUserAccount', '').strip()
            to_addr = transfer.get('toUserAccount', '').strip()
            lamports = transfer.get('amount', 0)
            
            # Validate addresses
            if not from_addr or not to_addr or lamports <= 0:
                continue
                
            if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                sol_amount = lamports / LAMPORTS_TO_SOL
                transfers.append({
                    'from': from_addr,
                    'to': to_addr,
                    'value_normalized': sol_amount,
                    'value_raw': lamports,
                    'mint_address': 'So11111111111111111111111111111111111111112',
                    'decimals': 9,
                    'token_symbol': 'SOL'
                })

        # Method 2: tokenTransfers for WSOL (wrapped SOL)
        token_transfers = raw_tx.get('tokenTransfers', [])
        for transfer in token_transfers:
            if transfer.get('mint') == 'So11111111111111111111111111111111111111112':
                from_addr = transfer.get('fromUserAccount', '').strip()
                to_addr = transfer.get('toUserAccount', '').strip()
                amount = transfer.get('tokenAmount', 0)
                
                if not from_addr or not to_addr or amount <= 0:
                    continue
                
                if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                    # tokenAmount is already normalized for WSOL
                    transfers.append({
                        'from': from_addr,
                        'to': to_addr,
                        'value_normalized': amount,
                        'value_raw': int(amount * LAMPORTS_TO_SOL),
                        'mint_address': 'So11111111111111111111111111111111111111112',
                        'decimals': 9,
                        'token_symbol': 'WSOL'
                    })

        return transfers

    def _extract_token_transfers(self, raw_tx: Dict, target_address: str) -> List[Dict]:
        """Extract SPL token transfers with proper decimal handling."""
        transfers = []
        
        token_transfers = raw_tx.get('tokenTransfers', [])
        for transfer in token_transfers:
            mint = transfer.get('mint', '').strip()
            
            # Skip SOL/WSOL (handled in _extract_sol_transfers)
            if mint == 'So11111111111111111111111111111111111111112' or not mint:
                continue
                
            from_addr = transfer.get('fromUserAccount', '').strip()
            to_addr = transfer.get('toUserAccount', '').strip()
            raw_amount = transfer.get('tokenAmount', 0)

            # Validate transfer data
            if not from_addr or not to_addr or raw_amount <= 0:
                continue
                
            if from_addr.lower() == target_address.lower() or to_addr.lower() == target_address.lower():
                # Get token info for proper decimal handling
                token_info = self.price_converter.get_token_info(mint)
                
                transfers.append({
                    'from': from_addr,
                    'to': to_addr,
                    'value_raw': raw_amount,
                    'mint_address': mint,
                    'decimals': token_info['decimals'],
                    'token_symbol': token_info['symbol']
                })
                
        return transfers


class SolanaFeatureCalculator:
    """Enhanced feature calculator with Solana-specific metrics and data quality indicators."""
    
    def __init__(self, price_converter: SolanaPriceConverter):
        self.price_converter = price_converter
    
    def calculate_features(self, address: str, transactions: List[Dict]) -> Optional[Dict]:
        """Enhanced feature calculation with data quality metrics."""
        if not transactions:
            return None
        
        address = address.lower()
        
        # Separate lists for different calculations
        sent_txs, received_txs, all_values_btc, all_fees_btc = [], [], [], []
        slots, counterparties = [], Counter()
        
        # Enhanced Solana-specific metrics
        failed_txs = 0
        sol_txs = 0
        token_txs = 0
        dex_txs = 0
        lending_txs = 0
        staking_txs = 0
        programmatic_txs = 0
        unique_tokens = set()
        price_failures = 0
        
        # Transaction context tracking
        context_counts = Counter()
        
        # Account creation and rent tracking
        account_creation_costs = 0.0
        
        for tx in transactions:
            timestamp = int(tx.get('timestamp', 0))
            if timestamp == 0:
                continue

            slot = int(tx.get('slot', 0))
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            tx_type = tx.get('tx_type', 'UNKNOWN')
            tx_context = tx.get('tx_context', 'UNKNOWN')
            success = tx.get('success', True)
            is_programmatic = tx.get('is_programmatic', False)
            price_success = tx.get('price_fetch_success', True)
            
            # Track various metrics
            if not success:
                failed_txs += 1
            if is_programmatic:
                programmatic_txs += 1
            if not price_success:
                price_failures += 1
                
            context_counts[tx_context] += 1
            
            # Count transaction types by context
            if tx_context == 'DEX_SWAP':
                dex_txs += 1
            elif tx_context == 'LENDING':
                lending_txs += 1
            elif tx_context == 'STAKING':
                staking_txs += 1

            # Calculate values in SOL and BTC
            value_sol = float(tx.get('value_sol', 0))
            fee_lamports = int(tx.get('fee_lamports', 0))
            fee_sol = fee_lamports / LAMPORTS_TO_SOL

            # Convert to BTC
            sol_btc_ratio = self.price_converter.get_sol_btc_ratio(timestamp)
            value_btc = value_sol * sol_btc_ratio
            fee_btc = fee_sol * sol_btc_ratio

            slots.append(slot)

            # Track transaction types
            if tx_type == 'SOL_TRANSFER':
                sol_txs += 1
            elif tx_type == 'TOKEN_TRANSFER':
                token_txs += 1
                mint_address = tx.get('mint_address', '')
                if mint_address:
                    unique_tokens.add(mint_address)

            # Detect account creation (approximate)
            if fee_sol > 0.002:  # Typical account creation cost
                account_creation_costs += fee_sol

            # Classify transaction direction and add to appropriate lists
            if tx_from == address:  # Outgoing
                all_fees_btc.append(fee_btc)
                if value_btc > 0:
                    sent_txs.append({
                        'value_btc': value_btc,
                        'value_sol': value_sol,
                        'fee_btc': fee_btc,
                        'slot': slot,
                        'tx_type': tx_type,
                        'tx_context': tx_context,
                        'is_programmatic': is_programmatic
                    })
                    all_values_btc.append(value_btc)
                    
                    # Add counterparty (filter out known programs)
                    if tx_to and tx_to not in COMPREHENSIVE_PROGRAM_ADDRESSES:
                        counterparties[tx_to] += 1

            if tx_to == address:  # Incoming
                if value_btc > 0:
                    received_txs.append({
                        'value_btc': value_btc,
                        'value_sol': value_sol,
                        'slot': slot,
                        'tx_type': tx_type,
                        'tx_context': tx_context,
                        'is_programmatic': is_programmatic
                    })
                    all_values_btc.append(value_btc)
                    
                    # Add counterparty (filter out known programs)
                    if tx_from and tx_from not in COMPREHENSIVE_PROGRAM_ADDRESSES:
                        counterparties[tx_from] += 1

        # Calculate comprehensive features
        return self._aggregate_enhanced_features(
            transactions, slots, sent_txs, received_txs, all_values_btc, all_fees_btc,
            counterparties, failed_txs, sol_txs, token_txs, len(unique_tokens),
            dex_txs, lending_txs, staking_txs, programmatic_txs, price_failures,
            context_counts, account_creation_costs
        )

    def _aggregate_enhanced_features(self, transactions, slots, sent_txs, received_txs, 
                                   all_values_btc, all_fees_btc, counterparties, failed_txs, 
                                   sol_txs, token_txs, unique_tokens, dex_txs, lending_txs, 
                                   staking_txs, programmatic_txs, price_failures, context_counts,
                                   account_creation_costs):
        """Aggregate enhanced features including data quality metrics."""
        features = {}
        total_txs = len(transactions)
        
        # Basic transaction counts
        features['num_txs_as_sender'] = float(len(sent_txs))
        features['num_txs_as_receiver'] = float(len(received_txs))
        features['total_txs'] = float(total_txs)
        
        # Enhanced Solana-specific features
        features['failed_txs'] = float(failed_txs)
        features['success_rate'] = float((total_txs - failed_txs) / total_txs) if total_txs > 0 else 0.0
        features['sol_txs'] = float(sol_txs)
        features['token_txs'] = float(token_txs)
        features['unique_tokens_transacted'] = float(unique_tokens)
        features['sol_to_token_ratio'] = float(sol_txs / token_txs) if token_txs > 0 else float('inf')
        
        # DeFi activity features
        features['dex_swap_txs'] = float(dex_txs)
        features['lending_txs'] = float(lending_txs)
        features['staking_txs'] = float(staking_txs)
        features['programmatic_txs'] = float(programmatic_txs)
        features['programmatic_ratio'] = float(programmatic_txs / total_txs) if total_txs > 0 else 0.0
        
        # DeFi ratios
        defi_txs = dex_txs + lending_txs + staking_txs
        features['defi_txs_total'] = float(defi_txs)
        features['defi_ratio'] = float(defi_txs / total_txs) if total_txs > 0 else 0.0
        features['dex_to_total_ratio'] = float(dex_txs / total_txs) if total_txs > 0 else 0.0
        
        # Data quality indicators
        features['price_fetch_failures'] = float(price_failures)
        features['price_fetch_success_rate'] = float((total_txs - price_failures) / total_txs) if total_txs > 0 else 0.0
        features['account_creation_costs_sol'] = float(account_creation_costs)
        
        # Context diversity
        features['transaction_context_diversity'] = float(len(context_counts))
        features['most_common_context_ratio'] = float(max(context_counts.values()) / total_txs) if context_counts and total_txs > 0 else 0.0
        
        # Slot-based features (Solana equivalent of block-based features)
        if slots:
            slots = [s for s in slots if s > 0]
            features['first_slot_appeared_in'] = float(min(slots)) if slots else 0.0
            features['last_slot_appeared_in'] = float(max(slots)) if slots else 0.0
            features['lifetime_in_slots'] = features['last_slot_appeared_in'] - features['first_slot_appeared_in']
            features['num_timesteps_appeared_in'] = float(len(set(slots)))
            
            # Slot density (indicator of bot behavior)
            if features['lifetime_in_slots'] > 0:
                features['slot_density'] = float(total_txs / features['lifetime_in_slots'])
            else:
                features['slot_density'] = 0.0
        else:
            features.update({
                'first_slot_appeared_in': 0.0,
                'last_slot_appeared_in': 0.0,
                'lifetime_in_slots': 0.0,
                'num_timesteps_appeared_in': 0.0,
                'slot_density': 0.0
            })

        # First transaction slots by direction
        sent_slots = sorted([tx['slot'] for tx in sent_txs if tx['slot'] > 0])
        received_slots = sorted([tx['slot'] for tx in received_txs if tx['slot'] > 0])
        features['first_sent_slot'] = float(sent_slots[0]) if sent_slots else 0.0
        features['first_received_slot'] = float(received_slots[0]) if received_slots else 0.0

        # Enhanced value statistics
        self._add_stats(features, 'btc_transacted', all_values_btc)
        self._add_stats(features, 'btc_sent', [tx['value_btc'] for tx in sent_txs])
        self._add_stats(features, 'btc_received', [tx['value_btc'] for tx in received_txs])
        self._add_stats(features, 'fees', all_fees_btc)
        
        # SOL-denominated statistics for comparison
        self._add_stats(features, 'sol_sent', [tx['value_sol'] for tx in sent_txs])
        self._add_stats(features, 'sol_received', [tx['value_sol'] for tx in received_txs])

        # Fee analysis
        fee_shares = []
        for tx in sent_txs:
            if tx['value_btc'] > 0:
                fee_share = (tx['fee_btc'] / tx['value_btc']) * 100
                fee_shares.append(fee_share)
        self._add_stats(features, 'fees_as_share', fee_shares)

        # Enhanced interval statistics
        self._add_interval_stats(features, 'slots_btwn_txs', sorted(set(slots)))
        self._add_interval_stats(features, 'slots_btwn_input_txs', sent_slots)
        self._add_interval_stats(features, 'slots_btwn_output_txs', received_slots)

        # Counterparty analysis with program filtering
        human_counterparties = {addr: count for addr, count in counterparties.items() 
                               if addr not in COMPREHENSIVE_PROGRAM_ADDRESSES}
        
        features['transacted_w_address_total'] = float(len(human_counterparties))
        features['transacted_w_programs_total'] = float(len(counterparties) - len(human_counterparties))
        features['num_addr_transacted_multiple'] = float(sum(1 for count in human_counterparties.values() if count > 1))
        
        if human_counterparties:
            self._add_stats(features, 'transacted_w_address', list(human_counterparties.values()), include_total=False)
        else:
            self._add_stats(features, 'transacted_w_address', [], include_total=False)
        
        # Behavioral pattern analysis
        features['avg_tx_complexity'] = self._calculate_tx_complexity(sent_txs + received_txs)
        features['burst_activity_score'] = self._calculate_burst_score(slots)
        features['round_number_ratio'] = self._calculate_round_number_ratio(all_values_btc)
        
        return features

    def _calculate_tx_complexity(self, all_txs: List[Dict]) -> float:
        """Calculate average transaction complexity based on context."""
        if not all_txs:
            return 0.0
        
        complexity_scores = {
            'PURE_TRANSFER': 1.0,
            'DEX_SWAP': 3.0,
            'LENDING': 2.5,
            'STAKING': 2.0,
            'OTHER_PROGRAM': 2.0,
            'UNKNOWN': 1.5
        }
        
        total_complexity = sum(complexity_scores.get(tx.get('tx_context', 'UNKNOWN'), 1.5) for tx in all_txs)
        return float(total_complexity / len(all_txs))

    def _calculate_burst_score(self, slots: List[int]) -> float:
        """Calculate burst activity score (indicator of bot behavior)."""
        if len(slots) < 3:
            return 0.0
        
        sorted_slots = sorted(set(slots))
        intervals = [sorted_slots[i] - sorted_slots[i-1] for i in range(1, len(sorted_slots))]
        
        if not intervals:
            return 0.0
        
        # Count very short intervals (< 10 slots apart, roughly < 5 seconds)
        short_intervals = sum(1 for interval in intervals if interval < 10)
        return float(short_intervals / len(intervals))

    def _calculate_round_number_ratio(self, values: List[float]) -> float:
        """Calculate ratio of round number transactions (potential bot indicator)."""
        if not values:
            return 0.0
        
        # Check for suspiciously round numbers
        round_numbers = 0
        for value in values:
            if value > 0:
                # Convert to string and check for patterns like 1.0, 0.1, 10.0, etc.
                str_val = f"{value:.8f}".rstrip('0').rstrip('.')
                if '.' not in str_val or len(str_val.split('.')[1]) <= 2:
                    round_numbers += 1
        
        return float(round_numbers / len(values))

    def _add_stats(self, features: Dict, prefix: str, values: List[float], include_total: bool = True):
        """Add statistical aggregations for a list of values."""
        if values:
            if include_total:
                features[f'{prefix}_total'] = float(sum(values))
            features[f'{prefix}_min'] = float(min(values))
            features[f'{prefix}_max'] = float(max(values))
            features[f'{prefix}_mean'] = float(np.mean(values))
            features[f'{prefix}_median'] = float(np.median(values))
            features[f'{prefix}_std'] = float(np.std(values))
        else:
            if include_total:
                features[f'{prefix}_total'] = 0.0
            features.update({
                f'{prefix}_min': 0.0,
                f'{prefix}_max': 0.0,
                f'{prefix}_mean': 0.0,
                f'{prefix}_median': 0.0,
                f'{prefix}_std': 0.0
            })

    def _add_interval_stats(self, features: Dict, prefix: str, sorted_slots: List[int]):
        """Calculate statistics for intervals between slots."""
        if len(sorted_slots) > 1:
            intervals = [float(sorted_slots[i] - sorted_slots[i-1]) for i in range(1, len(sorted_slots))]
            self._add_stats(features, prefix, intervals)
        else:
            self._add_stats(features, prefix, [])


class SolanaProcessor:
    """Enhanced main orchestrator with better error handling and validation."""
    
    def __init__(self, api_key: str = HELIUS_API_KEY):
        self.extractor = SolanaDataExtractor(api_key)
        self.calculator = SolanaFeatureCalculator(self.extractor.price_converter)
    
    def process_from_csv(self, csv_file_path: str, output_file: str = 'solana_features_output.csv'):
        """Process addresses from CSV with enhanced validation and quality checks."""
        try:
            df_source = pd.read_csv(csv_file_path)
            
            # Validate CSV structure
            required_columns = ['Address', 'FLAG']
            missing_columns = [col for col in required_columns if col not in df_source.columns]
            if missing_columns:
                print(f"❌ Missing required columns: {missing_columns}")
                return
            
            addresses_to_process = [(row['Address'], row['FLAG']) for _, row in df_source.iterrows()]
            print(f"📖 Found {len(addresses_to_process)} total addresses in source file.")
            
        except Exception as e:
            print(f"❌ Error reading source CSV '{csv_file_path}': {e}")
            return

        all_results, processed_addresses = self._load_previous_results(output_file)
        
        processed_in_session = 0
        quality_issues = {'low_tx_count': 0, 'high_price_failures': 0, 'suspicious_patterns': 0}
        
        for i, (address, flag) in enumerate(addresses_to_process, 1):
            if address in processed_addresses:
                continue

            print(f"\n--- Processing {i}/{len(addresses_to_process)}: {address} ---")
            
            try:
                # Validate address format
                if not self._validate_solana_address(address):
                    print(f"❌ Invalid Solana address format: {address}")
                    continue
                
                transactions = self.extractor.get_all_transactions(address)
                if not transactions:
                    print(f"No transactions found for {address}. Skipping.")
                    continue
                
                features = self.calculator.calculate_features(address, transactions)
                if features:
                    features['address'] = address
                    features['class'] = flag
                    
                    # Add data quality assessment
                    quality_assessment = self._assess_data_quality(features, transactions)
                    features.update(quality_assessment)
                    
                    all_results.append(features)
                    processed_in_session += 1
                    
                    # Track quality issues
                    if features.get('total_txs', 0) < 10:
                        quality_issues['low_tx_count'] += 1
                    if features.get('price_fetch_success_rate', 1) < 0.8:
                        quality_issues['high_price_failures'] += 1
                    if features.get('round_number_ratio', 0) > 0.5:
                        quality_issues['suspicious_patterns'] += 1
                    
                    print(f"✅ {address}: Features calculated successfully.")
                    self._print_feature_summary(features)
                
                # More frequent saves due to complexity
                if processed_in_session > 0 and processed_in_session % 3 == 0:
                    self._save_results(all_results, output_file)

            except Exception as e:
                print(f"❌ Unhandled error processing {address}: {e}")
                import traceback
                traceback.print_exc()
        
        # Final save and summary
        if processed_in_session > 0:
            print("\n✅ Processing complete. Performing final save...")
            self._save_results(all_results, output_file)
            self._print_session_summary(processed_in_session, len(all_results), quality_issues)
        else:
            print("\nNo new addresses were processed in this session.")

    def _validate_solana_address(self, address: str) -> bool:
        """Validate Solana address format."""
        if not address or len(address) < 32 or len(address) > 44:
            return False
        
        # Basic character set validation (Base58)
        valid_chars = set('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz')
        return all(c in valid_chars for c in address)

    def _assess_data_quality(self, features: Dict, transactions: List[Dict]) -> Dict:
        """Assess data quality and add quality indicators."""
        quality_metrics = {}
        
        # Transaction volume quality
        total_txs = features.get('total_txs', 0)
        if total_txs < 5:
            quality_metrics['data_quality_warning'] = 'LOW_TRANSACTION_COUNT'
        elif total_txs > 10000:
            quality_metrics['data_quality_warning'] = 'VERY_HIGH_TRANSACTION_COUNT'
        else:
            quality_metrics['data_quality_warning'] = 'NORMAL'
        
        # Price fetch quality
        price_success_rate = features.get('price_fetch_success_rate', 1.0)
        if price_success_rate < 0.5:
            quality_metrics['price_quality'] = 'POOR'
        elif price_success_rate < 0.8:
            quality_metrics['price_quality'] = 'MEDIUM'
        else:
            quality_metrics['price_quality'] = 'GOOD'
        
        # Behavioral consistency
        programmatic_ratio = features.get('programmatic_ratio', 0)
        defi_ratio = features.get('defi_ratio', 0)
        
        if programmatic_ratio > 0.9 and defi_ratio > 0.8:
            quality_metrics['behavior_pattern'] = 'LIKELY_BOT_OR_DEFI'
        elif programmatic_ratio < 0.1 and defi_ratio < 0.1:
            quality_metrics['behavior_pattern'] = 'LIKELY_HUMAN'
        else:
            quality_metrics['behavior_pattern'] = 'MIXED'
        
        return quality_metrics

    def _print_feature_summary(self, features: Dict):
        """Print a concise summary of calculated features."""
        print(f"   - Total transactions: {features.get('total_txs', 0)}")
        print(f"   - BTC transacted: {features.get('btc_transacted_total', 0):.8f}")
        print(f"   - Unique tokens: {features.get('unique_tokens_transacted', 0)}")
        print(f"   - DeFi ratio: {features.get('defi_ratio', 0):.2%}")
        print(f"   - Price success rate: {features.get('price_fetch_success_rate', 0):.2%}")

    def _print_session_summary(self, processed: int, total: int, quality_issues: Dict):
        """Print detailed session summary with quality metrics."""
        print(f"\n📊 Session Summary:")
        print(f"   - Processed: {processed} new addresses")
        print(f"   - Total in dataset: {total} addresses")
        print(f"\n⚠️  Data Quality Issues:")
        print(f"   - Low transaction count: {quality_issues['low_tx_count']} addresses")
        print(f"   - High price failures: {quality_issues['high_price_failures']} addresses")
        print(f"   - Suspicious patterns: {quality_issues['suspicious_patterns']} addresses")
        
        # Price converter summary
        price_failures = self.extractor.price_converter.price_validation_failures
        print(f"   - Total price validation failures: {price_failures}")

    def _load_previous_results(self, output_file: str) -> tuple[List[Dict], set]:
        """Load previously processed data with validation."""
        if os.path.exists(output_file):
            try:
                df_existing = pd.read_csv(output_file)
                if 'address' in df_existing.columns:
                    results = df_existing.to_dict('records')
                    processed = set(df_existing['address'])
                    print(f"✅ Resuming. Loaded {len(processed)} previously processed addresses from '{output_file}'.")
                    
                    # Validate existing data
                    if len(results) > 0:
                        sample_features = len([k for k in results[0].keys() if k not in ['address', 'class']])
                        print(f"   - Previous data has {sample_features} features per address")
                    
                    return results, processed
            except Exception as e:
                print(f"⚠️ Could not read existing output file '{output_file}'. Starting fresh. Error: {e}")
        return [], set()

    def _save_results(self, results: List[Dict], output_file: str):
        """Save results with enhanced data validation."""
        if not results:
            return
        
        df = pd.DataFrame(results)
        
        # Clean and standardize data types
        for col in df.columns:
            if col not in ['address', 'class', 'data_quality_warning', 'price_quality', 'behavior_pattern']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # Validate critical columns exist
        expected_columns = ['total_txs', 'btc_transacted_total', 'success_rate']
        missing_critical = [col for col in expected_columns if col not in df.columns]
        if missing_critical:
            print(f"⚠️ Warning: Missing critical columns: {missing_critical}")

        df.to_csv(output_file, index=False)
        self.extractor.price_converter.save_price_cache()
        print(f"💾 Progress saved. {len(df)} total addresses now in '{output_file}'.")

    def process_single_address(self, address: str, verbose: bool = True) -> Optional[Dict]:
        """Process a single address with enhanced validation and reporting."""
        if verbose:
            print(f"\n🔍 Testing single address: {address}")
        
        # Validate address
        if not self._validate_solana_address(address):
            if verbose:
                print("❌ Invalid Solana address format")
            return None
        
        try:
            transactions = self.extractor.get_all_transactions(address)
            if not transactions:
                if verbose:
                    print("No transactions found.")
                return None
            
            features = self.calculator.calculate_features(address, transactions)
            if features and verbose:
                print(f"✅ Successfully calculated {len(features)} features")
                self._print_feature_summary(features)
                
                # Additional quality reporting
                quality_assessment = self._assess_data_quality(features, transactions)
                print(f"   - Data quality: {quality_assessment.get('data_quality_warning', 'UNKNOWN')}")
                print(f"   - Price quality: {quality_assessment.get('price_quality', 'UNKNOWN')}")
                print(f"   - Behavior pattern: {quality_assessment.get('behavior_pattern', 'UNKNOWN')}")
            
            return features
            
        except Exception as e:
            if verbose:
                print(f"❌ Error processing address: {e}")
                import traceback
                traceback.print_exc()
            return None


# --- Enhanced Main Execution ---
if __name__ == "__main__":
    # Enhanced configuration check
    if HELIUS_API_KEY == "your_helius_api_key_here":
        print("❌ Please update your API keys in the configuration section before running!")
        print("Required API keys:")
        print("   - HELIUS_API_KEY (primary data source)")
        print("   - CRYPTOCOMPARE_API_KEY (price data)")
        print("   - MORALIS_API_KEY (backup data source)")
        exit(1)
    
    processor = SolanaProcessor()
    
    print("🚀 Enhanced Solana Blockchain Analyzer")
    print("=" * 60)
    print("🆕 New Features:")
    print("   - Enhanced decimal normalization")
    print("   - Comprehensive program filtering")
    print("   - Transaction context classification")
    print("   - Data quality validation")
    print("   - DeFi-specific features")
    print("   - Behavioral pattern analysis")
    print("=" * 60)
    
    # Test with a single address first
    test_address = input("Enter a Solana address to test (or press Enter to skip): ").strip()
    if test_address:
        print(f"\n🧪 Testing with address: {test_address}")
        features = processor.process_single_address(test_address)
        if features:
            print(f"\n📋 Key features calculated:")
            key_features = [
                'total_txs', 'btc_transacted_total', 'unique_tokens_transacted',
                'defi_ratio', 'programmatic_ratio', 'success_rate'
            ]
            for key in key_features:
                if key in features:
                    print(f"   {key}: {features[key]}")
            output_filename = f"{test_address}_features.json"
            with open(output_filename, 'w') as f:
                json.dump(features, f, indent=4)
            print(f"✅ Features for {test_address} saved to {output_filename}")
        
        proceed = input("\nProceed with CSV processing? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Exiting...")
            exit(0)
    
    # CSV processing
    csv_path = r"D:\AAL\Coding\piton\BlockchainAnalyzer\awholenewworld\labeled_seed_dataset.csv"
    
    if not csv_path:
        csv_path = "solana_addresses.csv"  # Default filename
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        print("Please ensure your CSV has columns: 'Address' and 'FLAG'")
        
        # Create enhanced sample CSV
        sample_data = {
            'Address': [
                '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  # Orca program
                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC token
                'So11111111111111111111111111111111111111112',   # Wrapped SOL
                'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'    # Jupiter program
            ],
            'FLAG': [0, 0, 0, 0]  # All legitimate programs/tokens
        }
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_csv('sample_solana_addresses.csv', index=False)
        print("📝 Created enhanced sample file: sample_solana_addresses.csv")
    else:
        print(f"📂 Processing file: {csv_path}")
        processor.process_from_csv(csv_path)