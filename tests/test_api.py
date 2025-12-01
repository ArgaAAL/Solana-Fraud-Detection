#!/usr/bin/env python3
"""
Helius API Test Script
======================
Test if your Helius API key is working properly.
"""

import requests
import json

def test_helius_api():
    api_key = "2924b86e-5cf9-4952-b335-fb4efea7eb6d"
    
    print("🔧 Testing Helius API Connection")
    print("=" * 40)
    
    # Test different endpoint formats
    endpoints = [
        f"https://mainnet.helius-rpc.com/?api-key={api_key}",
        f"https://api.helius.xyz/v0/rpc?api-key={api_key}",
        f"https://rpc.helius.xyz/?api-key={api_key}",
    ]
    
    for i, endpoint in enumerate(endpoints, 1):
        print(f"\n{i}️⃣ Testing endpoint: {endpoint[:50]}...")
        
        # Test with getSlot method
        try:
            response = requests.post(
                endpoint,
                json={"jsonrpc": "2.0", "id": 1, "method": "getSlot"},
                timeout=10,
                headers={"Content-Type": "application/json"}
            )
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if 'result' in result:
                    current_slot = result['result']
                    print(f"   ✅ SUCCESS! Current slot: {current_slot}")
                    return current_slot, endpoint
                else:
                    print(f"   ❌ No 'result': {result}")
            else:
                print(f"   ❌ Error response: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Request failed: {e}")
    
    print("\n❌ All endpoints failed!")
    return None, None
    
    # Test 3: Get a specific account (USDC) with working endpoint
    if endpoint:
        print(f"\n3️⃣ Testing getAccountInfo with working endpoint...")
        try:
            usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            response = requests.post(
                endpoint,
                json={
                    "jsonrpc": "2.0", 
                    "id": 1, 
                    "method": "getAccountInfo",
                    "params": [usdc_mint, {"encoding": "base64"}]
                },
                timeout=10,
                headers={"Content-Type": "application/json"}
            )
            print(f"   Status: {response.status_code}")
            result = response.json()
            
            if 'result' in result and result['result']:
                print(f"   ✅ Account info retrieved for USDC")
                return current_slot, endpoint
            else:
                print(f"   ❌ Failed to get account info")
                print(f"   Response: {json.dumps(result, indent=2)}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return current_slot, endpoint

def test_simple_addresses():
    """Test getting some simple, well-known addresses."""
    
    print("\n🎯 Testing Simple Address Collection")
    print("=" * 40)
    
    # Well-known Solana addresses
    known_addresses = [
        ("USDC Token", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
        ("USDT Token", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
        ("Wrapped SOL", "So11111111111111111111111111111111111111112"),
        ("Jupiter", "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"),
        ("Orca", "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"),
    ]
    
    print("\n📋 Using these well-known addresses for testing:")
    for name, address in known_addresses:
        print(f"   {name}: {address}")
    
    # Create a simple CSV
    import pandas as pd
    
    df = pd.DataFrame({
        'Address': [addr for _, addr in known_addresses],
        'FLAG': [-1] * len(known_addresses)  # Unlabeled
    })
    
    filename = 'test_addresses.csv'
    df.to_csv(filename, index=False)
    print(f"\n💾 Saved {len(known_addresses)} test addresses to '{filename}'")
    print(f"   You can use this file to test your feature extraction!")
    
    return filename

if __name__ == "__main__":
    print("🚀 Helius API & Address Collection Test")
    print("This will test if your API is working and create a simple test dataset.")
    
    # Test API
    current_slot, working_endpoint = test_helius_api()
    
    if working_endpoint:
        print(f"\n✅ Found working endpoint: {working_endpoint}")
        print(f"💡 Use this endpoint in your main script!")
    else:
        print(f"\n❌ No working endpoints found. Check your API key.")
    
    # Create test addresses
    test_file = test_simple_addresses()
    
    print(f"\n✨ Test Complete!")
    print(f"📁 Test file created: {test_file}")
    print(f"🔍 Try running your feature extraction on this test file first!")
    
    if working_endpoint:
        print(f"\n🔧 NEXT STEPS:")
        print(f"1. Update your main script to use: {working_endpoint}")
        print(f"2. Run feature extraction on {test_file}")
        print(f"3. Then try the full workflow again")