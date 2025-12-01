#!/usr/bin/env python3
"""
Process addresses in batches to handle large datasets
"""

import pandas as pd
import subprocess
import sys
import os
import time

def process_addresses_in_batches(input_file, batch_size=50, max_batches=5):
    """
    Process addresses in smaller batches to avoid timeouts.
    
    Args:
        input_file: CSV file with addresses
        batch_size: Number of addresses per batch
        max_batches: Maximum number of batches to process
    """
    
    print(f"🔄 BATCH PROCESSING SETUP")
    print(f"   Input file: {input_file}")
    print(f"   Batch size: {batch_size}")
    print(f"   Max batches: {max_batches}")
    print("="*50)
    
    # Load addresses
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return False
    
    df = pd.read_csv(input_file)
    total_addresses = len(df)
    print(f"📊 Total addresses to process: {total_addresses}")
    
    # Process in batches
    all_features = []
    processed_count = 0
    
    for batch_num in range(min(max_batches, (total_addresses // batch_size) + 1)):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_addresses)
        
        if start_idx >= total_addresses:
            break
            
        print(f"\n🔄 BATCH {batch_num + 1}: Processing addresses {start_idx + 1}-{end_idx}")
        
        # Create batch file
        batch_df = df.iloc[start_idx:end_idx].copy()
        batch_file = f"batch_{batch_num + 1}_addresses.csv"
        batch_df.to_csv(batch_file, index=False)
        
        print(f"   📁 Created batch file: {batch_file}")
        print(f"   📋 Addresses in batch: {len(batch_df)}")
        
        # Process batch
        try:
            feature_script = find_feature_script()
            if not feature_script:
                print(f"   ❌ Feature extraction script not found")
                break
            
            print(f"   🔧 Processing batch with feature extractor...")
            cmd = [sys.executable, feature_script, batch_file]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=600  # 10 minute timeout per batch
            )
            
            if result.returncode == 0:
                # Check if output file was created
                output_file = "solana_features_output.csv"
                if os.path.exists(output_file):
                    batch_features = pd.read_csv(output_file)
                    all_features.append(batch_features)
                    processed_count += len(batch_features)
                    print(f"   ✅ Batch completed: {len(batch_features)} addresses processed")
                    
                    # Rename output to avoid overwriting
                    backup_name = f"batch_{batch_num + 1}_features.csv"
                    os.rename(output_file, backup_name)
                    print(f"   📁 Saved as: {backup_name}")
                else:
                    print(f"   ❌ No output file generated for batch {batch_num + 1}")
            else:
                print(f"   ❌ Batch {batch_num + 1} failed:")
                print(f"   Error: {result.stderr}")
                if result.stdout:
                    print(f"   Output: {result.stdout}")
        
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Batch {batch_num + 1} timed out (>10 minutes)")
        except Exception as e:
            print(f"   💥 Error processing batch {batch_num + 1}: {e}")
        
        # Clean up batch file
        if os.path.exists(batch_file):
            os.remove(batch_file)
        
        # Rate limiting between batches
        if batch_num < max_batches - 1:
            print(f"   ⏳ Waiting 30 seconds before next batch...")
            time.sleep(30)
    
    # Combine all features
    if all_features:
        print(f"\n🔗 COMBINING RESULTS")
        combined_features = pd.concat(all_features, ignore_index=True)
        combined_features.to_csv("solana_features_output.csv", index=False)
        
        print(f"✅ BATCH PROCESSING COMPLETE!")
        print(f"   📊 Total addresses processed: {processed_count}/{total_addresses}")
        print(f"   📈 Processing rate: {processed_count/total_addresses*100:.1f}%")
        print(f"   📁 Combined output: solana_features_output.csv")
        return True
    else:
        print(f"❌ No features extracted from any batch")
        return False

def find_feature_script():
    """Find the feature extraction script."""
    possible_paths = [
        'solana_feature_extractor.py',
        'awholenewworld/patterns/solana_feature_extractor.py',
        '../patterns/solana_feature_extractor.py',
        '../../patterns/solana_feature_extractor.py',
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return None

def quick_test_processing():
    """Test with just a few addresses first."""
    print(f"🧪 QUICK TEST: Processing first 10 addresses")
    
    # Load original addresses
    if os.path.exists('collected_addresses.csv'):
        df = pd.read_csv('collected_addresses.csv')
        
        # Take first 10 addresses
        test_df = df.head(10).copy()
        test_df.to_csv('test_10_addresses.csv', index=False)
        
        print(f"📁 Created test file: test_10_addresses.csv")
        print(f"📋 Test addresses:")
        for i, addr in enumerate(test_df['Address']):
            print(f"   {i+1}. {addr}")
        
        # Process test batch
        success = process_addresses_in_batches('test_10_addresses.csv', batch_size=10, max_batches=1)
        
        if success:
            print(f"\n✅ Test successful! Ready to process larger batches.")
            return True
        else:
            print(f"\n❌ Test failed. Check feature extraction setup.")
            return False
    else:
        print(f"❌ collected_addresses.csv not found")
        return False

if __name__ == "__main__":
    print("🚀 SOLANA BATCH PROCESSOR")
    print("="*50)
    
    choice = input("Choose option:\n1. Quick test (10 addresses)\n2. Full batch processing (250 addresses)\n3. Custom batch size\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        quick_test_processing()
    elif choice == "2":
        process_addresses_in_batches('collected_addresses.csv', batch_size=50, max_batches=5)
    elif choice == "3":
        batch_size = int(input("Batch size: "))
        max_batches = int(input("Max batches: "))
        process_addresses_in_batches('collected_addresses.csv', batch_size=batch_size, max_batches=max_batches)
    else:
        print("Invalid choice")