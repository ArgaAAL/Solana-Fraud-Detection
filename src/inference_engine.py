import json
import sys
import numpy as np
import joblib
import onnxruntime as ort
from sklearn.metrics import classification_report
import pandas as pd

class SolanaRansomwareModelTester:
    def __init__(self):
        """Initialize the tester by loading all model artifacts"""
        self.model_artifacts = {}
        self.load_artifacts()
    
    def load_artifacts(self):
        """Load all the saved model artifacts"""
        print("🔧 Loading Model Artifacts...")
        
        try:
            # Load metadata
            with open('sol_model_metadata.json', 'r') as f:
                self.model_artifacts['metadata'] = json.load(f)
            print("   ✅ Metadata loaded")
            
            # Load scaler parameters
            with open('sol_scaler_parameters.json', 'r') as f:
                self.model_artifacts['scaler_params'] = json.load(f)
            print("   ✅ Scaler parameters loaded")
            
            # Load test sample
            with open('sol_test_sample.json', 'r') as f:
                self.model_artifacts['test_sample'] = json.load(f)
            print("   ✅ Test sample loaded")
            
            # Load Python model (for comparison)
            self.model_artifacts['python_model'] = joblib.load('sol_python_mlp_model_and_scaler.joblib')
            print("   ✅ Python model loaded")
            
            # Load ONNX model
            self.model_artifacts['onnx_session'] = ort.InferenceSession('sol_ransomware_model_mlp.onnx')
            print("   ✅ ONNX model loaded")
            
        except FileNotFoundError as e:
            print(f"   ❌ Missing file: {e}")
            raise
        except Exception as e:
            print(f"   ❌ Error loading artifacts: {e}")
            raise
    
    def manual_scaling(self, features_dict):
        """Manually apply scaling using saved parameters (simulates Rust canister)"""
        scaler_params = self.model_artifacts['scaler_params']
        feature_names = self.model_artifacts['metadata']['feature_names']
        
        # Convert dict to array in the correct order
        feature_array = np.array([features_dict.get(name, 0.0) for name in feature_names], dtype=np.float32)
        
        # Apply scaling: (x - mean) / scale
        mean = np.array(scaler_params['mean'], dtype=np.float32)
        scale = np.array(scaler_params['scale'], dtype=np.float32)
        
        scaled_features = (feature_array - mean) / scale
        return scaled_features
    
    def test_onnx_inference(self, features_dict):
        """Test ONNX model inference"""
        print("\n🧪 Testing ONNX Model Inference...")
        
        # Manual scaling (as would happen in Rust canister)
        scaled_features = self.manual_scaling(features_dict)
        
        # Prepare input for ONNX (needs to be 2D array)
        onnx_input = scaled_features.reshape(1, -1).astype(np.float32)
        
        # Get input name from ONNX model
        input_name = self.model_artifacts['onnx_session'].get_inputs()[0].name
        
        # Run ONNX inference
        onnx_result = self.model_artifacts['onnx_session'].run(None, {input_name: onnx_input})
        
        # Extract probability and prediction
        probabilities = onnx_result[1][0]  # Probabilities array
        prediction = onnx_result[0][0]     # Prediction
        malicious_probability = float(probabilities[1])  # Probability of class 1 (malicious)
        
        print(f"   📊 ONNX Raw Prediction: {prediction}")
        print(f"   📊 ONNX Probabilities: [Benign: {probabilities[0]:.4f}, Malicious: {probabilities[1]:.4f}]")
        print(f"   📊 Malicious Probability: {malicious_probability:.4f}")
        
        return malicious_probability, prediction
    
    def test_python_model(self, features_dict):
        """Test Python model for comparison"""
        print("\n🐍 Testing Python Model (for comparison)...")
        
        python_model = self.model_artifacts['python_model']['model']
        python_scaler = self.model_artifacts['python_model']['scaler']
        feature_names = self.model_artifacts['metadata']['feature_names']
        
        # Convert dict to DataFrame
        feature_array = np.array([features_dict.get(name, 0.0) for name in feature_names]).reshape(1, -1)
        
        # Scale using sklearn scaler
        scaled_features = python_scaler.transform(feature_array)
        
        # Predict
        probabilities = python_model.predict_proba(scaled_features)[0]
        prediction = python_model.predict(scaled_features)[0]
        malicious_probability = float(probabilities[1])
        
        print(f"   📊 Python Raw Prediction: {prediction}")
        print(f"   📊 Python Probabilities: [Benign: {probabilities[0]:.4f}, Malicious: {probabilities[1]:.4f}]")
        print(f"   📊 Malicious Probability: {malicious_probability:.4f}")
        
        return malicious_probability, prediction
    
    def apply_threshold_classification(self, malicious_probability):
        """Apply the deployment threshold to classify"""
        threshold = self.model_artifacts['metadata']['deployment_threshold']
        is_malicious = malicious_probability >= threshold
        confidence = malicious_probability if is_malicious else (1 - malicious_probability)
        
        print(f"\n🎯 Threshold Classification:")
        print(f"   📏 Deployment Threshold: {threshold:.4f}")
        print(f"   🔍 Classification: {'🚨 MALICIOUS' if is_malicious else '✅ BENIGN'}")
        print(f"   📈 Confidence: {confidence:.4f}")
        
        return is_malicious, confidence
    
    def run_comprehensive_test(self):
        """Run comprehensive test of the entire pipeline"""
        print("🚀 RUNNING COMPREHENSIVE SOLANA RANSOMWARE MODEL TEST")
        print("=" * 70)
        
        # Display model info
        metadata = self.model_artifacts['metadata']
        print(f"\n📋 Model Information:")
        print(f"   🏷️  Model Version: {metadata['model_version']}")
        print(f"   🧠  Model Type: {metadata['model_type']}")
        print(f"   ⛓️  Blockchain: {metadata['blockchain']}")
        print(f"   📊  Features: {metadata['num_features']}")
        print(f"   🎯  AUC Score: {metadata['auc_score']:.4f}")
        print(f"   🏆  Best F1 Score: {metadata['best_f1_score']:.4f}")
        
        # Test with the provided sample
        test_sample = self.model_artifacts['test_sample']
        print(f"\n🧪 Testing with Sample Transaction:")
        print(f"   📦 Features in sample: {len(test_sample)}")
        
        # Test ONNX model
        onnx_prob, onnx_pred = self.test_onnx_inference(test_sample)
        
        # Test Python model
        python_prob, python_pred = self.test_python_model(test_sample)
        
        # Compare results
        print(f"\n🔍 Model Comparison:")
        prob_diff = abs(onnx_prob - python_prob)
        print(f"   📊 ONNX Probability: {onnx_prob:.6f}")
        print(f"   📊 Python Probability: {python_prob:.6f}")
        print(f"   📏 Probability Difference: {prob_diff:.6f}")
        
        if prob_diff < 0.001:
            print("   ✅ Models are consistent! (difference < 0.001)")
        else:
            print("   ⚠️ Models show some difference (might be due to precision)")
        
        # Apply threshold classification
        is_malicious, confidence = self.apply_threshold_classification(onnx_prob)
        
        return {
            'onnx_probability': onnx_prob,
            'python_probability': python_prob,
            'is_malicious': is_malicious,
            'confidence': confidence,
            'models_consistent': prob_diff < 0.001
        }
    
    def test_custom_sample(self, custom_features):
        """Test with custom feature values"""
        print("\n🔧 Testing Custom Sample...")
        
        # Ensure all required features are present
        feature_names = self.model_artifacts['metadata']['feature_names']
        complete_sample = {}
        
        for feature in feature_names:
            complete_sample[feature] = custom_features.get(feature, 0.0)
        
        # Test with ONNX
        prob, pred = self.test_onnx_inference(complete_sample)
        is_malicious, confidence = self.apply_threshold_classification(prob)
        
        return {
            'probability': prob,
            'is_malicious': is_malicious,
            'confidence': confidence
        }
    
    def create_test_scenarios(self):
        """Create various test scenarios"""
        print("\n🎭 Creating Test Scenarios...")
        
        base_sample = self.model_artifacts['test_sample'].copy()
        scenarios = []
        
        # Scenario 1: High activity, many transactions
        scenario_1 = base_sample.copy()
        scenario_1.update({
            'total_txs': 1000.0,
            'activity_density': 0.1,
            'burst_activity_score': 0.8,
            'mixing_intensity': 0.9
        })
        scenarios.append(("High Activity Suspicious", scenario_1))
        
        # Scenario 2: Normal looking transaction
        scenario_2 = base_sample.copy()
        scenario_2.update({
            'total_txs': 10.0,
            'activity_density': 0.001,
            'burst_activity_score': 0.1,
            'mixing_intensity': 0.1
        })
        scenarios.append(("Normal Transaction", scenario_2))
        
        # Scenario 3: High value, low frequency
        scenario_3 = base_sample.copy()
        scenario_3.update({
            'sol_sent_total': 10000.0,
            'value_per_transaction': 1000.0,
            'total_txs': 10.0
        })
        scenarios.append(("High Value Low Frequency", scenario_3))
        
        # Test all scenarios
        for name, scenario in scenarios:
            print(f"\n   🎯 Testing: {name}")
            result = self.test_custom_sample(scenario)
            status = "🚨 MALICIOUS" if result['is_malicious'] else "✅ BENIGN"
            print(f"      Result: {status} (confidence: {result['confidence']:.3f})")

def main():
    """Main testing function"""
    try:
        tester = SolanaRansomwareModelTester()
        
        if len(sys.argv) > 1:
            custom_feature_file = sys.argv[1]
            print(f"\n--- TESTING CUSTOM FILE: {custom_feature_file} ---")
            with open(custom_feature_file, 'r') as f:
                custom_features = json.load(f)
            tester.test_custom_sample(custom_features)
        else:
            # Run comprehensive test
            results = tester.run_comprehensive_test()
            
            # Create and test scenarios
            tester.create_test_scenarios()
            
            # Final summary
            print("\n" + "=" * 70)
            print("✅ TESTING COMPLETE")
            print("=" * 70)
            
            if results['models_consistent']:
                print("🎉 SUCCESS: ONNX and Python models are consistent!")
                print("🚀 Your model is ready for deployment to the Rust canister!")
            else:
                print("⚠️ WARNING: Small differences detected between models")
                print("   This might be due to floating-point precision differences")
            
            print(f"\n📊 Final Results:")
            print(f"   🎯 Test Sample Classification: {'🚨 MALICIOUS' if results['is_malicious'] else '✅ BENIGN'}")
            print(f"   📈 Confidence: {results['confidence']:.4f}")
            print(f"   🔄 Model Consistency: {'✅' if results['models_consistent'] else '⚠️'}")
        
    except Exception as e:
        print(f"\n❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()