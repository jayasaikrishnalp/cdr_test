#!/usr/bin/env python3
"""
Test script for Phase 1 CDR Intelligence Agent implementations
Tests new patterns: circular loops, one-ring signals, silent periods, 
synchronized calling, and location analysis
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from agent.cdr_agent import CDRIntelligenceAgent
from loguru import logger
import time

def test_phase1_patterns():
    """Test all Phase 1 pattern detection implementations"""
    
    print("=" * 80)
    print("CDR INTELLIGENCE AGENT - PHASE 1 PATTERN TESTING")
    print("=" * 80)
    
    try:
        # Initialize agent
        print("\n[1/6] Initializing CDR Intelligence Agent...")
        agent = CDRIntelligenceAgent()
        print("✅ Agent initialized successfully")
        
        # Load CDR data
        print("\n[2/6] Loading CDR data...")
        result = agent.load_cdr_data()
        
        if result['status'] == 'success':
            print(f"✅ Loaded {result['files_loaded']} CDR files")
            print(f"   Suspects: {', '.join(result['suspects'][:5])}")
        else:
            print(f"❌ Failed to load data: {result.get('message')}")
            return
        
        # Test queries for each new pattern
        test_queries = [
            {
                'name': 'Circular Communication Loops',
                'query': 'Detect circular communication loops between suspects',
                'expected': ['CIRCULAR_COMMUNICATION', 'loop', '→']
            },
            {
                'name': 'One-Ring Signaling Patterns',
                'query': 'Find one-ring patterns and signaling behavior in all suspects',
                'expected': ['ONE-RING', 'SIGNALING', 'one-ring calls']
            },
            {
                'name': 'Silent Period Detection',
                'query': 'Analyze silent periods and communication gaps for all suspects',
                'expected': ['SILENT PERIOD', 'gap', 'hours']
            },
            {
                'name': 'Synchronized Calling',
                'query': 'Check for synchronized calling patterns between suspects',
                'expected': ['SYNCHRONIZED', 'COORDINATED', 'simultaneously']
            },
            {
                'name': 'Location Analysis',
                'query': 'Analyze location patterns including impossible travel and clustering',
                'expected': ['LOCATION', 'travel', 'cluster', 'km']
            },
            {
                'name': 'Comprehensive Analysis',
                'query': 'Generate comprehensive report with all new pattern detections',
                'expected': ['CIRCULAR', 'ONE-RING', 'SILENT', 'SYNCHRONIZED', 'LOCATION']
            }
        ]
        
        # Run tests
        print("\n[3/6] Testing new pattern detection capabilities...")
        print("-" * 60)
        
        test_results = []
        
        for i, test in enumerate(test_queries, 1):
            print(f"\nTest {i}: {test['name']}")
            print(f"Query: {test['query']}")
            
            start_time = time.time()
            
            try:
                result = agent.analyze(test['query'])
                elapsed = time.time() - start_time
                
                # Check if expected patterns are in result
                found_patterns = []
                missing_patterns = []
                
                for pattern in test['expected']:
                    if pattern.lower() in result.lower():
                        found_patterns.append(pattern)
                    else:
                        missing_patterns.append(pattern)
                
                success = len(missing_patterns) == 0
                
                test_results.append({
                    'test': test['name'],
                    'success': success,
                    'elapsed': elapsed,
                    'found': found_patterns,
                    'missing': missing_patterns
                })
                
                if success:
                    print(f"✅ Success! Found all expected patterns")
                    print(f"   Time: {elapsed:.2f}s")
                else:
                    print(f"⚠️  Partial success")
                    print(f"   Found: {found_patterns}")
                    print(f"   Missing: {missing_patterns}")
                
                # Print sample of result
                print("\nSample output:")
                lines = result.split('\n')[:10]
                for line in lines:
                    print(f"   {line}")
                if len(result.split('\n')) > 10:
                    print("   ...")
                    
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                test_results.append({
                    'test': test['name'],
                    'success': False,
                    'error': str(e)
                })
        
        # Summary
        print("\n" + "=" * 60)
        print("[4/6] TEST SUMMARY")
        print("=" * 60)
        
        successful = sum(1 for r in test_results if r.get('success', False))
        total = len(test_results)
        
        print(f"\nTotal tests: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {total - successful}")
        print(f"Success rate: {(successful/total)*100:.1f}%")
        
        print("\nDetailed Results:")
        for result in test_results:
            status = "✅" if result.get('success') else "❌"
            print(f"\n{status} {result['test']}")
            if result.get('success'):
                print(f"   Time: {result['elapsed']:.2f}s")
                print(f"   Patterns found: {', '.join(result['found'])}")
            elif 'error' in result:
                print(f"   Error: {result['error']}")
            else:
                print(f"   Missing patterns: {', '.join(result['missing'])}")
        
        # Performance test
        print("\n[5/6] Performance Testing...")
        print("-" * 60)
        
        perf_query = "Analyze all patterns: device switching, temporal patterns, communication analysis, network connections, risk scores, location patterns"
        
        print(f"Running comprehensive analysis...")
        start_time = time.time()
        result = agent.analyze(perf_query)
        elapsed = time.time() - start_time
        
        print(f"✅ Analysis completed in {elapsed:.2f} seconds")
        print(f"   Result length: {len(result)} characters")
        print(f"   Lines: {len(result.split(chr(10)))}")
        
        # Feature verification
        print("\n[6/6] Feature Verification")
        print("-" * 60)
        
        features = {
            'Circular Loops': 'CIRCULAR' in result or '→' in result,
            'One-Ring Signals': 'ONE-RING' in result or 'SIGNALING' in result,
            'Silent Periods': 'SILENT' in result or 'gap' in result,
            'Synchronized Calling': 'SYNCHRONIZED' in result or 'COORDINATED' in result,
            'Location Analysis': 'LOCATION' in result or 'km/h' in result,
            'Risk Scoring': 'RISK' in result and ('HIGH' in result or 'MEDIUM' in result),
            'Network Analysis': 'NETWORK' in result or 'connections' in result
        }
        
        print("\nFeature Status:")
        for feature, detected in features.items():
            status = "✅" if detected else "❌"
            print(f"{status} {feature}: {'Detected' if detected else 'Not found'}")
        
        # Final summary
        print("\n" + "=" * 80)
        print("PHASE 1 TESTING COMPLETE")
        print("=" * 80)
        
        if successful == total:
            print("\n🎉 All tests passed! Phase 1 implementation is working correctly.")
        else:
            print(f"\n⚠️  {total - successful} tests failed. Please review the implementation.")
        
        print("\nNext steps:")
        print("1. Fix any failing tests")
        print("2. Review performance metrics")
        print("3. Test with real CDR data")
        print("4. Proceed to Phase 2 implementation")
        
    except Exception as e:
        logger.error(f"Test script error: {str(e)}")
        print(f"\n❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_phase1_patterns()