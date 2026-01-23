"""
Test script for DOREMUS MCP Server

This script tests the basic functionality of the MCP server locally
before deployment.
"""

import sys
import json
from src.server.tools_internal import (
    find_candidate_entities_internal,
    get_entity_properties_internal
)
from src.server.utils import execute_sparql_query

# Change for DEBUG
PRINT_RESULT=False


def print_result(title: str, result: dict):
    """Print a formatted test result."""
    if PRINT_RESULT:
        print(f"\n{'='*60}")
        print(f"TEST: {title}")
        print('='*60)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print()


def test_find_entities():
    """Test entity search functionality."""
    print("\n🔍 Testing Entity Search...")
    
    # Test 1: Find Mozart
    result = find_candidate_entities_internal("Mozart", "artist")
    if result.get("matches_found", 0) == 0:
        print("⚠️ Could not find Mozart entity")
        return False
    
    print_result("Find Mozart (composer)", result)
    
    # Test 2: Find any entity named "Symphony"
    result = find_candidate_entities_internal("Symphony", "vocabulary")
    if result.get("matches_found", 0) == 0:
        print("⚠️ Could not find Symphony entity")
        return False
    
    print_result("Find Symphony (work)", result)
    
    return True


def test_custom_sparql():
    """Test custom SPARQL execution."""
    print("\n⚙️ Testing Custom SPARQL...")
    
    # Simple query to list some composers
    query = """
    SELECT DISTINCT ?composer ?name
    WHERE {
        ?expCreation efrbroo:R17_created ?expression ;
                     ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
        ?composer foaf:name ?name .
    }
    LIMIT 2
    """
    
    result = execute_sparql_query(query, limit=2)
    if result.get("success"):
        print_result("List composers (first 2)", result)
        return True
    
    return False



def main():
    """Run all tests."""
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║        DOREMUS MCP Server - Test Suite                    ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    tests = [
        ("Entity Search", test_find_entities),
        ("Custom SPARQL", test_custom_sparql),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
            print(f"✅ {test_name}: PASSED")
        except Exception as e:
            print(f"❌ {test_name}: FAILED - {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
