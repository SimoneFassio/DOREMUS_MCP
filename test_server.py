#!/usr/bin/env python3
"""
Test script for DOREMUS MCP Server

This script tests the basic functionality of the MCP server locally
before deployment.
"""

import sys
import json
from server import (
    find_candidate_entities,
    get_entity_details,
    search_musical_works,
    execute_custom_sparql
)


def print_result(title: str, result: dict):
    """Print a formatted test result."""
    print(f"\n{'='*60}")
    print(f"TEST: {title}")
    print('='*60)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print()


def test_find_entities():
    """Test entity search functionality."""
    print("\n🔍 Testing Entity Search...")
    
    # Test 1: Find Mozart
    result = find_candidate_entities("Mozart", "composer")
    print_result("Find Mozart (composer)", result)
    
    # Test 2: Find any entity named "Symphony"
    result = find_candidate_entities("Symphony", "work")
    print_result("Find Symphony (work)", result)
    
    return True


def test_search_works():
    """Test works search functionality."""
    print("\n🎵 Testing Works Search...")
    
    # Test 1: Simple search by composer name
    result = search_musical_works(
        composers=["Wolfgang Amadeus Mozart"],
        limit=5
    )
    print_result("Mozart's works (first 5)", result)
    
    # Test 2: Search by instrumentation
    result = search_musical_works(
        instruments=[
            {"name": "violin", "quantity": 2},
            {"name": "viola", "quantity": 1},
            {"name": "cello", "quantity": 1}
        ],
        limit=5
    )
    print_result("String quartets (first 5)", result)
    
    # Test 3: Search by date range
    result = search_musical_works(
        date_start=1800,
        date_end=1850,
        limit=5
    )
    print_result("Works from 1800-1850 (first 5)", result)
    
    # Test 4: Search by work type
    result = search_musical_works(
        work_type="sonata",
        limit=5
    )
    print_result("Sonatas (first 5)", result)
    
    return True


def test_custom_sparql():
    """Test custom SPARQL execution."""
    print("\n⚙️ Testing Custom SPARQL...")
    
    # Simple query to list some composers
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ecrm: <http://erlangen-crm.org/current/>
    PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
    
    SELECT DISTINCT ?composer ?name
    WHERE {
        ?expCreation efrbroo:R17_created ?expression ;
                     ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
        ?composer foaf:name ?name .
    }
    LIMIT 10
    """
    
    result = execute_custom_sparql(query, limit=10)
    print_result("List composers (first 10)", result)
    
    return True


def test_entity_details():
    """Test entity details retrieval."""
    print("\n📖 Testing Entity Details...")
    
    # First find Mozart's URI
    search_result = find_candidate_entities("Mozart", "composer")
    
    if search_result.get("success") and search_result.get("matches_found", 0) > 0:
        # Get the first Mozart result
        entities = search_result.get("entities", [])
        if entities:
            mozart_uri = entities[0].get("entity")
            if mozart_uri:
                result = get_entity_details(mozart_uri)
                print_result(f"Details for {mozart_uri}", result)
                return True
    
    print("⚠️ Could not find Mozart to test entity details")
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
        ("Works Search", test_search_works),
        ("Custom SPARQL", test_custom_sparql),
        ("Entity Details", test_entity_details),
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
