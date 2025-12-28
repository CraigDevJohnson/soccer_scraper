#!/usr/bin/env python3
"""
Test script for soccer_schedule_scraper functionality
Tests local functions without AWS dependencies
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from soccer_schedule_scraper import (
    validate_team_id,
    compare_schedules
)

def test_validate_team_id():
    """Test team ID validation"""
    print("Testing team ID validation...")
    
    # Valid team IDs
    test_cases = [
        ("123456", True, "Valid 6-digit team ID"),
        ("000001", True, "Valid team ID with leading zeros"),
        ("999999", True, "Valid maximum team ID"),
        ("12345", False, "Invalid: Only 5 digits"),
        ("1234567", False, "Invalid: 7 digits"),
        ("abcdef", False, "Invalid: Not numeric"),
        ("", False, "Invalid: Empty string"),
        ("000000", False, "Invalid: Zero"),
        ("-12345", False, "Invalid: Negative"),
        (123456, False, "Invalid: Not a string"),
    ]
    
    passed = 0
    failed = 0
    
    for test_id, should_pass, description in test_cases:
        try:
            result = validate_team_id(test_id)
            if should_pass:
                print(f"  ✓ {description}: {test_id}")
                passed += 1
            else:
                print(f"  ✗ {description}: Expected to fail but passed")
                failed += 1
        except (ValueError, TypeError) as e:
            if not should_pass:
                print(f"  ✓ {description}: Correctly rejected")
                passed += 1
            else:
                print(f"  ✗ {description}: Unexpected error: {e}")
                failed += 1
    
    print(f"\nValidation tests: {passed} passed, {failed} failed\n")
    return failed == 0

def test_compare_schedules():
    """Test schedule comparison logic"""
    print("Testing schedule comparison...")
    
    # Old schedule
    old_schedule = {
        'game1': {
            'game_id': 'game1',
            'date': 'Mon 12/01 06:00 PM',
            'field': '1',
            'home_team': 'Team A',
            'away_team': 'Team B'
        },
        'game2': {
            'game_id': 'game2',
            'date': 'Wed 12/03 07:00 PM',
            'field': '2',
            'home_team': 'Team C',
            'away_team': 'Team D'
        }
    }
    
    # New schedule with changes
    new_schedule = [
        {
            'id': 'game1',
            'date': 'Mon 12/01 07:00 PM',  # Time changed
            'field': '1',
            'home_team': 'Team A',
            'away_team': 'Team B'
        },
        {
            'id': 'game3',  # New game
            'date': 'Fri 12/05 06:00 PM',
            'field': '3',
            'home_team': 'Team E',
            'away_team': 'Team F'
        }
    ]
    # game2 is missing (cancelled)
    
    changes = compare_schedules(old_schedule, new_schedule)
    
    tests_passed = 0
    tests_failed = 0
    
    # Check for added games
    if len(changes['added']) == 1 and changes['added'][0]['id'] == 'game3':
        print("  ✓ Detected new game correctly")
        tests_passed += 1
    else:
        print(f"  ✗ Failed to detect new game: {changes['added']}")
        tests_failed += 1
    
    # Check for modified games
    if len(changes['modified']) == 1 and changes['modified'][0]['new']['id'] == 'game1':
        print("  ✓ Detected modified game correctly")
        tests_passed += 1
    else:
        print(f"  ✗ Failed to detect modified game: {changes['modified']}")
        tests_failed += 1
    
    # Check for removed games
    if len(changes['removed']) == 1 and changes['removed'][0]['game_id'] == 'game2':
        print("  ✓ Detected removed game correctly")
        tests_passed += 1
    else:
        print(f"  ✗ Failed to detect removed game: {changes['removed']}")
        tests_failed += 1
    
    print(f"\nSchedule comparison tests: {tests_passed} passed, {tests_failed} failed\n")
    return tests_failed == 0

def main():
    """Run all tests"""
    print("=" * 60)
    print("Soccer Scraper Test Suite")
    print("=" * 60 + "\n")
    
    all_passed = True
    
    # Run tests
    all_passed &= test_validate_team_id()
    all_passed &= test_compare_schedules()
    
    # Summary
    print("=" * 60)
    if all_passed:
        print("✓ All tests passed!")
        print("=" * 60)
        return 0
    else:
        print("✗ Some tests failed")
        print("=" * 60)
        return 1

if __name__ == "__main__":
    sys.exit(main())
