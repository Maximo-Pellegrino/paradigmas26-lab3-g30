#!/bin/bash

# Integration tests for reddit-ner-scala skeleton with output assertions
# Modular test functions for flexibility and extensibility

# Constants
ENV_VARS="SBT_OPTS=\"--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED\""
TESTS_DIR="data"
PASSED=0
FAILURES=0
ERRORS=0
TOTAL=0

# Execute a test command and save output to temp file
run_test() {
  local test_name="$1"
  local test_cmd="$2"
  local temp_file="$3"

  echo ""
  echo "==============================================="
  echo "TEST: $test_name"
  echo "Command: $test_cmd"
  echo "==============================================="

  TOTAL=$((TOTAL + 1))

  # Run test and capture output to temp file
  eval "$test_cmd" > "$temp_file" 2>&1
  local exit_code=$?

  if [ $exit_code -ne 0 ]; then
    echo "✗ ERROR: Test exited with code $exit_code"
    ERRORS=$((ERRORS + 1))
    return 1
  fi

  return 0
}

# Assert that output contains a string
assert_contains() {
  local temp_file="$1"
  local expected_string="$2"
  local description="$3"

  if grep -q "$expected_string" "$temp_file"; then
    echo "✓ Assertion passed: $description"
    return 0
  else
    echo "✗ Assertion FAILED: Expected to find '$expected_string'"
    echo "   Description: $description"
    FAILURES=$((FAILURES + 1))
    return 1
  fi
}

# Assert that output has exactly N entity lines after a marker
assert_entity_count() {
  local temp_file="$1"
  local expected_count="$2"
  local marker="$3"

  # Count lines starting with [ after the marker (entity lines)
  actual_count=$(awk "/$marker/,/^$/" "$temp_file" | tail -n +2 | grep -c "^\[info\] \[Type" || true)

  if [ "$actual_count" -eq "$expected_count" ]; then
    echo "✓ Assertion passed: Found exactly $expected_count entity lines after '$marker'"
    return 0
  else
    echo "✗ Assertion FAILED: Expected $expected_count entities after '$marker', found $actual_count"
    FAILURES=$((FAILURES + 1))
    return 1
  fi
}

# Assert that output does NOT contain a string
assert_not_contains() {
  local temp_file="$1"
  local unexpected_string="$2"
  local description="$3"

  if ! grep -q "$unexpected_string" "$temp_file"; then
    echo "✓ Assertion passed: $description"
    return 0
  else
    echo "✗ Assertion FAILED: Found unexpected '$unexpected_string'"
    echo "   Description: $description"
    FAILURES=$((FAILURES + 1))
    return 1
  fi
}

# Test 1: Default parameters with local fetch
temp_file=$(mktemp)
run_test \
  "Default parameters (with local urls)" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/local_subscriptions.json\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_entity_count "$temp_file" "10" "ENTIDADES NOMBRADAS"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 2: top-k=2
temp_file=$(mktemp)
run_test \
  "Custom top-k parameter (2)" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/local_subscriptions.json --entities-dir $TESTS_DIR/valid_entities --top-k 2\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_not_contains "$temp_file" "Warning: Failed to download from" "Should not warn about bad URL"
  assert_not_contains "$temp_file" "Warning: Skipping malformed subscription" "Should not warn about malformed subscription"
  assert_entity_count "$temp_file" "2" "ENTIDADES NOMBRADAS"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 3: Malformed JSON subscriptions
temp_file=$(mktemp)
run_test \
  "Malformed JSON in subscriptions file" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/malformed_json_subscriptions.json\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_contains "$temp_file" "invalid JSON format" "Should error on invalid JSON"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 4: Bad URLs in subscriptions
temp_file=$(mktemp)
run_test \
  "Bad/unreachable URLs in subscriptions" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/bad_url_subscriptions.json --entities-dir $TESTS_DIR/valid_entities\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_contains "$temp_file" "Warning: Failed to download from" "Should warn about bad URL"
  assert_contains "$temp_file" "Feeds fallidos: 1" "Should show 1 failed feed"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 5: Incorrect format subscriptions
temp_file=$(mktemp)
run_test \
  "Incorrect subscription format (missing fields)" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/incorrect_format_subscriptions.json --entities-dir $TESTS_DIR/valid_entities\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_contains "$temp_file" "Warning: Skipping malformed subscription" "Should warn about malformed entry"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 6: Missing entity directory
temp_file=$(mktemp)
run_test \
  "Missing entity directory" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/local_subscriptions.json --entities-dir $TESTS_DIR/nonexistent\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_contains "$temp_file" "Error.*entities directory.*not found" "Should error on missing directory"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Test 7: Missing some entity files
temp_file=$(mktemp)
run_test \
  "Partial entity files (some missing)" \
  "$ENV_VARS sbt \"run --subscription-file $TESTS_DIR/local_subscriptions.json --entities-dir $TESTS_DIR/missing_entities\"" \
  "$temp_file"

if [ $? -eq 0 ]; then
  assert_contains "$temp_file" "Warning: Could not load" "Should warn about missing files"
  rm "$temp_file"
  PASSED=$((PASSED + 1))
fi

# Summary
echo ""
echo "==============================================="
echo "TEST SUMMARY"
echo "==============================================="
echo "Total:    $TOTAL"
echo "Passed:   $PASSED"
echo "Errors:   $ERRORS"
echo "Assertion Failures: $FAILURES"
echo "==============================================="

if [ $((FAILURES + ERRORS)) -eq 0 ]; then
  echo "✓ All tests passed!"
  exit 0
else
  echo "✗ Some tests failed"
  exit 1
fi
