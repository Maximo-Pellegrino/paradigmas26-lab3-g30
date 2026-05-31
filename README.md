# Reddit Named Entity Recognition System

This system downloads Reddit posts from specified subreddits and performs named entity recognition (NER) to extract and count entities like people, organizations, universities, programming languages, and places.

## How It Works

The system combines functional and object-oriented programming paradigms:

**Functional pipeline**: Downloads feeds from Reddit subscriptions → parses JSON posts → filters empty posts → applies entity detection → aggregates counts → produces formatted output.

**Object-oriented design**: Entity types are modeled as a class hierarchy (Person, Organization, University, Place, Technology, ProgrammingLanguage) with polymorphic behavior. Dictionary loading and output formatting use encapsulated object methods.

## Requirements

- Java 11 or later
- Scala 2.13
- sbt 1.9 or later

## Setup

1. Clone or extract the project
2. No additional dependencies need manual installation—sbt will download them automatically on first build

## Building

```bash
sbt compile
```

This downloads json4s and scopt libraries and compiles the Scala code.

## Running

Basic usage with defaults:
```bash
sbt run
```

With custom parameters:
```bash
sbt "run --subscription-file subscriptions.json --entities-dir data --top-k 15"
```

### Command-Line Arguments

All arguments are optional:

- `--subscription-file <file>`: Path to JSON file containing Reddit subscriptions (default: `subscriptions.json`)
- `--entities-dir <dir>`: Path to directory containing entity dictionary files (default: `data`)
- `--top-k <n>`: Number of top entities to display (default: `10`)

## Project Structure

- `src/main/scala/`: Source code
  - `Main.scala`: Entry point and orchestration
  - `FileIO.scala`: File and network I/O operations
  - `JsonParser.scala`: Reddit JSON parsing
  - `Analyzer.scala`: Entity detection and counting logic
  - `Dictionary.scala`: Entity dictionary loading
  - `NamedEntity.scala`: Entity class hierarchy
  - `Formatters.scala`: Output formatting
  - `CommandLineArgs.scala`: Argument parsing
  - `Subscription.scala`, `Post.scala`: Data structures

- `data/`: Entity dictionary files and test data
  - `people.txt`, `universities.txt`, `languages.txt`, `organizations.txt`, `places.txt`: Entity lists
  - Test data directories for validation

- `subscriptions.json`: List of Reddit subreddits to process

## Understanding Entity Detection

The system performs dictionary-based entity matching. It reads entity names from dictionary files, then searches for whole-word matches (case-insensitive) in post content. A word matches only if it appears as a complete word, not as part of another word (e.g., "Scala" matches but "java" does not match in "javascript").

## Testing

The system includes integration tests that verify:

- Valid subscription processing with various top-k values
- Error handling for malformed JSON subscriptions
- Handling of unreachable URLs
- Handling of incorrect subscription formats
- Error detection for missing entity directories
- Graceful degradation when some entity files are missing
- Correct output counts for different top-k parameters
- Default parameter behavior

### Running Tests

```bash
bash tests.sh
```

This runs all 9 integration tests and displays:
- Individual test output with pass/fail results for each assertion
- Summary showing total tests, passed, errors, and assertion failures

### Test Data

The `data/` directory contains several test datasets:

- `valid_subscriptions.json`: Clean subscriptions file used as baseline
- `malformed_json_subscriptions.json`: Invalid JSON for error handling tests
- `bad_url_subscriptions.json`: Subscriptions with unreachable URLs
- `incorrect_format_subscriptions.json`: Subscriptions missing required fields
- `valid_entities/`: Complete set of entity dictionary files
- `missing_entities/`: Partial entity files for testing graceful degradation

## Output Format

The program prints two sections:

**Processing Statistics**: Shows how many feeds were successfully downloaded, how many failed, how many posts were processed, filtered, and failed.

**Top Named Entities**: Lists the most frequent entities by type and name, sorted by frequency (descending), then entity type (alphabetical), then entity name (alphabetical).

## Error Handling

The system handles failures gracefully:

- Missing subscriptions file: Prints error and exits
- Malformed JSON in subscriptions: Skips invalid entries, continues with valid ones
- Network failures: Warns about failed downloads and continues with successful feeds
- Missing entity directory: Prints error and exits
- Missing individual entity files: Warns but continues with available dictionaries

All errors are printed to console for visibility. The system continues processing valid data whenever possible.
