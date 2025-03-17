# S3 Migration CLI

A command-line tool for migrating objects between S3-compatible storage buckets.

## Features

- Migrate objects between any S3-compatible storage services
- Support for filtering objects by prefix, include/exclude patterns
- Concurrent transfers for improved performance
- Dry-run mode for testing without actual transfers
- Colorful and informative progress display with real-time transfer speeds
- File-level progress bars showing download and upload progress
- Automatic retry for failed transfers with configurable retry limit
- Interactive pause/resume/skip functionality for handling failures
- Verification of migrated files to ensure successful transfer
- Option to purge source files after successful migration
- Detailed migration summary

## Installation

```bash
# Install globally
npm install -g s3-migration-cli

# Or use with npx
npx s3-migration-cli <config-file>
```

## Usage

```bash
s3-migrate <config-file> [options]
```

### Options

- `-d, --dry-run`: Run in dry-run mode (no actual transfers)
- `-c, --concurrency <number>`: Number of concurrent transfers
- `-p, --prefix <prefix>`: Only migrate objects with this prefix
- `-y, --yes`: Skip confirmation prompts and proceed with migration
- `-v, --verbose`: Enable verbose logging with detailed error messages
- `-l, --log-file <path>`: Save logs to the specified file
- `-V, --version`: Output the version number
- `-h, --help`: Display help information

### Commands

- `help [topic]`: Display detailed help information about specific topics
  - Available topics: `config`, `filters`, `process`

### Examples

```bash
# Basic usage
s3-migrate ./migration.yaml

# Run in dry-run mode
s3-migrate ./migration.json --dry-run

# Set concurrency level
s3-migrate ./migration.yaml --concurrency 10

# Only migrate objects with a specific prefix
s3-migrate ./migration.yaml --prefix "images/"

# Skip confirmation prompts
s3-migrate ./migration.yaml --yes

# Enable verbose output and log to file
s3-migrate ./migration.yaml --verbose --log-file ./migration-logs.txt

# Get detailed help on configuration
s3-migrate help config

# Get detailed help on filtering options
s3-migrate help filters
```

## Configuration

Create a YAML or JSON configuration file with the following structure:

```yaml
# Source bucket configuration
source:
  endpoint: 'https://s3.amazonaws.com'
  accessKey: 'YOUR_SOURCE_ACCESS_KEY'
  secretKey: 'YOUR_SOURCE_SECRET_KEY'
  region: 'us-east-1'
  bucket: 'source-bucket-name'
  forcePathStyle: false
  useAccelerateEndpoint: false
  signatureVersion: 'v4' # Optional: 'v2' or 'v4'

# Target bucket configuration
target:
  endpoint: 'https://storage.example.com'
  accessKey: 'YOUR_TARGET_ACCESS_KEY'
  secretKey: 'YOUR_TARGET_SECRET_KEY'
  region: 'eu-west-1'
  bucket: 'target-bucket-name'
  forcePathStyle: true
  useAccelerateEndpoint: false
  signatureVersion: 'v4' # Optional: 'v2' or 'v4'

# Optional parameters
concurrency: 10
maxRetries: 3
prefix: 'data/'
include:
  - "\\.jpg$"
  - "\\.png$"
exclude:
  - '^temp/'
  - "\\.tmp$"
verifyAfterMigration: true
purgeSourceAfterMigration: false
dryRun: false
```

### Configuration Options

#### S3 Bucket Configuration Options

| Option                  | Description                          | Default  |
| ----------------------- | ------------------------------------ | -------- |
| `endpoint`              | S3 endpoint URL                      | Required |
| `accessKey`             | Access key for authentication        | Required |
| `secretKey`             | Secret key for authentication        | Required |
| `region`                | AWS region                           | Required |
| `bucket`                | Bucket name                          | Required |
| `forcePathStyle`        | Use path-style addressing            | false    |
| `useAccelerateEndpoint` | Use S3 accelerate endpoint           | false    |
| `signatureVersion`      | AWS signature version ('v2' or 'v4') | 'v4'     |

#### Migration Configuration Options

| Option                      | Description                                             | Default  |
| --------------------------- | ------------------------------------------------------- | -------- |
| `source`                    | Source bucket configuration                             | Required |
| `target`                    | Target bucket configuration                             | Required |
| `concurrency`               | Number of concurrent transfers                          | 5        |
| `maxRetries`                | Maximum number of retry attempts for failed transfers   | 3        |
| `prefix`                    | Only migrate objects with this prefix (optional)        | None     |
| `include`                   | Only include objects matching these patterns (optional) | None     |
| `exclude`                   | Exclude objects matching these patterns (optional)      | None     |
| `verifyAfterMigration`      | Verify objects exist in target after migration          | true     |
| `purgeSourceAfterMigration` | Delete objects from source after successful migration   | false    |
| `skipConfirmation`          | Skip confirmation prompts and proceed with migration    | false    |
| `dryRun`                    | Run in dry-run mode (no actual transfers)               | false    |
| `verbose`                   | Enable verbose logging with detailed error messages     | false    |
| `logFile`                   | Save logs to the specified file                         | None     |

See [example-config.yaml](./example-config.yaml) for a complete example.

## Logging

When using the `logFile` option, the CLI will generate detailed logs of the migration process. These logs include:

- Migration configuration details
- Step-by-step progress of the migration process
- Detailed error information including stack traces (when available)
- Status of each file's transfer, verification, and deletion
- Summary information and statistics

Log files are written in append mode, so multiple runs will add to the same log file rather than overwriting it. Each execution is clearly separated by a header that includes:

```
================================================================================
== S3 MIGRATION EXECUTION STARTED AT 2023-04-25T12:34:56.789Z ==
== Execution ID: 1682428496789-1234 ==
== PID: 1234, User: johndoe ==
== OS: darwin 20.6.0, Hostname: macbook-pro ==
================================================================================
```

This makes it easy to track and analyze multiple migration runs in the same log file.

## Filtering Options

The tool provides several ways to filter which objects are migrated:

- **Prefix**: Only migrate objects whose keys start with the specified prefix

  ```yaml
  prefix: 'images/' # Only migrate objects in the "images/" folder
  ```

- **Include patterns**: Only migrate objects whose keys match any of the specified regex patterns

  ```yaml
  include:
    - "\\.jpg$" # Only migrate JPG files
    - "\\.png$" # Only migrate PNG files
  ```

- **Exclude patterns**: Skip objects whose keys match any of the specified regex patterns
  ```yaml
  exclude:
    - '^temp/' # Skip objects in the "temp/" folder
    - "\\.tmp$" # Skip temporary files
  ```

All filtering options are optional and can be used in combination. If both include and exclude patterns are specified, objects must match an include pattern AND not match any exclude pattern to be migrated.

## Migration Process

The migration process follows these steps:

1. **Listing**: List all objects in the source bucket (applying filters if specified)
2. **Transfer**: Download objects from source and upload to target with progress tracking
3. **Verification**: Verify all migrated files exist in the target bucket
4. **Summary**: Display a detailed summary of the migration
5. **Retry**: Prompt to retry any failed transfers
6. **Purge**: Optionally delete successfully migrated files from the source bucket

## Error Handling

When a file transfer fails:

1. The tool will automatically retry the transfer up to the configured `maxRetries` limit
2. If the maximum retry count is reached, the migration will pause
3. You'll be prompted to:
   - Skip the failed files and continue
   - Retry the failed files
   - Stop the migration
4. After the migration completes, you'll be asked if you want to retry any failed files

## Verification

After all files are transferred, the tool will verify that each file exists in the target bucket.
If any files fail verification, they will be marked as failed and you'll be prompted to retry them.

## Source Purge

If all files are successfully migrated and verified, you'll be asked if you want to delete the files
from the source bucket. This requires double confirmation to prevent accidental deletion.

The purge process includes:

1. A progress bar showing real-time deletion progress
2. A summary of deleted and failed files
3. Detailed error information for any files that failed to delete

## License

MIT
