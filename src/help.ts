// Third-party dependencies
import chalk from 'chalk';

// Define help topic interface
export interface HelpTopic {
  title: string;
  content: string;
}

// Define help topics
export const helpTopics: Record<string, HelpTopic> = {
  config: {
    title: 'Configuration File Format',
    content: `
Configuration File (YAML or JSON):
  source:
    endpoint: 'https://s3.amazonaws.com'
    accessKey: 'YOUR_SOURCE_ACCESS_KEY'
    secretKey: 'YOUR_SOURCE_SECRET_KEY'
    region: 'us-east-1'
    bucket: 'source-bucket-name'
    forcePathStyle: false
    useAccelerateEndpoint: false
    signatureVersion: 'v4'   # Optional: 'v2' or 'v4' (default is 'v4')

  target:
    endpoint: 'https://storage.example.com'
    accessKey: 'YOUR_TARGET_ACCESS_KEY'
    secretKey: 'YOUR_TARGET_SECRET_KEY'
    region: 'eu-west-1'
    bucket: 'target-bucket-name'
    forcePathStyle: true
    useAccelerateEndpoint: false
    signatureVersion: 'v4'   # Optional: 'v2' or 'v4' (default is 'v4')

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

  # File transfer mode options
  transferMode: 'auto'        # 'auto', 'memory', 'disk', or 'stream'
  tempDir: './tmp'            # Temporary directory for disk mode
  largeFileSizeThreshold: 104857600  # 100MB threshold for large files

  verifyAfterMigration: true
  verifyFileContentAfterMigration: false  # Verify file content using checksums
  purgeSourceAfterMigration: false
  skipConfirmation: false      # Skip confirmation prompts
  verbose: false               # Enable verbose logging
  logFile: "./logs/migration.log"  # Save logs to file
  dryRun: false
    `
  },
  filters: {
    title: 'Filtering Options',
    content: `
Filtering Options:
  - prefix: Only migrate objects whose keys start with the specified prefix
    Example: prefix: "images/"

  - include: Only migrate objects whose keys match any of the specified regex patterns
    Example:
      include:
        - "\\.jpg$"  # Only migrate JPG files
        - "\\.png$"  # Only migrate PNG files

  - exclude: Skip objects whose keys match any of the specified regex patterns
    Example:
      exclude:
        - "^temp/"   # Skip objects in the "temp/" folder
        - "\\.tmp$"  # Skip temporary files

  All filtering options are optional and can be used in combination.
    `
  },
  transferModes: {
    title: 'Transfer Modes',
    content: `
Transfer Modes:
  The CLI supports several transfer modes for handling files of different sizes efficiently:

  - auto: (Default) Automatically select the best mode based on file size
    * Uses memory mode for small files
    * Uses disk mode for medium files
    * Uses stream mode for very large files
    * Recommended for most use cases

  - memory: Load the entire file into memory during transfer
    * Fastest performance for small to medium files
    * NOT recommended for large files
    * Will be automatically rejected for files larger than 50% of available memory
    * For files too large for memory, will automatically switch to disk mode
    * Example: transferMode: "memory"

  - disk: Use temporary files on disk for the transfer
    * Good balance between performance and memory usage
    * Suitable for files of any size
    * Requires sufficient disk space in the temp directory
    * Specify a custom temp directory with: tempDir: "/path/to/temp"
    * Example: transferMode: "disk"

  - stream: Stream files directly without caching
    * Uses direct piping between source and target
    * Lowest memory usage - only buffers current chunk in memory
    * Good performance with minimal memory consumption
    * Best for very large files or systems with limited memory
    * Default mode for all transfers
    * Example: transferMode: "stream"

  Large File Threshold:
    You can customize the threshold for what is considered a "large file" by setting:
    largeFileSizeThreshold: 104857600  # 100MB

  Note on Memory Mode Safety:
    When using memory mode, the CLI will automatically check if the file size exceeds
    50% of available system memory. If it does, the CLI will automatically switch to disk mode.
    For very large files (>80% of available memory), disk mode will automatically switch to stream mode.
    `
  },
  process: {
    title: 'Migration Process',
    content: `
Migration Process:
  1. List all objects in the source bucket (applying filters if specified)
  2. Transfer objects from source to target
  3. Verify all migrated files exist in the target
  4. Display detailed migration summary
  5. Retry any failed transfers (if needed)
  6. Optionally delete original files from source

Error Handling:
  - Automatic retries for failed transfers (up to maxRetries)
  - Interactive prompts when failures occur
  - Option to skip, retry, or stop the migration
  - Final retry opportunity for all failed files

Verification:
  After all files are transferred, the tool verifies each file exists in the target.
  This ensures that no files are missed during migration.

  Content Verification:
  When verifyFileContentAfterMigration is enabled, file content is also verified
  using checksums (MD5 or ETag) to ensure data integrity. This provides an additional
  level of verification beyond simple existence checks.

Source Purge:
  If all files are successfully migrated, you can choose to delete the original files.
  This requires double confirmation to prevent accidental deletion.
    `
  }
};

/**
 * Display help information for a specific topic or list all available topics
 * @param topic Optional topic to display help for
 * @param programName Name of the program for display in help text
 */
export function displayHelp(topic: string | undefined, programName: string): void {
  if (!topic) {
    console.log(chalk.blue.bold(`${programName} Help`));
    console.log(chalk.blue('─'.repeat(50)));
    console.log('Available help topics:');
    Object.keys(helpTopics).forEach(key => {
      console.log(`  ${chalk.yellow(key)}: ${helpTopics[key].title}`);
    });
    console.log('\nUse:', chalk.yellow(`${programName} help <topic>`), 'for detailed information');
    return;
  }

  if (helpTopics[topic]) {
    console.log(chalk.blue.bold(helpTopics[topic].title));
    console.log(chalk.blue('─'.repeat(50)));
    console.log(helpTopics[topic].content);
  } else {
    console.log(chalk.red(`Unknown help topic: ${topic}`));
    console.log('Available topics:', Object.keys(helpTopics).join(', '));
  }
}
