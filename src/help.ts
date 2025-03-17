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
  verifyAfterMigration: true
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
  process: {
    title: 'Migration Process',
    content: `
Migration Process:
  1. Listing: List all objects in the source bucket (applying filters if specified)
  2. Transfer: Download objects from source and upload to target with progress tracking
  3. Verification: Verify all migrated files exist in the target bucket
  4. Summary: Display a detailed summary of the migration
  5. Retry: Prompt to retry any failed transfers
  6. Purge: Optionally delete successfully migrated files from the source bucket
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
