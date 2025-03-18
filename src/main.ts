// Third-party dependencies
import chalk from 'chalk';
import { Command } from 'commander';

// Local imports
import { loadConfig } from './config';
import { displayHelp } from './help';
import { runMigration } from './migration';
import { purgeSourceBucket } from './purge-source';
import { cleanTargetBucket } from './clean-target';

// Package info
import { name, version } from '../package.json';

// Create the command line program
const program = new Command();

// Main command
program
  .name(name)
  .version(version)
  .description('A CLI tool for migrating objects between S3 compatible storage buckets')
  .argument('<config-file>', 'Path to the migration configuration file (YAML or JSON)')
  .option('-d, --dry-run', 'Run in dry-run mode (no actual transfers)')
  .option('-c, --concurrency <number>', 'Number of concurrent transfers', parseInt)
  .option('-p, --prefix <prefix>', 'Only migrate objects with this prefix')
  .option('-m, --mode <mode>', 'Transfer mode: auto, memory, disk, or stream')
  .option('-y, --yes', 'Skip confirmation prompts and proceed with migration')
  .option('-v, --verbose', 'Enable verbose logging with detailed error messages')
  .option('-l, --log-file <path>', 'Save logs to the specified file')
  .option('--verify-content', 'Verify file content after migration using checksums')
  .action(async (configFile, options) => {
    try {
      // Load configuration
      const config = loadConfig(configFile);

      // Override config with command line options
      if (options.dryRun) {
        config.dryRun = true;
      }

      if (options.concurrency) {
        config.concurrency = options.concurrency;
      }

      if (options.prefix) {
        config.prefix = options.prefix;
      }

      if (options.mode) {
        config.transferMode = options.mode;
      }

      if (options.verifyContent) {
        config.verifyFileContentAfterMigration = true;
      }

      if (options.yes) {
        config.skipConfirmation = true;
      }

      if (options.verbose) {
        config.verbose = true;
      }

      if (options.logFile) {
        config.logFile = options.logFile;
      }

      // Run migration
      await runMigration(config);
    } catch (error) {
      console.error(chalk.red.bold('Error:'), chalk.red((error as Error).message));
      process.exit(1);
    }
  });

// Purge source command
program
  .command('purge-source')
  .description('Purge files from source bucket (with verification against target if available)')
  .argument('<config-file>', 'Path to the migration configuration file (YAML or JSON)')
  .option('-p, --prefix <prefix>', 'Only purge objects with this prefix')
  .option('-d, --dry-run', 'Run in dry-run mode (no actual deletions)')
  .option('-y, --yes', 'Skip confirmation prompts and proceed with purge')
  .option('-v, --verbose', 'Enable verbose logging with detailed error messages')
  .option('-l, --log-file <path>', 'Save logs to the specified file')
  .option('--skip-verification', 'Skip verification of files existence in target bucket')
  .action(async (configFile, options) => {
    try {
      // Load configuration
      const config = loadConfig(configFile);

      // Override config with command line options
      if (options.dryRun) {
        config.dryRun = true;
      }

      if (options.prefix) {
        config.prefix = options.prefix;
      }

      if (options.yes) {
        config.skipConfirmation = true;
      }

      if (options.verbose) {
        config.verbose = true;
      }

      if (options.logFile) {
        config.logFile = options.logFile;
      }

      // Run purge process
      await purgeSourceBucket(config, {
        skipVerification: options.skipVerification
      });
    } catch (error) {
      console.error(chalk.red.bold('Error:'), chalk.red((error as Error).message));
      process.exit(1);
    }
  });

// Clean target command
program
  .command('clean-target')
  .description('Clean incomplete multipart uploads from target bucket')
  .argument('<config-file>', 'Path to the migration configuration file (YAML or JSON)')
  .option('-p, --prefix <prefix>', 'Only clean objects with this prefix')
  .option('-d, --dry-run', 'Run in dry-run mode (no actual deletions)')
  .option('-y, --yes', 'Skip confirmation prompts and proceed with cleanup')
  .option('-v, --verbose', 'Enable verbose logging with detailed error messages')
  .option('-l, --log-file <path>', 'Save logs to the specified file')
  .action(async (configFile, options) => {
    try {
      // Load configuration
      const config = loadConfig(configFile);

      // Override config with command line options
      if (options.dryRun) {
        config.dryRun = true;
      }

      if (options.prefix) {
        config.prefix = options.prefix;
      }

      if (options.yes) {
        config.skipConfirmation = true;
      }

      if (options.verbose) {
        config.verbose = true;
      }

      if (options.logFile) {
        config.logFile = options.logFile;
      }

      // Run cleanup process
      await cleanTargetBucket(config);
    } catch (error) {
      console.error(chalk.red.bold('Error:'), chalk.red((error as Error).message));
      process.exit(1);
    }
  });

// Help command
program
  .command('help [topic]')
  .description('Display help information about specific topics')
  .action((topic) => {
    displayHelp(topic, name);
  });

// Add examples
program.addHelpText('after', `
Examples:
  $ ${name} ./migration.yaml
  $ ${name} ./migration.json --dry-run
  $ ${name} ./migration.yaml --concurrency 10
  $ ${name} ./migration.yaml --prefix "images/"
  $ ${name} ./migration.yaml --mode stream
  $ ${name} ./migration.yaml --verify-content
  $ ${name} purge-source ./migration.yaml --prefix "temp/"
  $ ${name} clean-target ./migration.yaml
  $ ${name} help config
  $ ${name} help filters
  $ ${name} help transferModes
  $ ${name} help process
`);

// Parse command line arguments
program.parse();
