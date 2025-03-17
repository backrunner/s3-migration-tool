import { Command } from 'commander';
import chalk from 'chalk';
import { name, version } from '../package.json';
import { loadConfig } from './config';
import { runMigration } from './migration';
import { displayHelp } from './help';

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
  .option('-y, --yes', 'Skip confirmation prompts and proceed with migration')
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

      if (options.concurrency) {
        config.concurrency = options.concurrency;
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

      // Run migration
      await runMigration(config);
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
  $ ${name} help config
  $ ${name} help filters
  $ ${name} help process
`);

// Parse command line arguments
program.parse();
