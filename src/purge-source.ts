// Third-party dependencies
import chalk from 'chalk';
import cliProgress from 'cli-progress';
import inquirer from 'inquirer';
import ora from 'ora';

// Local imports
import { createS3Client, deleteObjects, listAllObjects, objectExists } from './s3-client';
import {
  initLogger,
  log,
  LogLevel,
  logError,
  logSuccess,
  logVerbose,
  logWarning,
  logInfo,
} from './logger';
import { formatTime } from './utils';

// Types
import type { S3Client } from '@aws-sdk/client-s3';
import type { MigrationConfig } from './types';

interface PurgeOptions {
  skipVerification?: boolean;
}

interface PurgeResult {
  total: number;
  deleted: number;
  failed: number;
  missingInTarget: number;
  startTime: number;
  endTime?: number;
}

/**
 * Function to purge objects from source bucket
 */
export async function purgeSourceBucket(
  config: MigrationConfig,
  options: PurgeOptions = {}
): Promise<void> {
  // Initialize logger
  initLogger(config);

  // Create S3 clients
  const sourceClient = createS3Client(config.source);
  let targetClient: S3Client | undefined;

  if (config.target && !options.skipVerification) {
    // Only create target client if we need to verify files exist in target
    targetClient = createS3Client(config.target);
  }

  try {
    logInfo('Starting source bucket purge...', chalk.cyanBright);
    logInfo(`Source: ${config.source.endpoint}/${config.source.bucket}`, chalk.cyan);

    if (targetClient && config.target) {
      logInfo(`Target: ${config.target.endpoint}/${config.target.bucket} (for verification)`, chalk.cyan);
    }

    if (config.prefix) {
      logInfo(`Prefix: ${config.prefix}`, chalk.cyan);
    }

    if (config.dryRun) {
      logWarning('DRY RUN MODE - No files will be deleted');
      log(LogLevel.WARNING, 'DRY RUN MODE - No files will be deleted');
    }

    // List all objects in source bucket
    const spinner = ora('Listing objects from source bucket...').start();
    let sourceKeys: string[] = [];

    try {
      // Use prefix only if provided
      for await (const keys of listAllObjects(sourceClient, config.source.bucket, config.prefix || undefined)) {
        sourceKeys.push(...keys);
      }

      // Apply include filters if provided
      if (config.include && config.include.length > 0) {
        try {
          const patterns = config.include.map(pattern => new RegExp(pattern));
          sourceKeys = sourceKeys.filter(key => patterns.some(pattern => pattern.test(key)));
        } catch (error) {
          logError(`Invalid include pattern`, error as Error);
          spinner.fail(`Invalid include pattern: ${(error as Error).message}`);
          process.exit(1);
        }
      }

      // Apply exclude filters if provided
      if (config.exclude && config.exclude.length > 0) {
        try {
          const patterns = config.exclude.map(pattern => new RegExp(pattern));
          sourceKeys = sourceKeys.filter(key => !patterns.some(pattern => pattern.test(key)));
        } catch (error) {
          logError(`Invalid exclude pattern`, error as Error);
          spinner.fail(`Invalid exclude pattern: ${(error as Error).message}`);
          process.exit(1);
        }
      }

      const totalFiles = sourceKeys.length;
      spinner.succeed(`Found ${chalk.bold(totalFiles.toString())} objects in source bucket`);
      logSuccess(`Found ${totalFiles} objects in source bucket`);

      if (totalFiles === 0) {
        logWarning('No objects to purge. Exiting.');
        return;
      }

      // Initialize purge result
      const result: PurgeResult = {
        total: totalFiles,
        deleted: 0,
        failed: 0,
        missingInTarget: 0,
        startTime: Date.now()
      };

      // Verify files exist in target if needed
      let keysToDelete = sourceKeys;
      let missingKeys: string[] = [];

      if (targetClient && config.target && !options.skipVerification) {
        const verifySpinner = ora('Verifying files exist in target bucket...').start();

        // Store missing keys
        missingKeys = [];

        // Check in batches to avoid overwhelming the target S3 service
        const batchSize = 100;
        for (let i = 0; i < sourceKeys.length; i += batchSize) {
          const batch = sourceKeys.slice(i, Math.min(i + batchSize, sourceKeys.length));

          // Check each key in the batch
          await Promise.all(batch.map(async (key) => {
            try {
              const exists = await objectExists(targetClient!, config.target!.bucket, key);
              if (!exists) {
                missingKeys.push(key);
              }
            } catch (error) {
              logError(`Error checking if object exists in target: ${key}`, error as Error);
              // Conservatively assume it's missing if we can't check
              missingKeys.push(key);
            }
          }));

          // Update spinner to show progress
          verifySpinner.text = `Verifying files exist in target bucket (${Math.min(i + batchSize, sourceKeys.length)}/${sourceKeys.length})...`;
        }

        if (missingKeys.length > 0) {
          verifySpinner.warn(`Found ${chalk.bold(missingKeys.length.toString())} files missing in target bucket`);

          // Log a sample of missing files (first 10)
          logInfo('\nFiles missing in target bucket (showing first 10):', chalk.yellow);
          const samplesToShow = Math.min(missingKeys.length, 10);
          for (let i = 0; i < samplesToShow; i++) {
            logInfo(`  - ${missingKeys[i]}`, chalk.yellow);
          }

          if (missingKeys.length > 10) {
            logInfo(`  ... and ${missingKeys.length - 10} more files`, chalk.yellow);
          }
          logInfo('', chalk.white);

          // Ask user what to do with missing files
          const { missingAction } = await inquirer.prompt([
            {
              type: 'list',
              name: 'missingAction',
              message: `Found ${missingKeys.length} files missing in target bucket. What would you like to do?`,
              choices: [
                { name: 'Skip these files (only delete files that exist in target)', value: 'skip' },
                { name: 'Delete all files (including those missing in target)', value: 'delete_all' },
                { name: 'Cancel operation', value: 'cancel' }
              ]
            }
          ]);

          if (missingAction === 'cancel') {
            logWarning('Purge operation cancelled by user.');
            return;
          } else if (missingAction === 'skip') {
            // Filter out missing keys
            keysToDelete = sourceKeys.filter(key => !missingKeys.includes(key));
            logWarning(`Will skip ${missingKeys.length} files that are missing in target bucket.`);
            result.missingInTarget = missingKeys.length;
          } else {
            // Keep all keys
            logWarning(`Will delete all ${sourceKeys.length} files, including those missing in target.`);
            result.missingInTarget = missingKeys.length;
          }
        } else {
          verifySpinner.succeed(`All ${chalk.bold(sourceKeys.length.toString())} files verified in target bucket`);
          logSuccess(`All ${sourceKeys.length} files verified in target bucket`);
        }
      }

      // Ask for confirmation (double confirmation)
      if (!config.skipConfirmation) {
        logInfo('', chalk.white);
        logInfo(`Will purge ${chalk.bold(keysToDelete.length.toString())} objects from source bucket:`, chalk.cyan);
        logInfo(`  Source bucket: ${chalk.blue(config.source.bucket)}`, chalk.white);

        if (config.prefix) {
          logInfo(`  Prefix: ${chalk.blue(config.prefix)}`, chalk.white);
        }

        // Sample of files to delete
        if (keysToDelete.length > 0) {
          logInfo('\nFiles to purge (showing first 10):', chalk.cyan);
          const samplesToShow = Math.min(keysToDelete.length, 10);
          for (let i = 0; i < samplesToShow; i++) {
            logInfo(`  - ${keysToDelete[i]}`, chalk.white);
          }

          if (keysToDelete.length > 10) {
            logInfo(`  ... and ${keysToDelete.length - 10} more files`, chalk.white);
          }
        }
        logInfo('', chalk.white);

        // First confirmation
        const { confirmPurge } = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'confirmPurge',
            message: `Do you want to proceed with purging ${chalk.bold(keysToDelete.length.toString())} objects from the source bucket?`,
            default: false
          }
        ]);

        if (!confirmPurge) {
          logWarning('Purge operation cancelled by user.');
          return;
        }

        // Second confirmation
        const { confirmPurgeAgain } = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'confirmPurgeAgain',
            message: chalk.red.bold(`WARNING: This will DELETE ${keysToDelete.length} files from the source bucket. Are you ABSOLUTELY sure?`),
            default: false
          }
        ]);

        if (!confirmPurgeAgain) {
          logWarning('Purge operation cancelled by user.');
          return;
        }
      } else {
        logInfo(`Proceeding with purge of ${keysToDelete.length.toString()} objects (confirmation skipped)`, chalk.yellow);
      }

      // Proceed with deletion
      if (keysToDelete.length === 0) {
        logWarning('No objects to purge after filtering. Exiting.');
        return;
      }

      // Skip actual deletion in dry run mode
      if (config.dryRun) {
        logWarning(`DRY RUN: Would have deleted ${keysToDelete.length} objects from source bucket.`);
        const messageText = `The following ${keysToDelete.length} objects would have been deleted:`;
        logWarning(messageText);

        // Display sample (first 10)
        const samplesToShow = Math.min(keysToDelete.length, 10);
        for (let i = 0; i < samplesToShow; i++) {
          logInfo(`  - ${keysToDelete[i]}`, chalk.gray);
        }

        if (keysToDelete.length > 10) {
          logInfo(`  ... and ${keysToDelete.length - 10} more files`, chalk.gray);
        }
        return;
      }

      // Create a progress bar for deletion
      const progressBar = new cliProgress.SingleBar({
        format: ' {bar} | {percentage}% | {value}/{total} | Deleting files from source bucket',
        clearOnComplete: false,
        hideCursor: true,
      }, cliProgress.Presets.shades_grey);

      progressBar.start(keysToDelete.length, 0);

      // Delete objects in batches
      const batchSize = 1000; // AWS allows max 1000 keys per delete operation
      let processedCount = 0;

      for (let i = 0; i < keysToDelete.length; i += batchSize) {
        const batch = keysToDelete.slice(i, i + batchSize);

        try {
          const result = await deleteObjects(sourceClient, config.source.bucket, batch);
          processedCount += batch.length;
          progressBar.update(processedCount);

          result.deleted.forEach(key => {
            logVerbose(`Deleted: ${key}`);
          });

          result.failed.forEach(key => {
            logError(`Failed to delete: ${key}`);
          });
        } catch (error) {
          logError(`Error deleting batch of objects: ${(error as Error).message}`);
        }
      }

      progressBar.stop();

      // Compute final stats
      result.endTime = Date.now();

      // Print summary
      const elapsedTime = Math.floor((result.endTime - result.startTime) / 1000);
      const elapsedTimeStr = formatTime(elapsedTime);

      logInfo('', chalk.white);
      logInfo('Purge Operation Summary', chalk.cyanBright.bold);
      logInfo('─'.repeat(50), chalk.white);
      logInfo(`Total objects:      ${result.total.toString()}`, chalk.white);
      logInfo(`Deleted:            ${(result.total - result.missingInTarget).toString()}`, chalk.green);

      if (result.missingInTarget > 0) {
        logInfo(`Skipped (missing):  ${result.missingInTarget.toString()}`, chalk.yellow);
      }

      logInfo(`Total time:         ${elapsedTimeStr}`, chalk.white);
      logInfo('', chalk.white);

      logSuccess('Source bucket purge completed successfully!');
    } catch (error) {
      spinner.fail(`Error listing objects: ${(error as Error).message}`);
      logError(`Failed to list objects in source bucket`, error as Error);
      process.exit(1);
    }
  } catch (error) {
    logError('Error running source bucket purge', error as Error);
    process.exit(1);
  }
}
