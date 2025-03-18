// Third-party dependencies
import chalk from 'chalk';
import cliProgress from 'cli-progress';
import inquirer from 'inquirer';
import ora from 'ora';

// Local imports
import { abortMultipartUpload, createS3Client, listMultipartUploads } from './s3-client';
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
import type { MigrationConfig } from './types';

interface CleanupResult {
  totalFound: number;
  aborted: number;
  failed: number;
  startTime: number;
  endTime?: number;
}

/**
 * Function to clean incomplete multipart uploads from target bucket
 */
export async function cleanTargetBucket(config: MigrationConfig): Promise<void> {
  // Initialize logger
  initLogger(config);

  // Create S3 client for target
  const targetClient = createS3Client(config.target);

  try {
    logInfo('Starting target bucket cleanup...', chalk.cyanBright);
    logInfo(`Target: ${config.target.endpoint}/${config.target.bucket}`, chalk.cyan);

    if (config.prefix) {
      logInfo(`Prefix: ${config.prefix}`, chalk.cyan);
    }

    if (config.dryRun) {
      logWarning('DRY RUN MODE - No incomplete uploads will be aborted');
      log(LogLevel.WARNING, 'DRY RUN MODE - No incomplete uploads will be aborted');
    }

    // List all in-progress multipart uploads
    const spinner = ora('Listing incomplete multipart uploads in target bucket...').start();

    try {
      const multipartUploads = await listMultipartUploads(
        targetClient,
        config.target.bucket,
        config.prefix
      );

      if (multipartUploads.length === 0) {
        spinner.succeed('No incomplete multipart uploads found in target bucket.');
        logSuccess('No incomplete multipart uploads found in target bucket.');
        return;
      }

      spinner.succeed(`Found ${chalk.bold(multipartUploads.length.toString())} incomplete multipart uploads in target bucket`);
      logSuccess(`Found ${multipartUploads.length} incomplete multipart uploads in target bucket`);

      // Initialize cleanup result
      const result: CleanupResult = {
        totalFound: multipartUploads.length,
        aborted: 0,
        failed: 0,
        startTime: Date.now()
      };

      // Display information about the uploads
      logInfo('\nIncomplete multipart uploads (showing first 10):', chalk.cyan);

      const samplesToShow = Math.min(multipartUploads.length, 10);
      for (let i = 0; i < samplesToShow; i++) {
        const upload = multipartUploads[i];
        const initiatedDate = upload.Initiated
          ? upload.Initiated.toISOString().split('T')[0]
          : 'Unknown';

        logInfo(`  - ${upload.Key} (ID: ${upload.UploadId.substring(0, 8)}..., Started: ${initiatedDate})`, chalk.white);
      }

      if (multipartUploads.length > 10) {
        logInfo(`  ... and ${multipartUploads.length - 10} more uploads`, chalk.white);
      }
      logInfo('', chalk.white);

      // Ask for confirmation (double confirmation)
      if (!config.skipConfirmation) {
        // First confirmation
        const { confirmCleanup } = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'confirmCleanup',
            message: `Do you want to proceed with aborting ${chalk.bold(multipartUploads.length.toString())} incomplete multipart uploads?`,
            default: false
          }
        ]);

        if (!confirmCleanup) {
          logWarning('Cleanup operation cancelled by user.');
          return;
        }

        // Second confirmation
        const { confirmCleanupAgain } = await inquirer.prompt([
          {
            type: 'confirm',
            name: 'confirmCleanupAgain',
            message: chalk.red.bold(`WARNING: This will ABORT ${multipartUploads.length} incomplete uploads from the target bucket. Are you ABSOLUTELY sure?`),
            default: false
          }
        ]);

        if (!confirmCleanupAgain) {
          logWarning('Cleanup operation cancelled by user.');
          return;
        }
      } else {
        logInfo(`Proceeding with aborting ${multipartUploads.length.toString()} incomplete uploads (confirmation skipped)`, chalk.yellow);
      }

      // Skip actual abortion in dry run mode
      if (config.dryRun) {
        logWarning(`DRY RUN: Would have aborted ${multipartUploads.length} incomplete multipart uploads.`);
        return;
      }

      // Create a progress bar for abortion
      const progressBar = new cliProgress.SingleBar({
        format: ' {bar} | {percentage}% | {value}/{total} | Aborting incomplete multipart uploads',
        clearOnComplete: false,
        hideCursor: true,
      }, cliProgress.Presets.shades_grey);

      progressBar.start(multipartUploads.length, 0);

      // Abort each upload individually
      let aborted = 0;
      let failed = 0;

      for (const upload of multipartUploads) {
        try {
          const success = await abortMultipartUpload(
            targetClient,
            config.target.bucket,
            upload.Key,
            upload.UploadId
          );

          if (success) {
            aborted++;
            result.aborted++;
            logVerbose(`Successfully aborted upload for: ${upload.Key} (ID: ${upload.UploadId.substring(0, 8)}...)`);
          } else {
            failed++;
            result.failed++;
            logError(`Failed to abort upload for: ${upload.Key} (ID: ${upload.UploadId.substring(0, 8)}...)`);
          }
        } catch (error) {
          failed++;
          result.failed++;
          logError(`Error aborting upload for ${upload.Key}: ${(error as Error).message}`);
        }

        progressBar.update(aborted + failed);
      }

      progressBar.stop();

      // Compute final stats
      result.endTime = Date.now();

      // Print summary
      const elapsedTime = Math.floor((result.endTime - result.startTime) / 1000);
      const elapsedTimeStr = formatTime(elapsedTime);

      logInfo('', chalk.white);
      logInfo('Cleanup Operation Summary', chalk.cyanBright.bold);
      logInfo('─'.repeat(50), chalk.white);
      logInfo(`Total found:      ${result.totalFound.toString()}`, chalk.white);
      logInfo(`Aborted:          ${result.aborted.toString()}`, chalk.green);

      if (result.failed > 0) {
        logInfo(`Failed:           ${result.failed.toString()}`, chalk.red);
      }

      logInfo(`Total time:       ${elapsedTimeStr}`, chalk.white);
      logInfo('', chalk.white);

      if (result.failed > 0) {
        logWarning('Target bucket cleanup completed with some failures.');
      } else {
        logSuccess('Target bucket cleanup completed successfully!');
      }
    } catch (error) {
      spinner.fail(`Error listing incomplete multipart uploads: ${(error as Error).message}`);
      logError(`Failed to list incomplete multipart uploads`, error as Error);
      process.exit(1);
    }
  } catch (error) {
    logError('Error running target bucket cleanup', error as Error);
    process.exit(1);
  }
}
