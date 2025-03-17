import type { S3Client } from '@aws-sdk/client-s3';
import chalk from 'chalk';
import cliProgress from 'cli-progress';
import ora from 'ora';
import inquirer from 'inquirer';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as fsExtra from 'fs-extra';
import type { MigrationConfig, FileTransfer } from './types';
import { TransferStatus, TransferMode } from './types';
import {
  createS3Client,
  downloadObject,
  getObjectSize,
  listAllObjects,
  uploadObject,
  objectExists,
  deleteObjects
} from './s3-client';
import {
  initLogger,
  closeLogger,
  log,
  logError,
  logVerbose,
  logSuccess,
  logWarning,
  LogLevel
} from './logger';

/**
 * Migration statistics
 */
interface MigrationStats {
  total: number;
  processed: number;
  succeeded: number;
  failed: number;
  skipped: number;
  verificationFailed: number;
  startTime: number;
  failedFiles: FileTransfer[];
  paused: boolean;
  endTime?: number;
  transfersByMode?: FileTransfer[];
}

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i];
}

/**
 * Format speed to human-readable string
 */
function formatSpeed(bytesPerSecond: number): string {
  return `${formatBytes(bytesPerSecond)}/s`;
}

/**
 * 获取系统可用内存（字节）
 */
function getAvailableMemory(): number {
  return os.freemem();
}

/**
 * 自动确定文件传输模式
 */
function determineTransferMode(fileSize: number, config: MigrationConfig): TransferMode {
  // 如果配置中已经指定了传输模式，则使用指定的模式
  if (config.transferMode && config.transferMode !== TransferMode.AUTO) {
    return config.transferMode;
  }

  // 获取可用内存
  const availableMemory = getAvailableMemory();
  // 大文件阈值，默认为可用内存的50%
  const largeFileThreshold = config.largeFileSizeThreshold || (availableMemory * 0.5);

  // 如果文件小于阈值，使用内存模式
  if (fileSize < largeFileThreshold) {
    return TransferMode.MEMORY;
  }

  // 文件很大（接近或超过可用内存），使用磁盘或流模式
  // 如果文件大于可用内存的95%，建议使用流模式
  if (fileSize > availableMemory * 0.95) {
    return TransferMode.STREAM;
  }

  // 否则使用磁盘模式
  return TransferMode.DISK;
}

/**
 * Run the migration process
 */
export async function runMigration(config: MigrationConfig): Promise<void> {
  // Initialize logger
  initLogger(config);

  try {
    console.log(chalk.blue.bold('Starting S3 migration...'));
    log(LogLevel.INFO, 'Starting S3 migration...');
    console.log(chalk.gray('Source:'), chalk.yellow(`${config.source.endpoint}/${config.source.bucket}`));
    console.log(chalk.gray('Target:'), chalk.yellow(`${config.target.endpoint}/${config.target.bucket}`));
    console.log(chalk.gray('Concurrency:'), chalk.yellow(config.concurrency));
    console.log(chalk.gray('Max Retries:'), chalk.yellow(config.maxRetries));
    console.log(chalk.gray('Verify After Migration:'), chalk.yellow(config.verifyAfterMigration));
    console.log(chalk.gray('Purge Source After Migration:'), chalk.yellow(config.purgeSourceAfterMigration));

    // Initialize transfer mode if not already set
    if (!config.transferMode) {
      config.transferMode = TransferMode.AUTO;
    }
    console.log(chalk.gray('Transfer Mode:'), chalk.yellow(config.transferMode));

    // Display available memory
    const availableMemory = getAvailableMemory();
    console.log(chalk.gray('Available Memory:'), chalk.yellow(formatBytes(availableMemory)));

    // Initialize temp directory if disk mode is possible
    if (config.transferMode === TransferMode.AUTO || config.transferMode === TransferMode.DISK) {
      // Use provided temp directory or create a default one
      config.tempDir = config.tempDir || path.join(process.cwd(), 'tmp');
      console.log(chalk.gray('Temp Directory:'), chalk.yellow(config.tempDir));

      // Make sure temp directory exists
      await fsExtra.ensureDir(config.tempDir);
    }

    // Only display prefix if it's provided
    if (config.prefix) {
      console.log(chalk.gray('Prefix:'), chalk.yellow(config.prefix));
    }

    // Display include patterns if provided
    if (config.include && config.include.length > 0) {
      console.log(chalk.gray('Include patterns:'), chalk.yellow(config.include.join(', ')));
    }

    // Display exclude patterns if provided
    if (config.exclude && config.exclude.length > 0) {
      console.log(chalk.gray('Exclude patterns:'), chalk.yellow(config.exclude.join(', ')));
    }

    if (config.dryRun) {
      console.log(chalk.yellow.bold('DRY RUN MODE - No files will be transferred'));
      log(LogLevel.WARNING, 'DRY RUN MODE - No files will be transferred');
    }

    // Create S3 clients
    const sourceClient = createS3Client(config.source);
    const targetClient = createS3Client(config.target);

    // Initialize stats
    const stats: MigrationStats = {
      total: 0,
      processed: 0,
      succeeded: 0,
      failed: 0,
      skipped: 0,
      verificationFailed: 0,
      startTime: Date.now(),
      failedFiles: [],
      paused: false,
      transfersByMode: []
    };

    // List all objects
    const spinner = ora('Listing objects from source bucket...').start();
    let allKeys: string[] = [];

    try {
      // Use prefix only if provided
      for await (const keys of listAllObjects(sourceClient, config.source.bucket, config.prefix || undefined)) {
        allKeys.push(...keys);
      }

      // Apply include filters only if provided
      if (config.include && config.include.length > 0) {
        try {
          const patterns = config.include.map(pattern => new RegExp(pattern));
          allKeys = allKeys.filter(key => patterns.some(pattern => pattern.test(key)));
        } catch (error) {
          logError(`Invalid include pattern`, error as Error);
          spinner.fail(`Invalid include pattern: ${(error as Error).message}`);
          process.exit(1);
        }
      }

      // Apply exclude filters only if provided
      if (config.exclude && config.exclude.length > 0) {
        try {
          const patterns = config.exclude.map(pattern => new RegExp(pattern));
          allKeys = allKeys.filter(key => !patterns.some(pattern => pattern.test(key)));
        } catch (error) {
          logError(`Invalid exclude pattern`, error as Error);
          spinner.fail(`Invalid exclude pattern: ${(error as Error).message}`);
          process.exit(1);
        }
      }

      stats.total = allKeys.length;

      spinner.succeed(`Found ${chalk.bold(stats.total.toString())} objects to migrate`);
      logSuccess(`Found ${stats.total} objects to migrate`);
    } catch (error) {
      logError(`Failed to list objects`, error as Error);
      spinner.fail(`Failed to list objects: ${(error as Error).message}`);
      process.exit(1);
    }

    if (stats.total === 0) {
      console.log(chalk.yellow('No objects to migrate. Exiting.'));
      return;
    }

    // Create file transfer objects
    const transfers: FileTransfer[] = allKeys.map(key => ({
      key,
      status: TransferStatus.PENDING,
      retryCount: 0,
      progress: 0
    }));

    // After listing and filtering objects, check for large files
    if (stats.total > 0 && config.transferMode === TransferMode.AUTO) {
      // Find the largest file
      let largestFileSize = 0;
      let largestFile = '';

      for (const transfer of transfers) {
        // Get file size if not already known
        if (!transfer.size) {
          try {
            transfer.size = await getObjectSize(sourceClient, config.source.bucket, transfer.key);
          } catch (error) {
            logError(`Failed to get size for ${transfer.key}`, error as Error);
            continue;
          }
        }

        if (transfer.size && transfer.size > largestFileSize) {
          largestFileSize = transfer.size;
          largestFile = transfer.key;
        }
      }

      // Calculate suggested transfer mode based on the largest file
      const suggestedMode = determineTransferMode(largestFileSize, config);

      // If largest file exceeds memory threshold, suggest appropriate transfer mode
      if (suggestedMode !== TransferMode.MEMORY) {
        console.log('');
        console.log(chalk.yellow('Large file detected:'), chalk.blue(largestFile));
        console.log(chalk.yellow('Size:'), chalk.blue(formatBytes(largestFileSize)));
        console.log(chalk.yellow('Available memory:'), chalk.blue(formatBytes(availableMemory)));

        if (!config.skipConfirmation) {
          const { selectedMode } = await inquirer.prompt([
            {
              type: 'list',
              name: 'selectedMode',
              message: 'Choose a transfer mode for large files:',
              choices: [
                { name: `Memory (fastest, but may cause out-of-memory for files > ${formatBytes(availableMemory * 0.5)})`, value: TransferMode.MEMORY },
                { name: `Disk (good balance, uses temp files in ${config.tempDir})`, value: TransferMode.DISK },
                { name: 'Stream (slowest, but uses minimal memory)', value: TransferMode.STREAM },
                { name: 'Auto (choose best mode based on file size)', value: TransferMode.AUTO }
              ],
              default: suggestedMode
            }
          ]);

          config.transferMode = selectedMode;
          console.log(chalk.blue(`Using transfer mode: ${config.transferMode}`));
          log(LogLevel.INFO, `Transfer mode selected: ${config.transferMode}`);
        } else {
          // If confirmation is skipped, use the suggested mode
          config.transferMode = suggestedMode;
          console.log(chalk.blue(`Automatically selected transfer mode: ${config.transferMode}`));
          log(LogLevel.INFO, `Transfer mode automatically selected: ${config.transferMode}`);
        }
      }
    }

    // Show sample of files to migrate (first 10 files)
    if (transfers.length > 0) {
      console.log(chalk.cyan('\nFiles to migrate (showing first 10):'));
      const samplesToShow = Math.min(transfers.length, 10);
      for (let i = 0; i < samplesToShow; i++) {
        console.log(chalk.gray(`  - ${transfers[i].key}`));
      }

      if (transfers.length > 10) {
        console.log(chalk.gray(`  ... and ${transfers.length - 10} more files`));
      }
      console.log('');
    }

    // Ask user for confirmation before proceeding (unless skipConfirmation is true)
    if (!config.skipConfirmation) {
      const { confirmMigration } = await inquirer.prompt([
        {
          type: 'confirm',
          name: 'confirmMigration',
          message: `Do you want to proceed with migrating ${chalk.bold(stats.total.toString())} objects?`,
          default: false
        }
      ]);

      if (!confirmMigration) {
        console.log(chalk.yellow('Migration cancelled by user.'));
        return;
      }
    } else {
      console.log(chalk.blue(`Proceeding with migration of ${chalk.bold(stats.total.toString())} objects (confirmation skipped)`));
    }

    // Create progress bar
    const multibar = new cliProgress.MultiBar({
      clearOnComplete: false,
      hideCursor: true,
      format: ' {bar} | {percentage}% | {value}/{total} | {status} | {file} | {speed}',
    }, cliProgress.Presets.shades_grey);

    // Create overall progress bar
    const overallBar = multibar.create(stats.total, 0, {
      status: chalk.blue('In Progress'),
      file: 'Overall Progress',
      speed: '',
      percentage: 0
    });

    // Process objects in batches with concurrency
    const concurrency = config.concurrency || 5;
    const maxRetries = config.maxRetries || 3;

    // Create file progress bars (one for each concurrent transfer)
    const fileProgressBars: { [key: string]: cliProgress.SingleBar } = {};

    // Start migration
    console.log(chalk.blue(`Starting migration with concurrency of ${chalk.bold(concurrency.toString())}`));
    console.log('');

    // Process files
    let currentIndex = 0;

    while (currentIndex < transfers.length) {
      if (stats.paused) {
        // Ask user what to do with failed files
        const { action } = await inquirer.prompt([
          {
            type: 'list',
            name: 'action',
            message: 'Migration paused due to failures. What would you like to do?',
            choices: [
              { name: 'Skip failed files and continue', value: 'skip' },
              { name: 'Retry failed files', value: 'retry' },
              { name: 'Stop migration', value: 'stop' }
            ]
          }
        ]);

        if (action === 'stop') {
          console.log(chalk.yellow('Migration stopped by user.'));
          break;
        } else if (action === 'skip') {
          // Mark all failed files as skipped
          for (const file of stats.failedFiles) {
            file.status = TransferStatus.SKIPPED;
            stats.skipped++;
            stats.failed--;
          }
          stats.failedFiles = [];
          stats.paused = false;
        } else if (action === 'retry') {
          // Reset retry count and status for failed files
          for (const file of stats.failedFiles) {
            file.status = TransferStatus.PENDING;
            file.retryCount = 0;
          }
          stats.failedFiles = [];
          stats.paused = false;
        }
      }

      // Get next batch of files to process
      const endIndex = Math.min(currentIndex + concurrency, transfers.length);
      const batch = transfers.slice(currentIndex, endIndex);

      // Process batch
      await Promise.all(
        batch.map(async (transfer) => {
          // Skip already processed files
          if (
            transfer.status === TransferStatus.SUCCEEDED ||
            transfer.status === TransferStatus.SKIPPED
          ) {
            return;
          }

          // Create progress bar for this file if it doesn't exist
          if (!fileProgressBars[transfer.key]) {
            fileProgressBars[transfer.key] = multibar.create(100, 0, {
              status: chalk.blue('Pending'),
              file: transfer.key.length > 30 ? '...' + transfer.key.slice(-30) : transfer.key,
              speed: '',
              percentage: 0
            });
          }

          const progressBar = fileProgressBars[transfer.key];

          try {
            // Get file size
            if (!transfer.size) {
              progressBar.update(0, {
                status: chalk.blue('Getting size'),
                speed: ''
              });

              transfer.size = await getObjectSize(sourceClient, config.source.bucket, transfer.key);
            }

            // Process the file
            await processObject(
              sourceClient,
              targetClient,
              config,
              transfer,
              progressBar,
              stats,
              maxRetries
            );

            // Update overall progress
            stats.processed++;
            overallBar.update(stats.processed, {
              status: getOverallStatus(stats),
              percentage: Math.floor((stats.processed / stats.total) * 100)
            });

            // Remove progress bar for this file
            if (progressBar) {
              multibar.remove(progressBar);
              delete fileProgressBars[transfer.key];
            }

            // 记录传输模式
            if (!stats.transfersByMode) {
              stats.transfersByMode = [];
            }
            stats.transfersByMode.push({ ...transfer });
          } catch (error) {
            // This should not happen as errors are handled in processObject
            console.error(chalk.red(`Unexpected error: ${(error as Error).message}`));
          }
        })
      );

      // Move to next batch if not paused
      if (!stats.paused) {
        currentIndex = endIndex;
      }
    }

    // Stop all progress bars
    multibar.stop();
    console.log('\n');

    // Set end time
    stats.endTime = Date.now();

    // Verify files in target bucket if enabled
    if (config.verifyAfterMigration && !config.dryRun && stats.succeeded > 0) {
      await verifyMigratedFiles(targetClient, config.target.bucket, transfers, stats);
    }

    // Print final summary
    printSummary(stats);

    // Handle failed files
    if (stats.failed > 0 || stats.verificationFailed > 0) {
      await handleFailedFiles(sourceClient, targetClient, config, transfers, stats);
    } else if (stats.succeeded === stats.total) {
      // All files migrated successfully, ask if user wants to purge source
      if (config.purgeSourceAfterMigration && !config.dryRun) {
        await handlePurgeSource(sourceClient, config.source.bucket, transfers);
      }
    }
  } catch (error) {
    // Log any uncaught errors
    logError('Uncaught error during migration', error as Error);
    throw error;
  } finally {
    // Close logger when migration is complete
    closeLogger();
  }
}

/**
 * Verify that all migrated files exist in the target bucket
 */
async function verifyMigratedFiles(
  targetClient: S3Client,
  targetBucket: string,
  transfers: FileTransfer[],
  stats: MigrationStats
): Promise<void> {
  console.log(chalk.blue('Verifying migrated files in target bucket...'));
  log(LogLevel.INFO, 'Verifying migrated files in target bucket...');

  // Only verify files that were marked as succeeded
  const successfulTransfers = transfers.filter(t => t.status === TransferStatus.SUCCEEDED);
  logVerbose(`Starting verification of ${successfulTransfers.length} files`);

  let verified = 0;
  let failed = 0;

  // Create a progress bar
  const verifyBar = new cliProgress.SingleBar({
    format: ' {bar} | {percentage}% | Verifying: {value}/{total} | ' +
      chalk.green('{verified}') + '/' + chalk.red('{failed}') + ' | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
  }, cliProgress.Presets.shades_classic);

  verifyBar.start(successfulTransfers.length, 0, {
    verified: 0,
    failed: 0,
    file: ''
  });

  let i = 0;
  for (const transfer of successfulTransfers) {
    try {
      logVerbose(`Verifying file: ${transfer.key}`);
      const exists = await objectExists(targetClient, targetBucket, transfer.key);

      if (exists) {
        transfer.verified = true;
        verified++;
        logVerbose(`File verified successfully: ${transfer.key}`);
      } else {
        transfer.verified = false;
        transfer.status = TransferStatus.VERIFICATION_FAILED;
        transfer.error = 'File not found in target bucket after migration';
        stats.verificationFailed++;
        stats.succeeded--;
        stats.failedFiles.push(transfer);
        failed++;
        logError(`Verification failed - file not found in target: ${transfer.key}`);
      }

      // Update progress bar
      i++;
      verifyBar.update(i, {
        verified: verified,
        failed: failed,
        file: transfer.key.length > 30 ? '...' + transfer.key.slice(-30) : transfer.key
      });
    } catch (error) {
      transfer.verified = false;
      transfer.status = TransferStatus.VERIFICATION_FAILED;
      transfer.error = `Verification error: ${(error as Error).message}`;
      stats.verificationFailed++;
      stats.succeeded--;
      stats.failedFiles.push(transfer);
      failed++;
      logError(`Error during verification of file: ${transfer.key}`, error as Error);
    }
  }

  verifyBar.update(successfulTransfers.length, {
    verified: verified,
    failed: failed,
    file: ''
  });
  verifyBar.stop();

  if (failed > 0) {
    console.log(chalk.red(`Verification completed with ${failed} failures`));
    logWarning(`Verification completed with ${failed} failures`);
  } else {
    console.log(chalk.green(`All ${verified} files verified successfully`));
    logSuccess(`All ${verified} files verified successfully`);
  }
}

/**
 * Handle failed files after migration
 */
async function handleFailedFiles(
  sourceClient: S3Client,
  targetClient: S3Client,
  config: MigrationConfig,
  transfers: FileTransfer[],
  stats: MigrationStats
): Promise<void> {
  const totalFailed = stats.failed + stats.verificationFailed;

  const { retry } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'retry',
      message: `${totalFailed} files failed to migrate. Do you want to retry?`,
      default: true
    }
  ]);

  if (retry) {
    console.log(chalk.blue('Retrying failed files...'));
    log(LogLevel.INFO, `Retrying ${totalFailed} failed files.`);

    // Reset failed files status
    for (const file of stats.failedFiles) {
      file.status = TransferStatus.PENDING;
      file.retryCount = 0;
      file.verified = undefined;
    }

    // Create a new config with only failed files
    const retryConfig = { ...config };
    const failedKeys = stats.failedFiles.map(f => f.key);

    // Filter transfers to only include failed files
    const retryTransfers = transfers.filter(t => failedKeys.includes(t.key));

    // Run migration again with only failed files
    await runMigrationForFiles(sourceClient, targetClient, retryConfig, retryTransfers);
  }
}

/**
 * Run migration for specific files
 */
async function runMigrationForFiles(
  sourceClient: S3Client,
  targetClient: S3Client,
  config: MigrationConfig,
  transfers: FileTransfer[]
): Promise<void> {
  // Initialize stats
  const stats: MigrationStats = {
    total: transfers.length,
    processed: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    verificationFailed: 0,
    startTime: Date.now(),
    failedFiles: [],
    paused: false,
    transfersByMode: []
  };

  log(LogLevel.INFO, `Starting migration of ${transfers.length} files from ${config.source.bucket} to ${config.target.bucket}`);

  // Create progress bar
  const multibar = new cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: ' {bar} | {percentage}% | {value}/{total} | {status} | {file} | {speed}',
  }, cliProgress.Presets.shades_grey);

  // Create overall progress bar
  const overallBar = multibar.create(stats.total, 0, {
    status: chalk.blue('In Progress'),
    file: 'Retry Progress',
    speed: '',
    percentage: 0
  });

  // Process objects in batches with concurrency
  const concurrency = config.concurrency || 5;
  const maxRetries = config.maxRetries || 3;

  // Create file progress bars (one for each concurrent transfer)
  const fileProgressBars: { [key: string]: cliProgress.SingleBar } = {};

  // Start migration
  console.log(chalk.blue(`Starting retry with concurrency of ${chalk.bold(concurrency.toString())}`));
  console.log('');

  // Process files in batches
  for (let i = 0; i < transfers.length; i += concurrency) {
    const batch = transfers.slice(i, Math.min(i + concurrency, transfers.length));

    await Promise.all(
      batch.map(async (transfer) => {
        // Create progress bar for this file
        if (!fileProgressBars[transfer.key]) {
          fileProgressBars[transfer.key] = multibar.create(100, 0, {
            status: chalk.blue('Pending'),
            file: transfer.key.length > 30 ? '...' + transfer.key.slice(-30) : transfer.key,
            speed: '',
            percentage: 0
          });
        }

        const progressBar = fileProgressBars[transfer.key];

        try {
          // Get file size if not already known
          if (!transfer.size) {
            progressBar.update(0, {
              status: chalk.blue('Getting size'),
              speed: ''
            });

            transfer.size = await getObjectSize(sourceClient, config.source.bucket, transfer.key);
          }

          // Process the file
          await processObject(
            sourceClient,
            targetClient,
            config,
            transfer,
            progressBar,
            stats,
            maxRetries
          );

          // Update overall progress
          stats.processed++;
          overallBar.update(stats.processed, {
            status: getOverallStatus(stats),
            percentage: Math.floor((stats.processed / stats.total) * 100)
          });

          // Remove progress bar for this file
          if (progressBar) {
            multibar.remove(progressBar);
            delete fileProgressBars[transfer.key];
          }
        } catch (error) {
          console.error(chalk.red(`Unexpected error: ${(error as Error).message}`));
        }
      })
    );
  }

  // Stop all progress bars
  multibar.stop();
  console.log('\n');

  // Set end time
  stats.endTime = Date.now();

  // Verify files in target bucket if enabled
  if (config.verifyAfterMigration && !config.dryRun && stats.succeeded > 0) {
    await verifyMigratedFiles(targetClient, config.target.bucket, transfers, stats);
  }

  // Print final summary
  printSummary(stats);

  // Handle failed files
  if (stats.failed > 0 || stats.verificationFailed > 0) {
    await handleFailedFiles(sourceClient, targetClient, config, transfers, stats);
  }
}

/**
 * Handle purging source bucket after successful migration
 */
async function handlePurgeSource(
  sourceClient: S3Client,
  sourceBucket: string,
  transfers: FileTransfer[]
): Promise<void> {
  // Get successfully migrated files
  const successfulTransfers = transfers.filter(t =>
    t.status === TransferStatus.SUCCEEDED && t.verified === true
  );

  if (successfulTransfers.length === 0) {
    log(LogLevel.INFO, 'No files to purge from source bucket');
    return;
  }

  log(LogLevel.INFO, `Preparing to purge ${successfulTransfers.length} files from source bucket`);

  const { confirmPurge } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'confirmPurge',
      message: `Do you want to delete ${successfulTransfers.length} successfully migrated files from the source bucket?`,
      default: false
    }
  ]);

  if (!confirmPurge) {
    console.log(chalk.yellow('Source bucket purge cancelled.'));
    log(LogLevel.INFO, 'Source bucket purge cancelled by user');
    return;
  }

  // Double confirm
  const { confirmAgain } = await inquirer.prompt([
    {
      type: 'confirm',
      name: 'confirmAgain',
      message: chalk.red.bold(`WARNING: This will permanently delete ${successfulTransfers.length} files from the source bucket. Are you absolutely sure?`),
      default: false
    }
  ]);

  if (!confirmAgain) {
    console.log(chalk.yellow('Source bucket purge cancelled.'));
    log(LogLevel.INFO, 'Source bucket purge cancelled by user on second confirmation');
    return;
  }

  // Proceed with deletion
  console.log(chalk.blue(`Deleting ${successfulTransfers.length} files from source bucket...`));
  log(LogLevel.INFO, `Starting deletion of ${successfulTransfers.length} files from source bucket`);

  // Create progress bar
  const purgeBar = new cliProgress.SingleBar({
    format: ' {bar} | {percentage}% | {value}/{total} | {status}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  // Initialize the progress bar
  purgeBar.start(successfulTransfers.length, 0, {
    status: chalk.blue('Deleting files...')
  });

  const keys = successfulTransfers.map(t => t.key);
  let deletedCount = 0;
  let failedCount = 0;

  try {
    const result = await deleteObjects(sourceClient, sourceBucket, keys,
      (deleted, total) => {
        // Update progress bar
        purgeBar.update(deleted, {
          status: chalk.blue(`Deleting files... ${deleted}/${total}`)
        });
        deletedCount = deleted;
      }
    );

    // Complete the progress bar
    purgeBar.update(successfulTransfers.length, {
      status: chalk.green('Deletion completed')
    });
    purgeBar.stop();

    if (result.failed.length > 0) {
      failedCount = result.failed.length;
      console.log(chalk.yellow(`\nDeletion completed with ${failedCount} failures`));
      console.log(chalk.yellow('Failed to delete the following files:'));
      logWarning(`Deletion completed with ${failedCount} failures`);

      // Log failed files
      for (const key of result.failed) {
        console.log(chalk.red(`  - ${key}`));
        logError(`Failed to delete file: ${key}`);
      }
    } else {
      console.log(chalk.green(`\nSuccessfully deleted ${result.deleted.length} files from source bucket`));
      logSuccess(`Successfully deleted ${result.deleted.length} files from source bucket`);
    }
  } catch (error) {
    // Stop the progress bar in case of error
    purgeBar.stop();
    console.log(chalk.red(`\nFailed to delete files: ${(error as Error).message}`));
    logError(`Failed to delete files from source bucket`, error as Error);
  }

  // Print summary
  console.log(chalk.blue.bold('\nPurge Summary'));
  console.log(chalk.blue('─'.repeat(50)));
  console.log(chalk.gray('Total files:      '), chalk.white(successfulTransfers.length.toString()));
  console.log(chalk.gray('Deleted:          '), chalk.green(deletedCount.toString()));
  console.log(chalk.gray('Failed:           '), chalk.red(failedCount.toString()));

  log(LogLevel.INFO, `Purge Summary - Total: ${successfulTransfers.length}, Deleted: ${deletedCount}, Failed: ${failedCount}`);

  if (deletedCount === successfulTransfers.length) {
    console.log(chalk.green.bold('\n✓ Source bucket purge completed successfully!'));
    logSuccess('Source bucket purge completed successfully!');
  } else if (deletedCount > 0) {
    console.log(chalk.yellow.bold('\n⚠ Source bucket purge completed with some issues.'));
    logWarning('Source bucket purge completed with some issues.');
  } else {
    console.log(chalk.red.bold('\n✗ Source bucket purge failed!'));
    logError('Source bucket purge failed!');
  }
}

/**
 * Get overall status text based on stats
 */
function getOverallStatus(stats: MigrationStats): string {
  if (stats.paused) {
    return chalk.yellow('Paused');
  } else if (stats.failed > 0) {
    return chalk.yellow('Warning');
  } else {
    return chalk.blue('In Progress');
  }
}

/**
 * Process a single object
 */
async function processObject(
  sourceClient: S3Client,
  targetClient: S3Client,
  config: MigrationConfig,
  transfer: FileTransfer,
  progressBar: cliProgress.SingleBar,
  stats: MigrationStats,
  maxRetries: number
): Promise<void> {
  // Skip if already processed
  if (
    transfer.status === TransferStatus.SUCCEEDED ||
    transfer.status === TransferStatus.SKIPPED
  ) {
    return;
  }

  // Skip if in dry run mode
  if (config.dryRun) {
    transfer.status = TransferStatus.SKIPPED;
    progressBar.update(100, {
      status: chalk.yellow('Skipped (dry run)'),
      speed: ''
    });
    stats.skipped++;
    logVerbose(`Skipped file in dry run mode: ${transfer.key}`);
    return;
  }

  try {
    // Determine transfer mode based on file size
    if (!transfer.transferMode && transfer.size) {
      transfer.transferMode = determineTransferMode(transfer.size, config);
      logVerbose(`Selected transfer mode for ${transfer.key}: ${transfer.transferMode}`);
    }

    // Create temp directory if needed
    let tempDir = config.tempDir;
    if (transfer.transferMode === TransferMode.DISK && !tempDir) {
      tempDir = path.join(process.cwd(), 'tmp');
      await fsExtra.ensureDir(tempDir);
    }

    // Download from source
    transfer.status = TransferStatus.DOWNLOADING;
    progressBar.update(0, {
      status: chalk.blue(`Downloading (${transfer.transferMode})`),
      speed: ''
    });
    logVerbose(`Starting download of ${transfer.key} using ${transfer.transferMode} mode`);

    const data = await downloadObject(
      sourceClient,
      config.source.bucket,
      transfer.key,
      (downloaded, total, speed) => {
        const progress = Math.floor((downloaded / total) * 50); // 0-50 for download
        transfer.progress = progress;
        transfer.downloadSpeed = speed;

        progressBar.update(progress, {
          status: chalk.blue(`Downloading (${transfer.transferMode})`),
          speed: formatSpeed(speed)
        });
      },
      transfer.transferMode,
      tempDir
    );

    // Upload to target
    transfer.status = TransferStatus.UPLOADING;
    progressBar.update(50, {
      status: chalk.blue(`Uploading (${transfer.transferMode})`),
      speed: ''
    });
    logVerbose(`Starting upload of ${transfer.key} to target using ${transfer.transferMode} mode`);

    // If using disk mode, store the temporary file path
    if (typeof data === 'string') {
      transfer.tempFilePath = data;
    }

    await uploadObject(
      targetClient,
      config.target.bucket,
      transfer.key,
      data,
      (uploaded, total, speed) => {
        const progress = 50 + Math.floor((uploaded / total) * 50); // 50-100 for upload
        transfer.progress = progress;
        transfer.uploadSpeed = speed;

        progressBar.update(progress, {
          status: chalk.blue(`Uploading (${transfer.transferMode})`),
          speed: formatSpeed(speed)
        });
      },
      transfer.transferMode === TransferMode.DISK // 如果是磁盘模式，上传后删除临时文件
    );

    // Mark as succeeded
    transfer.status = TransferStatus.SUCCEEDED;
    progressBar.update(100, {
      status: chalk.green('Succeeded'),
      speed: ''
    });
    stats.succeeded++;

    // 记录传输模式
    if (!stats.transfersByMode) {
      stats.transfersByMode = [];
    }
    stats.transfersByMode.push({ ...transfer });

    logSuccess(`Successfully transferred: ${transfer.key} using ${transfer.transferMode} mode`);

    // Clean up temp file if it exists and wasn't automatically deleted
    if (transfer.tempFilePath && fs.existsSync(transfer.tempFilePath)) {
      try {
        await fs.promises.unlink(transfer.tempFilePath);
        logVerbose(`Removed temporary file: ${transfer.tempFilePath}`);
      } catch {
        logWarning(`Failed to remove temporary file: ${transfer.tempFilePath}`);
      }
    }
  } catch (error) {
    const err = error as Error;
    transfer.error = err.message;
    logError(`Error transferring file: ${transfer.key}`, err);

    // Clean up temp file if it exists
    if (transfer.tempFilePath && fs.existsSync(transfer.tempFilePath)) {
      try {
        await fs.promises.unlink(transfer.tempFilePath);
        logVerbose(`Removed temporary file after error: ${transfer.tempFilePath}`);
      } catch {
        logWarning(`Failed to remove temporary file after error: ${transfer.tempFilePath}`);
      }
    }

    // Check if we should retry
    if (transfer.retryCount < maxRetries) {
      transfer.retryCount++;
      transfer.status = TransferStatus.RETRYING;

      progressBar.update(transfer.progress || 0, {
        status: chalk.yellow(`Retrying (${transfer.retryCount}/${maxRetries})`),
        speed: ''
      });
      logWarning(`Retrying file (${transfer.retryCount}/${maxRetries}): ${transfer.key}`);

      // Retry after a short delay
      await new Promise(resolve => { setTimeout(resolve, 1000) });
      return processObject(sourceClient, targetClient, config, transfer, progressBar, stats, maxRetries);
    } else {
      // Mark as failed
      transfer.status = TransferStatus.FAILED;
      progressBar.update(transfer.progress || 0, {
        status: chalk.red('Failed'),
        speed: ''
      });

      stats.failed++;
      stats.failedFiles.push(transfer);
      logError(`File transfer failed after ${maxRetries} retries: ${transfer.key}`, err);

      // Pause migration if this is the first failure
      if (stats.failed === 1) {
        stats.paused = true;
        logWarning(`Migration paused due to failure: ${transfer.key}`);
      }
    }
  }
}

/**
 * Print final summary
 */
function printSummary(stats: MigrationStats): void {
  const elapsedSeconds = Math.floor(((stats.endTime || Date.now()) - stats.startTime) / 1000);

  // Count by transfer mode
  const transferModeCounts: Record<string, number> = {};

  for (const transfer of stats.transfersByMode || []) {
    const mode = transfer.transferMode || TransferMode.MEMORY;
    transferModeCounts[mode] = (transferModeCounts[mode] || 0) + 1;
  }

  console.log(chalk.blue.bold('Migration Summary'));
  console.log(chalk.blue('─'.repeat(50)));
  console.log(chalk.gray('Total objects:     '), chalk.white(stats.total.toString()));
  console.log(chalk.gray('Succeeded:         '), chalk.green(stats.succeeded.toString()));
  console.log(chalk.gray('Failed:            '), chalk.red(stats.failed.toString()));
  console.log(chalk.gray('Verification failed:'), chalk.red(stats.verificationFailed.toString()));
  console.log(chalk.gray('Skipped:           '), chalk.yellow(stats.skipped.toString()));
  console.log(chalk.gray('Total time:        '), chalk.white(formatTime(elapsedSeconds)));

  // Print transfer mode stats if available
  if (Object.keys(transferModeCounts).length > 0) {
    console.log('');
    console.log(chalk.blue.bold('Transfer Modes Used'));
    console.log(chalk.blue('─'.repeat(50)));

    for (const [mode, count] of Object.entries(transferModeCounts)) {
      console.log(chalk.gray(`${mode}:`), chalk.white(count.toString()));
    }
  }

  // Log summary information
  log(LogLevel.INFO, `Migration Summary - Total: ${stats.total}, Succeeded: ${stats.succeeded}, Failed: ${stats.failed}, Verification failed: ${stats.verificationFailed}, Skipped: ${stats.skipped}, Time: ${formatTime(elapsedSeconds)}`);

  if (stats.failed > 0 || stats.verificationFailed > 0) {
    console.log('');
    console.log(chalk.yellow('Failed files:'));

    for (const file of stats.failedFiles) {
      console.log(chalk.red(`  ✗ ${file.key}`), chalk.gray(`(${file.error})`));
    }
  }

  console.log('');

  if (stats.succeeded === stats.total) {
    console.log(chalk.green.bold('✓ Migration completed successfully!'));
  } else if (stats.succeeded > 0) {
    console.log(chalk.yellow.bold('⚠ Migration completed with some issues.'));
  } else {
    console.log(chalk.red.bold('✗ Migration failed!'));
  }
}

/**
 * Format seconds into a human-readable time string
 */
function formatTime(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;

  const parts = [];

  if (hours > 0) {
    parts.push(`${hours}h`);
  }

  if (minutes > 0 || hours > 0) {
    parts.push(`${minutes}m`);
  }

  parts.push(`${remainingSeconds}s`);

  return parts.join(' ');
}
