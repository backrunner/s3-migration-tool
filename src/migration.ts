// Node.js built-in modules
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { Readable } from 'node:stream';

// Third-party dependencies
import chalk from 'chalk';
import cliProgress from 'cli-progress';
import * as fsExtra from 'fs-extra';
import inquirer from 'inquirer';
import ora from 'ora';
import { v4 as uuidv4 } from 'uuid';

// Local imports
import {
  abortMultipartUploadsForKeys,
  calculateHash,
  createS3Client,
  deleteObjects,
  downloadObject,
  getObjectHash,
  getObjectSize,
  listAllObjects,
  listMultipartUploads,
  objectExists,
  streamObject,
  uploadObject,
  abortMultipartUpload,
} from './s3-client';
import {
  initLogger,
  log,
  LogLevel,
  logError,
  logSilent,
  logSuccess,
  logVerbose,
  logWarning,
  logInfo,
} from './logger';
import { TransferMode, TransferStatus } from './types';

// Types
import type { S3Client } from '@aws-sdk/client-s3';
import type { FileTransfer, MigrationConfig } from './types';

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
  contentVerificationFailed: number;
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
 * Get system available memory (in bytes)
 */
function getAvailableMemory(): number {
  return os.freemem();
}

/**
 * Check and handle duplicate files in target bucket
 */
async function checkAndHandleDuplicateFiles(
  targetClient: S3Client,
  config: MigrationConfig,
  transfers: FileTransfer[]
): Promise<void> {
  if (transfers.length === 0) {
    return;
  }

  const spinner = ora('Checking for existing files in target bucket...').start();

  try {
    // Get duplicate keys
    const { allDuplicateKeys, completedFileKeys, multipartUploadKeys, multipartUploadData } = await findDuplicateKeys(targetClient, config.target.bucket, transfers, spinner);

    // Handle duplicate files if any
    if (allDuplicateKeys.length > 0) {
      spinner.succeed(`Found ${chalk.bold(allDuplicateKeys.length.toString())} files that already exist in target bucket`);
      logWarning(`Found ${allDuplicateKeys.length} files that already exist in target bucket`);

      // Show sample of duplicate files
      displayDuplicateSamples(allDuplicateKeys, completedFileKeys, multipartUploadKeys, multipartUploadData);

      // Ask user for confirmation to delete files or skip them
      const action = await confirmDeletion(allDuplicateKeys.length, completedFileKeys.length, multipartUploadKeys.length);

      if (action === 'delete_all') {
        await deleteDuplicateFiles(targetClient, config.target.bucket, allDuplicateKeys, 'all');
      } else if (action === 'delete_multipart') {
        await deleteDuplicateFiles(targetClient, config.target.bucket, multipartUploadKeys, 'multipart_only', multipartUploadData);
      } else if (action === 'skip') {
        // Mark all duplicate files as skipped
        for (const file of transfers) {
          if (allDuplicateKeys.includes(file.key)) {
            file.status = TransferStatus.SKIPPED;
            file.verified = false;
            file.error = 'Duplicate file skipped during migration';
          }
        }
        logVerbose(`Marked ${allDuplicateKeys.length} duplicate files to be skipped during migration`);
      } else if (action === 'cancel') {
        throw new Error('Migration cancelled by user due to duplicate files');
      }
    } else {
      spinner.succeed('No duplicate files found in target bucket.');
      logSuccess('No duplicate files found in target bucket.');
    }
  } catch (error) {
    spinner.fail(`Error checking target bucket: ${(error as Error).message}`);
    logError('Error checking duplicate files in target bucket', error as Error);

    // Ask user if they want to continue
    const { continueAnyway } = await inquirer.prompt([
      {
        type: 'confirm',
        name: 'continueAnyway',
        message: 'Failed to check for duplicate files in target bucket. Do you want to continue anyway?',
        default: false
      }
    ]);

    if (!continueAnyway) {
      throw new Error('Migration cancelled due to error checking target bucket');
    }
  }
}

/**
 * Find keys that exist in both source and target
 */
async function findDuplicateKeys(
  targetClient: S3Client,
  targetBucket: string,
  transfers: FileTransfer[],
  spinner: any
): Promise<{
  allDuplicateKeys: string[];
  completedFileKeys: string[];
  multipartUploadKeys: string[];
  multipartUploadData: Array<{ Key: string; UploadId: string; Initiated?: Date }>
}> {
  // Get a list of all source file keys
  const sourceKeys = transfers.map(t => t.key);

  // Store keys that exist in target bucket
  const allDuplicateKeys: string[] = [];
  const completedFileKeys: string[] = [];
  const multipartUploadKeys: string[] = [];
  let multipartUploadData: Array<{ Key: string; UploadId: string; Initiated?: Date }> = [];

  // Get any in-progress multipart uploads for these keys
  try {
    const inProgressUploads = await listMultipartUploads(targetClient, targetBucket);

    // Create a map for faster lookup
    const inProgressKeys = new Set(inProgressUploads.map(upload => upload.Key));

    logVerbose(`Found ${inProgressUploads.length} in-progress multipart uploads in target bucket`);

    // Find multipart duplicates
    const multipartDuplicates = sourceKeys.filter(key => inProgressKeys.has(key));
    if (multipartDuplicates.length > 0) {
      logVerbose(`Found ${multipartDuplicates.length} files with in-progress multipart uploads that match source files`);
      multipartUploadKeys.push(...multipartDuplicates);
      allDuplicateKeys.push(...multipartDuplicates);

      // Store multipart upload data for these keys
      multipartUploadData = inProgressUploads.filter(upload =>
        multipartDuplicates.includes(upload.Key));
    }
  } catch (error) {
    logWarning(`Error checking for in-progress multipart uploads: ${(error as Error).message}`);
    // Continue with the normal object existence checks
  }

  // Check in batches to avoid overwhelming the target S3 service
  const batchSize = 100;
  for (let i = 0; i < sourceKeys.length; i += batchSize) {
    const batch = sourceKeys.slice(i, Math.min(i + batchSize, sourceKeys.length));

    // Check each key in the batch
    await Promise.all(batch.map(async (key) => {
      try {
        const exists = await objectExists(targetClient, targetBucket, key);
        if (exists) {
          // Skip already counted multipart uploads to avoid double counting
          if (!multipartUploadKeys.includes(key)) {
            completedFileKeys.push(key);
            allDuplicateKeys.push(key);
          }
        }
      } catch (error) {
        logError(`Error checking if object exists in target: ${key}`, error as Error);
      }
    }));

    // Update spinner to show progress
    spinner.text = `Checking for existing files in target bucket (${Math.min(i + batchSize, sourceKeys.length)}/${sourceKeys.length})...`;
  }

  return {
    allDuplicateKeys,
    completedFileKeys,
    multipartUploadKeys,
    multipartUploadData
  };
}

/**
 * Display samples of duplicate files
 */
function displayDuplicateSamples(
  allDuplicateKeys: string[],
  completedFileKeys: string[],
  multipartUploadKeys: string[],
  multipartUploadData: Array<{ Key: string; UploadId: string; Initiated?: Date }>
): void {
  // Show sample of all duplicate files (first 10)
  logInfo('\nDuplicate files in target bucket (showing first 10):', chalk.cyan);
  const samplesToShow = Math.min(allDuplicateKeys.length, 10);
  for (let i = 0; i < samplesToShow; i++) {
    const key = allDuplicateKeys[i];
    const isMultipart = multipartUploadKeys.includes(key);
    const label = isMultipart ? '[incomplete upload]' : '[completed file]';

    // For multipart uploads, show when it was initiated if available
    if (isMultipart) {
      const uploadInfo = multipartUploadData.find(u => u.Key === key);
      if (uploadInfo?.Initiated) {
        const date = uploadInfo.Initiated.toISOString().split('T')[0];
        logInfo(`  - ${key} ${chalk.yellow(label)} (started: ${date})`, chalk.white);
      } else {
        logInfo(`  - ${key} ${chalk.yellow(label)}`, chalk.white);
      }
    } else {
      logInfo(`  - ${key} ${chalk.blue(label)}`, chalk.white);
    }
  }

  if (allDuplicateKeys.length > 10) {
    logInfo(`  ... and ${allDuplicateKeys.length - 10} more files`, chalk.white);
  }
  logInfo('', chalk.white);
}

/**
 * Ask user for confirmation to delete files or skip them
 * Returns:
 * - "delete_all" if user wants to delete all files
 * - "delete_multipart" if user wants to delete only incomplete multipart uploads
 * - "skip" if user wants to skip them
 * - "cancel" to cancel migration
 */
async function confirmDeletion(
  totalCount: number,
  completedFilesCount: number,
  multipartUploadsCount: number
): Promise<"delete_all" | "delete_multipart" | "skip" | "cancel"> {
  // Show detailed counts
  logInfo(`Found ${chalk.bold(totalCount.toString())} files in target bucket:`, chalk.white);
  logInfo(`  - ${chalk.bold(completedFilesCount.toString())} completed files`, chalk.white);
  logInfo(`  - ${chalk.bold(multipartUploadsCount.toString())} incomplete multipart uploads`, chalk.white);
  logInfo('', chalk.white);

  // Ask user what to do with duplicate files
  const { action } = await inquirer.prompt([
    {
      type: 'list',
      name: 'action',
      message: `What would you like to do with these existing files?`,
      choices: [
        { name: 'Skip these files (continue migration without them)', value: 'skip' },
        { name: 'Delete ALL files from target bucket before migration', value: 'delete_all' },
        { name: 'Delete ONLY incomplete multipart uploads (keep completed files)', value: 'delete_multipart' },
        { name: 'Cancel migration', value: 'cancel' }
      ]
    }
  ]);

  if (action === 'cancel') {
    logWarning('Migration cancelled by user.');
    return 'cancel';
  }

  if (action === 'skip') {
    logWarning(`Will skip ${totalCount} duplicate files during migration.`);
    return 'skip';
  }

  if (action === 'delete_multipart') {
    if (multipartUploadsCount === 0) {
      logInfo('No incomplete multipart uploads to delete.', chalk.white);

      // Ask again with reduced options if there are no multipart uploads
      if (completedFilesCount > 0) {
        const { secondAction } = await inquirer.prompt([
          {
            type: 'list',
            name: 'secondAction',
            message: 'There are no incomplete uploads, only completed files. What would you like to do?',
            choices: [
              { name: 'Skip these files (continue migration without them)', value: 'skip' },
              { name: 'Delete ALL files from target bucket before migration', value: 'delete_all' },
              { name: 'Cancel migration', value: 'cancel' }
            ]
          }
        ]);

        if (secondAction === 'skip') return 'skip';
        if (secondAction === 'cancel') {
          logWarning('Migration cancelled by user.');
          return 'cancel';
        }
        if (secondAction === 'delete_all') return 'delete_all';
      } else {
        // Nothing to delete, return skip
        return 'skip';
      }
    } else {
      logWarning(`Will delete ${multipartUploadsCount} incomplete multipart uploads.`);
      return 'delete_multipart';
    }
  }

  // Double confirm for safety if user chose to delete all
  if (action === 'delete_all') {
    const { confirmDeleteAgain } = await inquirer.prompt([
      {
        type: 'confirm',
        name: 'confirmDeleteAgain',
        message: chalk.red.bold(`WARNING: This will DELETE ${totalCount} files from the target bucket. Are you ABSOLUTELY sure?`),
        default: false
      }
    ]);

    if (!confirmDeleteAgain) {
      logWarning('Deletion cancelled. What would you like to do instead?');
      // Ask again with reduced options
      const { secondAction } = await inquirer.prompt([
        {
          type: 'list',
          name: 'secondAction',
          message: 'Please choose an alternative action:',
          choices: [
            { name: 'Skip these files (continue migration without them)', value: 'skip' },
            { name: 'Delete ONLY incomplete multipart uploads', value: 'delete_multipart' },
            { name: 'Cancel migration', value: 'cancel' }
          ]
        }
      ]);

      if (secondAction === 'cancel') {
        logWarning('Migration cancelled by user.');
        return 'cancel';
      } else if (secondAction === 'skip') {
        logWarning(`Will skip ${totalCount} duplicate files during migration.`);
        return 'skip';
      } else if (secondAction === 'delete_multipart') {
        if (multipartUploadsCount === 0) {
          logWarning('No incomplete multipart uploads to delete.');
          return 'skip'; // Skip if there are no multipart uploads
        }
        logWarning(`Will delete ${multipartUploadsCount} incomplete multipart uploads.`);
        return 'delete_multipart';
      }
    }

    return 'delete_all';
  }

  return 'skip'; // Default fallback (should never reach here)
}

/**
 * Delete duplicate files from target bucket
 */
async function deleteDuplicateFiles(
  targetClient: S3Client,
  targetBucket: string,
  duplicateKeys: string[],
  deleteMode: 'all' | 'multipart_only' = 'all',
  multipartUploadData?: Array<{ Key: string; UploadId: string; Initiated?: Date }>
): Promise<void> {
  // Create a progress bar for deletion
  const progressBar = new cliProgress.SingleBar({
    format: ' {bar} | {percentage}% | {value}/{total} | Deleting files from target bucket',
    clearOnComplete: false,
    hideCursor: true,
  }, cliProgress.Presets.shades_grey);

  progressBar.start(duplicateKeys.length, 0);

  // First, abort any in-progress multipart uploads for these keys
  logVerbose(`Checking for in-progress multipart uploads for ${duplicateKeys.length} files...`);

  try {
    // If we're only deleting multipart uploads and have the data already, use it directly
    if (deleteMode === 'multipart_only' && multipartUploadData && multipartUploadData.length > 0) {
      // Abort each upload individually with its upload ID
      let aborted = 0;
      let failed = 0;

      for (const upload of multipartUploadData) {
        try {
          const success = await abortMultipartUpload(
            targetClient,
            targetBucket,
            upload.Key,
            upload.UploadId
          );

          if (success) {
            aborted++;
          } else {
            failed++;
          }
          progressBar.update(aborted + failed);
        } catch (error) {
          failed++;
          progressBar.update(aborted + failed);
          logError(`Failed to abort multipart upload for ${upload.Key}: ${(error as Error).message}`);
        }
      }

      progressBar.stop();

      if (failed > 0) {
        logWarning(`\nAborted ${aborted} incomplete multipart uploads, ${failed} failed to abort.`);
      } else {
        logSuccess(`\nSuccessfully aborted ${aborted} incomplete multipart uploads.`);
      }

      return; // Exit early if we're only handling multipart uploads
    } else {
      // Default behavior - call the bulk API
      const multipartResult = await abortMultipartUploadsForKeys(
        targetClient,
        targetBucket,
        duplicateKeys
      );

      if (multipartResult.totalFound > 0) {
        logVerbose(`Found and aborted ${multipartResult.aborted} in-progress multipart uploads for duplicate files. ${multipartResult.failed} failed to abort.`);
      } else {
        logVerbose('No in-progress multipart uploads found for duplicate files.');
      }
    }
  } catch (error) {
    logWarning(`Error handling in-progress multipart uploads: ${(error as Error).message}`);
    // Continue with deletion even if this fails
  }

  // If we're only deleting multipart uploads, we're done
  if (deleteMode === 'multipart_only') {
    progressBar.stop();
    return;
  }

  // Delete complete files in batches
  let deleted = 0;
  let failed = 0;
  const failedKeys: string[] = [];

  // Process in batches of 1000 (S3 limitation)
  const deleteBatchSize = 1000;
  for (let i = 0; i < duplicateKeys.length; i += deleteBatchSize) {
    const batch = duplicateKeys.slice(i, i + deleteBatchSize);

    try {
      const result = await deleteObjects(targetClient, targetBucket, batch);
      deleted += result.deleted.length;
      failed += result.failed.length;

      if (result.failed.length > 0) {
        failedKeys.push(...result.failed);
      }

      progressBar.update(i + batch.length);
    } catch (error) {
      failed += batch.length;
      failedKeys.push(...batch);
      logError(`Error deleting objects from target bucket: ${(error as Error).message}`);
    }
  }

  progressBar.stop();

  // Show results
  if (failed > 0) {
    logWarning(`\nDeleted ${deleted} files from target bucket, ${failed} deletions failed.`);
    log(LogLevel.WARNING, 'Failed to delete the following files:', false);

    for (const key of failedKeys) {
      log(LogLevel.ERROR, `  - ${key}`, false);
    }
  } else {
    logSuccess(`\nSuccessfully deleted ${deleted} files from target bucket.`);
  }
}

/**
 * Determine the optimal transfer mode based on file characteristics and system resources
 * @param fileSize File size in bytes
 * @param fileKey File key/path
 * @param availableMemory Available system memory in bytes
 * @returns The recommended TransferMode
 */
function determineOptimalTransferMode(
  fileSize: number,
  fileKey: string,
  availableMemory: number
): TransferMode {
  // If file size not known, default to STREAM mode as it's safest
  if (!fileSize || fileSize <= 0) {
    logVerbose(`File size unknown for ${fileKey}, defaulting to STREAM mode`);
    return TransferMode.STREAM;
  }

  // 1. Check file extension for format-specific optimizations
  const fileExt = path.extname(fileKey).toLowerCase();
  const compressedFormats = ['.zip', '.gz', '.bz2', '.tar', '.rar', '.7z', '.xz'];
  const imageFormats = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff'];
  const textFormats = ['.txt', '.csv', '.json', '.xml', '.html', '.md', '.log'];

  // 2. Define size thresholds (dynamic based on available memory)
  const smallFileThreshold = Math.min(5 * 1024 * 1024, availableMemory * 0.1); // 5MB or 10% of RAM, whichever is smaller
  const mediumFileThreshold = Math.min(100 * 1024 * 1024, availableMemory * 0.3); // 100MB or 30% of RAM, whichever is smaller
  const largeFileThreshold = Math.min(500 * 1024 * 1024, availableMemory * 0.5); // 500MB or 50% of RAM, whichever is smaller

  // 3. Format-specific behaviors combined with size
  // Smallest files - use memory mode for fastest transfer
  if (fileSize < smallFileThreshold) {
    // For text formats (typically highly compressible), memory mode is generally best for small files
    if (textFormats.includes(fileExt)) {
      logVerbose(`Small text file (${formatBytes(fileSize)}) detected for ${fileKey}, using MEMORY mode for optimization`);
      return TransferMode.MEMORY;
    }

    // For small images, memory mode is usually efficient
    if (imageFormats.includes(fileExt)) {
      logVerbose(`Small image file (${formatBytes(fileSize)}) detected for ${fileKey}, using MEMORY mode for optimization`);
      return TransferMode.MEMORY;
    }

    // For all other small files, use memory mode
    logVerbose(`Small file (${formatBytes(fileSize)}) detected for ${fileKey}, using MEMORY mode`);
    return TransferMode.MEMORY;
  }

  // Medium files - balance between performance and resource usage
  if (fileSize < mediumFileThreshold) {
    // For compressed formats in medium size, prefer STREAM mode to avoid double compression/decompression
    if (compressedFormats.includes(fileExt)) {
      logVerbose(`Medium compressed file (${formatBytes(fileSize)}) detected for ${fileKey}, using STREAM mode to avoid unnecessary decompression`);
      return TransferMode.STREAM;
    }

    // For medium size images, disk mode is usually a good balance
    if (imageFormats.includes(fileExt)) {
      logVerbose(`Medium image file (${formatBytes(fileSize)}) detected for ${fileKey}, using DISK mode for balance`);
      return TransferMode.DISK;
    }

    // For other medium files, disk mode offers good balance
    logVerbose(`Medium file (${formatBytes(fileSize)}) detected for ${fileKey}, using DISK mode for balance of performance and memory`);
    return TransferMode.DISK;
  }

  // Large files - conserve memory
  if (fileSize < largeFileThreshold) {
    // All large files usually benefit from DISK mode
    logVerbose(`Large file (${formatBytes(fileSize)}) detected for ${fileKey}, using DISK mode to conserve memory`);
    return TransferMode.DISK;
  }

  // Very large files - always use STREAM mode
  logVerbose(`Very large file (${formatBytes(fileSize)}) detected for ${fileKey}, using STREAM mode to minimize memory usage`);
  return TransferMode.STREAM;
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
    // First, respect user's global config if specified
    if (config.transferMode && config.transferMode !== TransferMode.AUTO && !transfer.transferMode) {
      // Apply the user's globally specified transfer mode to this file
      transfer.transferMode = config.transferMode;
      logVerbose(`Using user-specified ${transfer.transferMode} mode for ${transfer.key}`);
    }

    // If no mode specified yet or auto mode, determine the best mode
    if (!transfer.transferMode || transfer.transferMode === TransferMode.AUTO) {
      // Apply the intelligent mode selection algorithm
      const availableMemory = getAvailableMemory();
      transfer.transferMode = determineOptimalTransferMode(
        transfer.size || 0,
        transfer.key,
        availableMemory
      );

      logVerbose(`Auto-selected ${transfer.transferMode} mode for ${transfer.key} based on file characteristics`);

      // Safety check - for very large files relative to memory, always use STREAM mode
      if (transfer.size && transfer.size > availableMemory * 0.8 && transfer.transferMode !== TransferMode.STREAM) {
        logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large relative to available memory (${formatBytes(availableMemory)}), overriding to STREAM mode`);
        transfer.transferMode = TransferMode.STREAM;
      }
    }

    // Resource-based safety checks for all modes (including user-specified modes)
    // These will only change the mode if there are resource constraints

    // Create temp directory if needed for disk or memory mode (that could fallback to disk)
    let tempDir = config.tempDir;
    if ((transfer.transferMode === TransferMode.DISK || transfer.transferMode === TransferMode.MEMORY) && !tempDir) {
      tempDir = path.join(process.cwd(), 'tmp');
      await fsExtra.ensureDir(tempDir);
    }

    // Check if memory mode is suitable for this file's size
    if (transfer.transferMode === TransferMode.MEMORY && transfer.size) {
      const availableMemory = os.freemem();

      // If file is larger than 50% of available memory, switch to disk mode
      if (transfer.size > availableMemory * 0.5) {
        logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large for memory mode, switching to disk mode`);
        transfer.transferMode = TransferMode.DISK;
      }
    }

    // Check if disk mode is suitable for this file's size
    if (transfer.transferMode === TransferMode.DISK && transfer.size) {
      const availableMemory = os.freemem();

      // If file is larger than 80% of available memory, switch to stream mode
      if (transfer.size > availableMemory * 0.8) {
        logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large for disk mode, switching to stream mode`);
        transfer.transferMode = TransferMode.STREAM;
      }
    }

    const isStreamMode = transfer.transferMode === TransferMode.STREAM;

    // Calculate source hash if content verification is enabled and not in stream mode
    // For stream mode, we'll handle differently since we don't store the entire file locally
    if (config.verifyFileContentAfterMigration && !isStreamMode) {
      try {
        progressBar.update(0, {
          status: chalk.blue(`Calculating source hash`),
          speed: ''
        });

        // Calculate hash of source object
        transfer.sourceHash = await getObjectHash(
          sourceClient,
          config.source.bucket,
          transfer.key,
          'md5',
          tempDir
        );

        logVerbose(`Source hash for ${transfer.key}: ${transfer.sourceHash}`);
      } catch (error) {
        logWarning(`Failed to calculate source hash for ${transfer.key}: ${(error as Error).message}`);
        logWarning(`Content verification will be skipped for this file`);
      }
    }

    // For stream mode, use the streamObject function that transfers directly
    if (isStreamMode) {
      progressBar.update(0, {
        status: chalk.blue(`Streaming`),
        speed: ''
      });

      transfer.status = TransferStatus.STREAMING;
      logVerbose(`Starting streaming transfer of ${transfer.key} using direct pipe mode`);

      // Use the direct streaming method
      await streamObject(
        sourceClient,
        targetClient,
        config.source.bucket,
        config.target.bucket,
        transfer.key,
        transfer.key,
        (transferred, total, speed) => {
          transfer.downloadSpeed = speed; // Using download speed for the entire transfer

          // For stream mode, progress spans from 0-100% in one operation
          const progress = Math.floor((transferred / total) * 100);
          transfer.progress = progress;

          // Check if transfer is stalled (speed = 0)
          if (speed === 0) {
            progressBar.update(progress, {
              status: chalk.yellow(`Stalled`),
              speed: chalk.red(`STALLED`)
            });
            logSilent(`Stream transfer appears to be stalled for ${transfer.key} at ${progress}%`);
          } else {
            progressBar.update(progress, {
              status: chalk.blue(`Direct Streaming`),
              speed: formatSpeed(speed)
            });
          }
        },
        maxRetries,  // Pass maximum retry count
        // Based on file size, choose fallback mode
        transfer.size && transfer.size < getAvailableMemory() * 0.2
          ? TransferMode.MEMORY   // Memory mode for smaller files
          : TransferMode.DISK,    // Disk mode for larger files
        // Pass temp directory
        config.tempDir || path.join(process.cwd(), 'tmp'),
        2                         // Try fallback after 2 failures
      );

      // Mark as succeeded
      transfer.status = TransferStatus.SUCCEEDED;
      const finalSpeedInfo = transfer.downloadSpeed ? formatSpeed(transfer.downloadSpeed) : 'N/A';

      progressBar.update(100, {
        status: chalk.green('Stream Completed'),
        speed: finalSpeedInfo
      });
      stats.succeeded++;

      // Verify content hash if enabled for stream mode
      if (config.verifyFileContentAfterMigration) {
        try {
          progressBar.update(100, {
            status: chalk.blue(`Verifying content`),
            speed: ''
          });

          // Calculate hash of source and target objects
          logVerbose(`Calculating source hash for ${transfer.key} after streaming`);
          transfer.sourceHash = await getObjectHash(
            sourceClient,
            config.source.bucket,
            transfer.key,
            'md5',
            tempDir
          );

          logVerbose(`Calculating target hash for ${transfer.key} after streaming`);
          transfer.targetHash = await getObjectHash(
            targetClient,
            config.target.bucket,
            transfer.key,
            'md5',
            tempDir
          );

          // Compare hashes
          if (transfer.sourceHash === transfer.targetHash) {
            transfer.contentVerified = true;
            progressBar.update(100, {
              status: chalk.green('Content Verified'),
              speed: finalSpeedInfo
            });
            logSuccess(`Content verified for ${transfer.key}: ${transfer.sourceHash}`);
          } else {
            transfer.contentVerified = false;
            transfer.status = TransferStatus.CONTENT_VERIFICATION_FAILED;
            stats.succeeded--;
            stats.contentVerificationFailed++;
            stats.failedFiles.push(transfer);

            progressBar.update(100, {
              status: chalk.red('Content Verification Failed'),
              speed: finalSpeedInfo
            });

            logError(`Content verification failed for ${transfer.key}`);
            logError(`Source hash: ${transfer.sourceHash}`);
            logError(`Target hash: ${transfer.targetHash}`);
          }
        } catch (error) {
          logWarning(`Failed to verify content for ${transfer.key}: ${(error as Error).message}`);
        }
      }

      // Record transfer mode
      if (!stats.transfersByMode) {
        stats.transfersByMode = [];
      }
      stats.transfersByMode.push({ ...transfer });

      logSuccess(`Successfully streamed: ${transfer.key} using direct pipe mode`);
      return;
    }

    // The rest of the code for non-stream modes
    progressBar.update(0, {
      status: chalk.blue(`Downloading`),
      speed: ''
    });

    // Download from source
    transfer.status = TransferStatus.DOWNLOADING;
    logVerbose(`Starting download of ${transfer.key} using ${transfer.transferMode} mode`);

    const data = await downloadObject(
      sourceClient,
      config.source.bucket,
      transfer.key,
      (downloaded, total, speed) => {
        transfer.downloadSpeed = speed;

        // For memory/disk mode, use the main progress bar for download (0-100%)
        const progress = Math.floor((downloaded / total) * 100);

        // Check if download is stalled (speed = 0)
        if (speed === 0) {
          progressBar.update(progress, {
            status: chalk.yellow(`Download Stalled`),
            speed: chalk.red(`STALLED`)
          });
          logSilent(`Download appears to be stalled for ${transfer.key} at ${progress}%`);
        } else {
          progressBar.update(progress, {
            status: chalk.blue(`Downloading`),
            speed: formatSpeed(speed)
          });
        }
      },
      transfer.transferMode,
      tempDir
    );

    // For content verification in memory/disk mode, we can calculate hash during download
    // if we didn't already calculate it
    if (config.verifyFileContentAfterMigration && !transfer.sourceHash) {
      try {
        progressBar.update(100, {
          status: chalk.blue(`Calculating source hash`),
          speed: ''
        });

        // Calculate hash from the downloaded data
        transfer.sourceHash = await calculateHash(data as string | Buffer | Readable);
        logVerbose(`Source hash for ${transfer.key}: ${transfer.sourceHash}`);
      } catch (error) {
        logWarning(`Failed to calculate source hash for ${transfer.key}: ${(error as Error).message}`);
        logWarning(`Content verification will be skipped for this file`);
      }
    }

    // Output debug info about the downloaded data
    logVerbose(`Download completed for ${transfer.key}`);
    logVerbose(`Data type received from downloadObject: ${typeof data}`);
    if (typeof data === 'string') {
      logVerbose(`Disk mode - file path: ${data}`);
    } else if (Buffer.isBuffer(data)) {
      logVerbose(`Memory mode - buffer size: ${formatBytes(data.length)}`);
    } else {
      logVerbose(`Stream mode - using data stream`);
    }

    // Calculate average download speed if available
    const avgSpeed = transfer.downloadSpeed ? formatSpeed(transfer.downloadSpeed) : 'N/A';

    progressBar.update(100, {
      status: chalk.green('Download Complete'),
      speed: avgSpeed
    });

    // Log that we're transitioning to upload
    logVerbose(`Download complete for ${transfer.key}, preparing for upload using ${transfer.transferMode} mode`);

    // Now update the bar for upload phase
    progressBar.update(0, {
      status: chalk.blue(`Uploading`),
      speed: ''
    });

    // Log before starting upload
    logVerbose(`Starting upload for ${transfer.key}...`);

    // Upload to target
    transfer.status = TransferStatus.UPLOADING;
    logVerbose(`Starting upload of ${transfer.key} to target using ${transfer.transferMode} mode`);

    // Store temp file path for disk mode
    if (typeof data === 'string') {
      transfer.tempFilePath = data;
      logVerbose(`Using temporary file for upload: ${transfer.tempFilePath}`);
    } else if (Buffer.isBuffer(data)) {
      logVerbose(`Using memory buffer for upload, size: ${formatBytes(data.length)}`);
    } else {
      logVerbose(`Using stream for upload`);
    }

    try {
      // Add debug information
      logVerbose(`Calling uploadObject with data type: ${typeof data}, transfer mode: ${transfer.transferMode}`);

      // For file paths, verify existence
      if (typeof data === 'string') {
        logVerbose(`File path exists check: ${fs.existsSync(data)}`);
        try {
          const stats = fs.statSync(data);
          logVerbose(`File size on disk: ${formatBytes(stats.size)}`);
        } catch (err) {
          logError(`Error checking file stats: ${(err as Error).message}`);
        }
      }

      logVerbose(`Target bucket: ${config.target.bucket}, key: ${transfer.key}`);
      logVerbose(`Starting upload with AWS S3 client endpoint: ${config.target.endpoint}`);

      // Delete temp file after upload only for disk mode
      const shouldDeleteTempFile = transfer.transferMode === TransferMode.DISK;

      await uploadObject(
        targetClient,
        config.target.bucket,
        transfer.key,
        data,
        (uploaded, total, speed) => {
          transfer.uploadSpeed = speed;

          // Use the main progress bar for upload progress (0-100%)
          const progress = Math.floor((uploaded / total) * 100);

          // Check if upload is stalled (speed = 0)
          if (speed === 0) {
            progressBar.update(progress, {
              status: chalk.yellow(`Upload Stalled`),
              speed: chalk.red(`STALLED`)
            });
          } else {
            progressBar.update(progress, {
              status: chalk.blue(`Uploading`),
              speed: formatSpeed(speed)
            });
          }

          // Add detailed logs
          if (progress % 10 === 0 || speed === 0) { // Log every 10% and when stalled
            if (speed === 0) {
              logSilent(`Upload appears to be stalled for ${transfer.key} at ${progress}%`);
            } else {
              logVerbose(`Upload progress for ${transfer.key}: ${progress}%, ${formatBytes(uploaded)}/${formatBytes(total)} at ${formatSpeed(speed)}`);
            }
          }
        },
        shouldDeleteTempFile,
        transfer.transferMode
      );
    } catch (error) {
      // Handle error based on transfer mode
      if (transfer.transferMode === TransferMode.MEMORY && (error as Error).message.includes('memory')) {
        logWarning(`Memory mode failed for upload: ${(error as Error).message}`);
        logWarning(`Switching to disk mode for ${transfer.key}`);
        logVerbose(`This is an automatic downgrade due to memory constraints, not counted as a retry`);

        // Switch to disk mode
        transfer.transferMode = TransferMode.DISK;

        // Ensure temp directory exists
        if (!tempDir) {
          tempDir = path.join(process.cwd(), 'tmp');
          await fsExtra.ensureDir(tempDir);
        }

        // If data is a Buffer, save it to a temporary file first
        if (Buffer.isBuffer(data)) {
          const tempFilePath = path.join(tempDir, `${uuidv4()}-${path.basename(transfer.key)}`);
          await fs.promises.writeFile(tempFilePath, data);
          transfer.tempFilePath = tempFilePath;

          // Update the progress bar for retry
          progressBar.update(0, {
            status: chalk.blue(`Uploading (disk - retry)`),
            speed: ''
          });

          logVerbose(`Retrying upload using disk mode with temporary file: ${tempFilePath}`);

          // Use the temporary file for upload
          await uploadObject(
            targetClient,
            config.target.bucket,
            transfer.key,
            tempFilePath,
            (uploaded, total, speed) => {
              transfer.uploadSpeed = speed;

              const progress = Math.floor((uploaded / total) * 100);

              // Check if retry upload is stalled
              if (speed === 0) {
                progressBar.update(progress, {
                  status: chalk.yellow(`Retry Upload Stalled`),
                  speed: chalk.red(`STALLED`)
                });
                logSilent(`Retry upload appears to be stalled for ${transfer.key} at ${progress}%`);
              } else {
                progressBar.update(progress, {
                  status: chalk.blue(`Uploading (disk - retry)`),
                  speed: formatSpeed(speed)
                });
              }
            },
            true, // Delete the temporary file after upload
            TransferMode.DISK
          );
        } else {
          logWarning(`Cannot convert from ${typeof data} to disk mode, trying stream mode instead`);
          // If not a Buffer but another type, try stream mode as a last resort
          transfer.transferMode = TransferMode.STREAM;
          throw new Error(`Fallback to stream mode initiated for ${transfer.key}`);
        }
      } else if (transfer.transferMode === TransferMode.DISK && ((error as Error).message.includes('memory') || (error as Error).message.includes('disk'))) {
        logWarning(`Disk mode failed for upload: ${(error as Error).message}`);
        logWarning(`Switching to stream mode for ${transfer.key}`);
        logVerbose(`This is an automatic downgrade due to resource constraints, not counted as a retry`);

        // Switch to stream mode for a last attempt
        transfer.transferMode = TransferMode.STREAM;
        throw new Error(`Fallback to stream mode initiated for ${transfer.key}`);
      } else {
        // For other errors, throw directly
        throw error;
      }
    }

    // Mark as succeeded
    transfer.status = TransferStatus.SUCCEEDED;

    // Calculate final speed information
    let finalSpeedInfo = '';
    if (isStreamMode) {
      // For stream mode, show the average transfer speed
      const avgSpeed = transfer.uploadSpeed ? formatSpeed(transfer.uploadSpeed) : 'N/A';
      finalSpeedInfo = avgSpeed;
    } else {
      // For memory/disk mode, show the average upload speed
      const avgSpeed = transfer.uploadSpeed ? formatSpeed(transfer.uploadSpeed) : 'N/A';
      finalSpeedInfo = avgSpeed;
    }

    progressBar.update(100, {
      status: chalk.green('Succeeded'),
      speed: finalSpeedInfo
    });
    stats.succeeded++;

    // Verify content hash if enabled for memory/disk mode
    if (config.verifyFileContentAfterMigration && transfer.sourceHash) {
      try {
        progressBar.update(100, {
          status: chalk.blue(`Verifying content`),
          speed: ''
        });

        // Calculate hash of the target object
        logVerbose(`Calculating target hash for ${transfer.key}`);
        transfer.targetHash = await getObjectHash(
          targetClient,
          config.target.bucket,
          transfer.key,
          'md5',
          tempDir
        );

        // Compare hashes
        if (transfer.sourceHash === transfer.targetHash) {
          transfer.contentVerified = true;
          progressBar.update(100, {
            status: chalk.green('Content Verified'),
            speed: finalSpeedInfo
          });
          logSuccess(`Content verified for ${transfer.key}: ${transfer.sourceHash}`);
        } else {
          transfer.contentVerified = false;
          transfer.status = TransferStatus.CONTENT_VERIFICATION_FAILED;
          stats.succeeded--;
          stats.contentVerificationFailed++;
          stats.failedFiles.push(transfer);

          progressBar.update(100, {
            status: chalk.red('Content Verification Failed'),
            speed: finalSpeedInfo
          });

          logError(`Content verification failed for ${transfer.key}`);
          logError(`Source hash: ${transfer.sourceHash}`);
          logError(`Target hash: ${transfer.targetHash}`);
        }
      } catch (error) {
        logWarning(`Failed to verify content for ${transfer.key}: ${(error as Error).message}`);
      }
    }

    // Record transfer mode
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
      await new Promise<void>(resolve => { setTimeout(resolve, 1000); });
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
 * Format time duration in seconds to human-readable string
 */
function formatTime(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;

  const parts = [];
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0) {
    parts.push(`${minutes}m`);
  }
  if (secs > 0 || parts.length === 0) {
    parts.push(`${secs}s`);
  }

  return parts.join(' ');
}

/**
 * Run the migration process
 */
export async function runMigration(config: MigrationConfig): Promise<void> {
  // Initialize logger
  initLogger(config);

  try {
    // Create S3 clients early so we can use them for checking target bucket
    const sourceClient = createS3Client(config.source);
    const targetClient = createS3Client(config.target);

    logInfo('Starting S3 migration...', chalk.cyan);
    logInfo(`Source: ${config.source.endpoint}/${config.source.bucket}`, chalk.cyan);
    logInfo(`Target: ${config.target.endpoint}/${config.target.bucket}`, chalk.cyan);
    logInfo(`Concurrency: ${config.concurrency}`, chalk.white);
    logInfo(`Max Retries: ${config.maxRetries}`, chalk.white);
    logInfo(`Verify After Migration: ${config.verifyAfterMigration}`, chalk.white);
    logInfo(`Verify File Content After Migration: ${config.verifyFileContentAfterMigration}`, chalk.white);
    logInfo(`Purge Source After Migration: ${config.purgeSourceAfterMigration}`, chalk.white);

    // Log if we're skipping target existence check
    if (config.skipTargetExistenceCheck) {
      logInfo(`Skip Target Existence Check: true`, chalk.magenta);
      logWarning('Skipping check for existing files in target bucket.');
    }

    // Initialize transfer mode if not already set
    if (!config.transferMode) {
      config.transferMode = TransferMode.AUTO; // 默认使用AUTO模式
      logInfo(`Using AUTO mode as default for intelligent selection based on file characteristics`, chalk.cyan);
    }

    logInfo(`Transfer Mode: ${config.transferMode}`, chalk.magenta);

    // Display available memory
    const availableMemory = getAvailableMemory();
    logInfo(`Available Memory: ${formatBytes(availableMemory)}`, chalk.white);

    // Initialize temp directory if disk mode is possible
    if (config.transferMode === TransferMode.AUTO || config.transferMode === TransferMode.DISK) {
      // Use provided temp directory or create a default one
      config.tempDir = config.tempDir || path.join(process.cwd(), 'tmp');
      logInfo(`Temp Directory: ${config.tempDir}`, chalk.white);

      // Make sure temp directory exists
      await fsExtra.ensureDir(config.tempDir);
    }

    // Only display prefix if it's provided
    if (config.prefix) {
      logInfo(`Prefix: ${config.prefix}`, chalk.cyan);
    }

    // Display include patterns if provided
    if (config.include && config.include.length > 0) {
      logInfo(`Include patterns: ${config.include.join(', ')}`, chalk.cyan);
    }

    // Display exclude patterns if provided
    if (config.exclude && config.exclude.length > 0) {
      logInfo(`Exclude patterns: ${config.exclude.join(', ')}`, chalk.cyan);
    }

    if (config.dryRun) {
      logWarning('DRY RUN MODE - No files will be transferred');
      log(LogLevel.WARNING, 'DRY RUN MODE - No files will be transferred');
    }

    // Initialize stats
    const stats: MigrationStats = {
      total: 0,
      processed: 0,
      succeeded: 0,
      failed: 0,
      skipped: 0,
      verificationFailed: 0,
      contentVerificationFailed: 0,
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
      logWarning('No objects to migrate. Exiting.');
      return;
    }

    // Create file transfer objects
    const transfers: FileTransfer[] = allKeys.map(key => ({
      key,
      status: TransferStatus.PENDING,
      retryCount: 0,
      progress: 0
    }));

    // Check for duplicate files in target bucket before proceeding
    if (!config.skipTargetExistenceCheck && !config.dryRun) {
      await checkAndHandleDuplicateFiles(targetClient, config, transfers);

      // After checking duplicates, update stats for skipped files
      const skippedFiles = transfers.filter(t => t.status === TransferStatus.SKIPPED);
      if (skippedFiles.length > 0) {
        stats.skipped += skippedFiles.length;

        // Log information about skipped files
        logWarning(`Skipping ${skippedFiles.length} files that already exist in target bucket.`);
        log(LogLevel.INFO, `Adjusted migration count: ${stats.total - skippedFiles.length} files will be migrated.`, false);
      }
    }

    // After listing and filtering objects, check for large files
    if (stats.total > 0) {
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

        // First, respect user's global config if specified
        if (config.transferMode && config.transferMode !== TransferMode.AUTO && !transfer.transferMode) {
          // Apply the user's globally specified transfer mode to this file
          transfer.transferMode = config.transferMode;
          logVerbose(`Using user-specified ${transfer.transferMode} mode for ${transfer.key}`);
        }

        // If no mode specified yet or auto mode, determine the best mode
        if (!transfer.transferMode || transfer.transferMode === TransferMode.AUTO) {
          // Apply the intelligent mode selection algorithm
          const availableMemory = getAvailableMemory();
          transfer.transferMode = determineOptimalTransferMode(
            transfer.size || 0,
            transfer.key,
            availableMemory
          );

          logVerbose(`Auto-selected ${transfer.transferMode} mode for ${transfer.key} based on file characteristics`);

          // Safety check - for very large files relative to memory, always use STREAM mode
          if (transfer.size && transfer.size > availableMemory * 0.8 && transfer.transferMode !== TransferMode.STREAM) {
            logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large relative to available memory (${formatBytes(availableMemory)}), overriding to STREAM mode`);
            transfer.transferMode = TransferMode.STREAM;
          }
        }

        // Resource-based safety checks for all modes (including user-specified modes)
        // These will only change the mode if there are resource constraints

        // Create temp directory if needed for disk or memory mode (that could fallback to disk)
        let tempDir = config.tempDir;
        if ((transfer.transferMode === TransferMode.DISK || transfer.transferMode === TransferMode.MEMORY) && !tempDir) {
          tempDir = path.join(process.cwd(), 'tmp');
          await fsExtra.ensureDir(tempDir);
        }

        // Check if memory mode is suitable for this file's size
        if (transfer.transferMode === TransferMode.MEMORY && transfer.size) {
          const availableMemory = os.freemem();

          // If file is larger than 50% of available memory, switch to disk mode
          if (transfer.size > availableMemory * 0.5) {
            logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large for memory mode, switching to disk mode`);
            transfer.transferMode = TransferMode.DISK;
          }
        }

        // Check if disk mode is suitable for this file's size
        if (transfer.transferMode === TransferMode.DISK && transfer.size) {
          const availableMemory = os.freemem();

          // If file is larger than 80% of available memory, switch to stream mode
          if (transfer.size > availableMemory * 0.8) {
            logWarning(`File ${transfer.key} (${formatBytes(transfer.size)}) is too large for disk mode, switching to stream mode`);
            transfer.transferMode = TransferMode.STREAM;
          }
        }
      }

      // If no global transfer mode is specified by user, use AUTO mode
      if (!config.transferMode) {
        config.transferMode = TransferMode.AUTO;
        logInfo(`No transfer mode specified, using AUTO mode for intelligent selection`, chalk.cyan);
      }

      // Display the file size information if there are large files
      if (largestFileSize > 0) {
        // Always display file size and memory information
        logInfo('', chalk.white); // Empty line
        logInfo(`Largest file detected: ${largestFile}`, chalk.cyan);
        logInfo(`Size: ${formatBytes(largestFileSize)}`, chalk.white);
        logInfo(`Available memory: ${formatBytes(availableMemory)}`, chalk.white);

        // Display selected transfer mode
        logInfo(`Using transfer mode: ${config.transferMode}`, chalk.magenta);
        logInfo(`Note: Stream mode is now recommended for all files for better reliability.`, chalk.yellow);
      }
    }

    // Show sample of files to migrate (first 10 files)
    if (transfers.length > 0) {
      logInfo('\nFiles to migrate (showing first 10):', chalk.cyan);
      const samplesToShow = Math.min(transfers.length, 10);
      for (let i = 0; i < samplesToShow; i++) {
        logInfo(`  - ${transfers[i].key}`, chalk.white);
      }

      if (transfers.length > 10) {
        logInfo(`  ... and ${transfers.length - 10} more files`, chalk.white);
      }
      logInfo('', chalk.white);
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
        logWarning('Migration cancelled by user.');
        return;
      }
    } else {
      log(LogLevel.INFO, `Proceeding with migration of ${stats.total.toString()} objects (confirmation skipped)`, false);
    }

    // Create multibar
    const multibar = new cliProgress.MultiBar({
      clearOnComplete: false,
      hideCursor: true,
      format: ' {bar} | {percentage}% | {value}/{total} | {status} | {file} | {speed}',
    }, cliProgress.Presets.shades_grey);

    // Create overall progress bar - used to track overall migration progress
    multibar.create(stats.total, 0, {
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
    logInfo(`Starting migration with concurrency of ${concurrency.toString()}`, chalk.cyan);
    logInfo('', chalk.white);

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
          logWarning('Migration stopped by user.');
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
          // Skip if already processed files
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

          await processObject(sourceClient, targetClient, config, transfer, progressBar, stats, maxRetries);
        })
      );

      currentIndex = endIndex;
    }

    /**
     * Print final summary
     */
    const printSummary = (stats: MigrationStats): void => {
      const elapsedTime = Math.floor(((stats.endTime || Date.now()) - stats.startTime) / 1000);

      // Count by transfer mode
      const transferModeCounts: Record<string, number> = {};

      for (const transfer of stats.transfersByMode || []) {
        const mode = transfer.transferMode || TransferMode.STREAM;
        transferModeCounts[mode] = (transferModeCounts[mode] || 0) + 1;
      }

      logInfo('Migration Summary', chalk.cyanBright.bold);
      logInfo('─'.repeat(50), chalk.white);
      logInfo(`Total objects:      ${stats.total.toString()}`, chalk.white);
      logInfo(`Succeeded:          ${stats.succeeded.toString()}`, chalk.green);
      logInfo(`Failed:             ${stats.failed.toString()}`, chalk.redBright);
      logInfo(`Verification failed: ${stats.verificationFailed.toString()}`, chalk.yellow);
      logInfo(`Content verification failed: ${stats.contentVerificationFailed.toString()}`, chalk.yellow);
      logInfo(`Skipped:            ${stats.skipped.toString()}`, chalk.gray);
      logInfo(`Total time:         ${formatTime(elapsedTime)}`, chalk.white);

      // Add detailed summary to log file
      log(LogLevel.INFO, `Migration Summary - Total: ${stats.total}, Succeeded: ${stats.succeeded}, Failed: ${stats.failed}, Verification failed: ${stats.verificationFailed}, Content verification failed: ${stats.contentVerificationFailed}, Skipped: ${stats.skipped}, Time: ${formatTime(elapsedTime)}`);

      // Print transfer mode stats if available
      if (Object.keys(transferModeCounts).length > 0) {
        logInfo('', chalk.white);
        logInfo('Transfer Modes Used', chalk.cyanBright.bold);
        logInfo('─'.repeat(50), chalk.white);

        for (const [mode, count] of Object.entries(transferModeCounts)) {
          let displayMode = mode;
          if (mode === TransferMode.STREAM) {
            displayMode = `${mode} (direct piping)`;
          }
          logInfo(`${displayMode}: ${count.toString()}`, chalk.white);
        }
      }

      if (stats.failed > 0 || stats.verificationFailed > 0 || stats.contentVerificationFailed > 0) {
        logInfo('', chalk.white);
        logWarning('Failed files:');

        for (const file of stats.failedFiles) {
          if (file.status === TransferStatus.CONTENT_VERIFICATION_FAILED) {
            logError(`  ✗ ${file.key} (Content verification failed - source: ${file.sourceHash}, target: ${file.targetHash})`);
          } else {
            logError(`  ✗ ${file.key} (${file.error || 'Unknown error'})`);
          }
        }
      }

      logInfo('', chalk.white);

      if (stats.succeeded === stats.total) {
        logSuccess('✓ Migration completed successfully!');
      } else if (stats.succeeded > 0) {
        logWarning('⚠ Migration completed with some issues.');
      } else {
        logError('✗ Migration failed!');
      }
    };

    printSummary(stats);
  } catch (error) {
    logError('Error running migration', error as Error);
    process.exit(1);
  }
}
