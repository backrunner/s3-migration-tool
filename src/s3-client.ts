// Node.js built-in modules
import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { Readable } from 'node:stream';

// Third-party dependencies
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  ListObjectsV2Command,
  S3Client,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import * as fsExtra from 'fs-extra';
import { v4 as uuidv4 } from 'uuid';

// Local imports
import { logError, logSilent, logSuccess, logVerbose, logWarning } from './logger';
import { TransferMode } from './types';
import { formatBytes, formatSpeed } from './utils';

// Types
import type {
  CreateMultipartUploadCommandOutput,
  ListMultipartUploadsCommandOutput,
  ListObjectsV2CommandOutput,
  UploadPartCommandOutput,
} from '@aws-sdk/client-s3';
import type { S3Credentials } from './types';

/**
 * Create an S3 client from credentials
 */
export function createS3Client(credentials: S3Credentials): S3Client {
  const clientConfig: any = {
    endpoint: credentials.endpoint,
    region: credentials.region,
    credentials: {
      accessKeyId: credentials.accessKey,
      secretAccessKey: credentials.secretKey,
    },
    forcePathStyle: credentials.forcePathStyle ?? false,
    useAccelerateEndpoint: credentials.useAccelerateEndpoint ?? false,
  };

  // Add signature version if specified
  if (credentials.signatureVersion) {
    // For S3 v2 signatures, we need to use a special configuration
    if (credentials.signatureVersion === 'v2') {
      clientConfig.signatureVersion = 'v2';
    }
    // v4 is the default for AWS SDK v3, so no need to specify it
  }

  return new S3Client(clientConfig);
}

/**
 * List all objects in a bucket with pagination
 */
export async function* listAllObjects(
  client: S3Client,
  bucket: string,
  prefix?: string
): AsyncGenerator<string[]> {
  let continuationToken: string | undefined;

  do {
    const command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix || undefined,
      ContinuationToken: continuationToken,
      MaxKeys: 1000,
    });

    const response: ListObjectsV2CommandOutput = await client.send(command);

    if (response.Contents && response.Contents.length > 0) {
      yield response.Contents
        .filter(item => item.Key)
        .map(item => item.Key as string);
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
}

/**
 * Get object size
 */
export async function getObjectSize(
  client: S3Client,
  bucket: string,
  key: string
): Promise<number> {
  const command = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  try {
    const response = await client.send(command);
    return response.ContentLength || 0;
  } catch (error) {
    logError(`Failed to get size for ${key}: ${(error as Error).message}`);
    return 0;
  }
}

/**
 * Download an object from S3 with progress tracking
 * Can work in disk, stream, or memory mode depending on the transferMode parameter
 */
export async function downloadObject(
  client: S3Client,
  bucket: string,
  key: string,
  onProgress?: (downloaded: number, total: number, speed: number) => void,
  transferMode: TransferMode = TransferMode.STREAM,
  tempDir?: string
): Promise<string | Readable | Buffer> {
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  const response = await client.send(command);

  if (!response.Body) {
    throw new Error(`Failed to download object: ${key}`);
  }

  const contentLength = response.ContentLength || 0;
  const responseBody = response.Body;

  // If stream mode, return the readable stream directly
  if (transferMode === TransferMode.STREAM) {
    return responseBody as Readable;
  }

  // If memory mode, load the entire file into memory as a Buffer
  if (transferMode === TransferMode.MEMORY) {
    return new Promise<Buffer>((resolve, reject) => {
      try {
        let lastReportTime = Date.now();
        let lastReportedBytes = 0;
        let downloadedBytes = 0;
        const chunks: Buffer[] = [];
        const readableStream = responseBody as Readable;

        readableStream.on('data', (chunk) => {
          downloadedBytes += chunk.length;
          chunks.push(Buffer.from(chunk));

          if (onProgress) {
            const now = Date.now();
            const timeDiff = now - lastReportTime;

            // Update progress every 100ms
            if (timeDiff >= 100) {
              const byteDiff = downloadedBytes - lastReportedBytes;
              const speed = byteDiff / (timeDiff / 1000); // bytes per second

              onProgress(downloadedBytes, contentLength, speed);

              lastReportTime = now;
              lastReportedBytes = downloadedBytes;
            }
          }
        });

        readableStream.on('end', () => {
          if (onProgress) {
            onProgress(contentLength, contentLength, 0);
          }

          // Concatenate all chunks into a single Buffer
          const buffer = Buffer.concat(chunks);
          resolve(buffer);
        });

        readableStream.on('error', (error) => {
          reject(error);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  // If disk mode, save to temporary file
  if (transferMode === TransferMode.DISK) {
    const tmpDir = tempDir || path.join(process.cwd(), 'tmp');

    // Ensure temp directory exists
    await fsExtra.ensureDir(tmpDir);

    const tempFilePath = path.join(tmpDir, `${uuidv4()}-${path.basename(key)}`);

    return new Promise<string>((resolve, reject) => {
      try {
        let lastReportTime = Date.now();
        let lastReportedBytes = 0;
        let downloadedBytes = 0;

        // Create write stream to file
        const fileStream = fs.createWriteStream(tempFilePath);
        const readableStream = responseBody as Readable;

        readableStream.on('data', (chunk) => {
          downloadedBytes += chunk.length;

          if (onProgress) {
            const now = Date.now();
            const timeDiff = now - lastReportTime;

            // Update progress every 100ms
            if (timeDiff >= 100) {
              const byteDiff = downloadedBytes - lastReportedBytes;
              const speed = byteDiff / (timeDiff / 1000); // bytes per second

              onProgress(downloadedBytes, contentLength, speed);

              lastReportTime = now;
              lastReportedBytes = downloadedBytes;
            }
          }
        });

        readableStream.on('end', () => {
          if (onProgress) {
            onProgress(contentLength, contentLength, 0);
          }
        });

        readableStream.on('error', (error) => {
          // Clean up temp file on error
          fs.unlink(tempFilePath, () => {
            reject(error);
          });
        });

        fileStream.on('error', (error) => {
          // Clean up temp file on error
          fs.unlink(tempFilePath, () => {
            reject(error);
          });
        });

        fileStream.on('finish', () => {
          resolve(tempFilePath);
        });

        // Pipe stream to file
        readableStream.pipe(fileStream);
      } catch (error) {
        // Clean up temp file on error
        fs.unlink(tempFilePath, () => {
          reject(error);
        });
      }
    });
  }

  // Default to stream mode if no other mode selected
  return responseBody as Readable;
}

/**
 * Upload an object to S3 with progress tracking
 * Can work with file path or stream depending on the input
 * Includes retry and resume capabilities for handling network interruptions
 */
export async function uploadObject(
  client: S3Client,
  bucket: string,
  key: string,
  body: string | Readable | Buffer,
  onProgress?: (uploaded: number, total: number, speed: number) => void,
  removeSourceAfterUpload = false,
  // transferMode parameter kept for API compatibility, but no longer used internally
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  _transferMode: TransferMode = TransferMode.STREAM,
  maxRetries = 3
): Promise<void> {
  // Use random UUID to track this transfer session
  const transferId = uuidv4();

  let uploadBody: Readable | Buffer;
  let totalBytes: number;

  // Prepare for multipart upload with resumability
  let multipartUploadId: string | undefined;
  let uploadedParts: { ETag: string; PartNumber: number }[] = [];
  let currentPartNumber = 1;
  let lastUploadedByte = 0;
  let retryCount = 0;
  let resuming = false;

  // If body is a string, assume it's a file path
  if (typeof body === 'string') {
    totalBytes = fs.statSync(body).size;
    uploadBody = fs.createReadStream(body) as Readable;
  }
  // If body is a Buffer
  else if (Buffer.isBuffer(body)) {
    totalBytes = body.length;
    uploadBody = body;
  }
  // If body is a Readable stream
  else {
    // For streams, we don't know the size in advance
    // Set a large value for progress tracking
    totalBytes = 1000000000; // 1GB default
    uploadBody = body as Readable;
  }

  // Define part size based on file size for optimal performance
  const partSize = calculateOptimalPartSize(totalBytes);
  logVerbose(`Using part size of ${formatBytes(partSize)} for upload of ${key}`);

  // Main upload loop with retries
  while (retryCount <= maxRetries) {
    try {
      if (resuming) {
        logVerbose(`Resuming upload for ${key} from part ${currentPartNumber} (byte position ${lastUploadedByte})`);
      } else if (retryCount > 0) {
        logWarning(`Retry ${retryCount}/${maxRetries} for ${key}`);
      }

      // Initialize the multipart upload if we don't have an ID yet
      if (!multipartUploadId) {
        const initCommand = new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          // Add metadata to help identify this upload session
          Metadata: {
            'x-amz-meta-transfer-id': transferId
          }
        });

        const initResponse = await client.send(initCommand) as CreateMultipartUploadCommandOutput;
        multipartUploadId = initResponse.UploadId;

        if (!multipartUploadId) {
          throw new Error(`Failed to initialize multipart upload for ${key}`);
        }

        logVerbose(`Initiated multipart upload with ID: ${multipartUploadId} for ${key}`);
        uploadedParts = [];
        currentPartNumber = 1;
        lastUploadedByte = 0;
      }

      let uploadStream: Readable;

      // Prepare the upload stream based on the body type
      if (Buffer.isBuffer(uploadBody)) {
        // If we're resuming and the body is a buffer, we need to slice it
        const slicedBuffer = resuming
          ? uploadBody.slice(lastUploadedByte)
          : uploadBody;

        // Convert Buffer to a stream for multipart upload
        const bufferStream = new Readable();
        bufferStream.push(slicedBuffer);
        bufferStream.push(null);
        uploadStream = bufferStream;
      }
      else if (typeof body === 'string' && resuming) {
        // For file paths when resuming, create a stream with the correct start position
        uploadStream = fs.createReadStream(body, {
          start: lastUploadedByte
        }) as Readable;
      }
      else {
        // Use the original stream (can't resume streams that aren't file-based)
        uploadStream = uploadBody as Readable;
      }

      // Track progress variables
      let uploadedBytes = lastUploadedByte; // Start from where we left off
      let lastReportTime = Date.now();
      let lastReportedBytes = lastUploadedByte;
      let lastProgressTime = Date.now();
      let uploadStalled = false;
      let errorOccurred = false;

      // We'll use this buffer to accumulate data for each part
      let currentBuffer: Buffer[] = [];
      let currentBufferSize = 0;

      // Report initial progress if resuming
      if (lastUploadedByte > 0 && onProgress) {
        onProgress(lastUploadedByte, totalBytes, 0);
      }

      // Add timeout detection
      const timeoutCheckId = setInterval(() => {
        const now = Date.now();
        if (now - lastProgressTime > 30000 && !uploadStalled) {
          uploadStalled = true;
          logSilent(`Upload appears to be stalled for ${key}, no progress for 30 seconds`);
          logSilent(`Upload speed has dropped to 0 B/s for ${key}`);

          // Update progress callback with stalled status
          if (onProgress) {
            // Call with 0 speed to indicate stalled transfer
            onProgress(lastReportedBytes, totalBytes, 0);
          }
        }

        // If stalled for too long, abort and retry from last good part
        if (uploadStalled && now - lastProgressTime > 60000) {
          logError(`Upload for ${key} has been stalled for over 1 minute. Initiating resume sequence.`);
          logSilent(`Aborting stalled upload for ${key} after 1 minute of inactivity`);
          errorOccurred = true;
          uploadStream.destroy(new Error('Upload timeout after 1 minute of inactivity'));
        }
      }, 5000);

      try {
        // Process the upload stream in chunks
        for await (const chunk of uploadStream) {
          // Reset stall detection
          lastProgressTime = Date.now();

          if (uploadStalled) {
            uploadStalled = false;
            logSilent(`Upload for ${key} has resumed after being stalled`);
          }

          // Add chunk to current buffer
          currentBuffer.push(Buffer.from(chunk));
          currentBufferSize += chunk.length;
          uploadedBytes += chunk.length;

          // Report progress
          if (onProgress) {
            const now = Date.now();
            const timeDiff = now - lastReportTime;

            if (timeDiff >= 100) {
              const byteDiff = uploadedBytes - lastReportedBytes;
              const speed = byteDiff / (timeDiff / 1000);

              onProgress(uploadedBytes, totalBytes, speed);

              lastReportTime = now;
              lastReportedBytes = uploadedBytes;

              // Add detailed logs
              logVerbose(`Upload progress for ${key}: ${Math.floor((uploadedBytes / totalBytes) * 100)}%, ${formatBytes(uploadedBytes)}/${formatBytes(totalBytes)} at ${formatSpeed(speed)}`);
            }
          }

          // If we've accumulated enough data for a part, upload it
          if (currentBufferSize >= partSize) {
            try {
              const partBuffer = Buffer.concat(currentBuffer);

              // Upload the part
              const uploadPartCommand = new UploadPartCommand({
                Bucket: bucket,
                Key: key,
                PartNumber: currentPartNumber,
                UploadId: multipartUploadId,
                Body: partBuffer
              });

              logVerbose(`Uploading part ${currentPartNumber} (${formatBytes(partBuffer.length)}) for ${key}`);
              const uploadPartResponse = await client.send(uploadPartCommand) as UploadPartCommandOutput;

              if (uploadPartResponse.ETag) {
                uploadedParts.push({
                  ETag: uploadPartResponse.ETag,
                  PartNumber: currentPartNumber
                });

                logVerbose(`Successfully uploaded part ${currentPartNumber} for ${key}`);

                // Update tracking variables
                lastUploadedByte = uploadedBytes;
                currentPartNumber++;

                // Reset buffer for next part
                currentBuffer = [];
                currentBufferSize = 0;
              } else {
                throw new Error(`Missing ETag for part ${currentPartNumber}`);
              }
            } catch (error) {
              logError(`Error uploading part ${currentPartNumber} for ${key}: ${(error as Error).message}`);

              // Don't immediately abort, set error flag to start resume process after stream closes
              errorOccurred = true;
              break;
            }
          }
        }

        // Handle any remaining data as the final part if there was no error
        if (!errorOccurred && currentBufferSize > 0) {
          try {
            const partBuffer = Buffer.concat(currentBuffer);

            const uploadPartCommand = new UploadPartCommand({
              Bucket: bucket,
              Key: key,
              PartNumber: currentPartNumber,
              UploadId: multipartUploadId,
              Body: partBuffer
            });

            logVerbose(`Uploading final part ${currentPartNumber} (${formatBytes(partBuffer.length)}) for ${key}`);
            const uploadPartResponse = await client.send(uploadPartCommand) as UploadPartCommandOutput;

            if (uploadPartResponse.ETag) {
              uploadedParts.push({
                ETag: uploadPartResponse.ETag,
                PartNumber: currentPartNumber
              });

              logVerbose(`Successfully uploaded final part ${currentPartNumber} for ${key}`);
            } else {
              throw new Error(`Missing ETag for final part ${currentPartNumber}`);
            }
          } catch (error) {
            logError(`Error uploading final part ${currentPartNumber} for ${key}: ${(error as Error).message}`);
            errorOccurred = true;
          }
        }

        // If an error occurred, prepare for resuming
        if (errorOccurred) {
          clearInterval(timeoutCheckId);
          logWarning(`Preparing to resume upload of ${key} from part ${currentPartNumber}`);
          retryCount++;
          resuming = true;

          // Short delay before retry
          await new Promise<void>(resolve => {
            setTimeout(() => {
              resolve();
            }, 1000 * retryCount);
          });
          continue;
        }

        // No errors - complete the multipart upload
        try {
          // Sort parts by part number for the complete call
          const sortedParts = [...uploadedParts].sort((a, b) => a.PartNumber - b.PartNumber);

          const completeCommand = new CompleteMultipartUploadCommand({
            Bucket: bucket,
            Key: key,
            UploadId: multipartUploadId,
            MultipartUpload: {
              Parts: sortedParts
            }
          });

          logVerbose(`Completing multipart upload for ${key} with ${sortedParts.length} parts`);
          await client.send(completeCommand);

          // Final progress report
          if (onProgress) {
            onProgress(totalBytes, totalBytes, 0);
          }

          logSuccess(`Upload completed for ${key}`);

          // Clean up source file if needed
          if (removeSourceAfterUpload && typeof body === 'string') {
            try {
              await fs.promises.unlink(body);
            } catch (error) {
              logError(`Failed to remove temp file ${body}: ${(error as Error).message}`);
            }
          }

          return; // Success!
        } catch (error) {
          logError(`Error completing multipart upload for ${key}: ${(error as Error).message}`);
          throw error; // Re-throw to trigger retry
        }
      } finally {
        clearInterval(timeoutCheckId);
      }
    } catch (error) {
      logError(`Error in upload for ${key}: ${(error as Error).message}`);

      if (retryCount >= maxRetries) {
        logError(`Exceeded maximum retries (${maxRetries}) for ${key}. Aborting upload.`);

        // Try to abort the multipart upload to avoid orphaned parts
        if (multipartUploadId) {
          try {
            const abortCommand = new AbortMultipartUploadCommand({
              Bucket: bucket,
              Key: key,
              UploadId: multipartUploadId
            });

            await client.send(abortCommand);
            logWarning(`Aborted multipart upload for ${key}`);
          } catch (abortError) {
            logError(`Failed to abort multipart upload: ${(abortError as Error).message}`);
          }
        }

        // Clean up source file if needed (only on complete failure)
        if (removeSourceAfterUpload && typeof body === 'string') {
          try {
            await fs.promises.unlink(body);
          } catch (error) {
            logError(`Failed to remove temp file ${body}: ${(error as Error).message}`);
          }
        }

        throw new Error(`Failed to upload ${key} after ${maxRetries} retries: ${(error as Error).message}`);
      }

      retryCount++;
      resuming = true;

      // Exponential backoff before retry
      const backoffTime = Math.min(1000 * Math.pow(2, retryCount), 30000);
      logVerbose(`Retrying in ${backoffTime / 1000} seconds...`);
      await new Promise<void>(resolve => {
        setTimeout(() => {
          resolve();
        }, backoffTime);
      });
    }
  }
}

/**
 * Check if an object exists in the bucket
 */
export async function objectExists(
  client: S3Client,
  bucket: string,
  key: string
): Promise<boolean> {
  const command = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  try {
    await client.send(command);
    return true;
  } catch {
    return false;
  }
}

/**
 * Delete a single object from the bucket
 */
export async function deleteObject(
  client: S3Client,
  bucket: string,
  key: string
): Promise<void> {
  const command = new DeleteObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  await client.send(command);
}

/**
 * Delete multiple objects from the bucket in batches
 */
export async function deleteObjects(
  client: S3Client,
  bucket: string,
  keys: string[],
  onProgress?: (deleted: number, total: number) => void
): Promise<{ deleted: string[], failed: string[] }> {
  const batchSize = 1000; // AWS allows max 1000 keys per delete operation
  const deleted: string[] = [];
  const failed: string[] = [];

  // Process in batches
  for (let i = 0; i < keys.length; i += batchSize) {
    const batch = keys.slice(i, i + batchSize);

    try {
      const command = new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: batch.map(key => ({ Key: key })),
          Quiet: false
        }
      });

      const response = await client.send(command);

      // Add successfully deleted objects
      if (response.Deleted) {
        for (const obj of response.Deleted) {
          if (obj.Key) {
            deleted.push(obj.Key);
          }
        }
      }

      // Add failed objects
      if (response.Errors) {
        for (const error of response.Errors) {
          if (error.Key) {
            failed.push(error.Key);
          }
        }
      }

      // Report progress
      if (onProgress) {
        onProgress(deleted.length, keys.length);
      }
    } catch {
      // If the batch operation fails, add all keys in this batch to failed
      failed.push(...batch);
    }
  }

  return { deleted, failed };
}

/**
 * Stream an object directly from source S3 to target S3 with progress tracking
 * This is the preferred method for transferring large files that avoids buffering the entire file
 * Includes retry and resume capabilities for handling network interruptions
 * Can fallback to memory or disk mode if streaming repeatedly fails
 */
export async function streamObject(
  sourceClient: S3Client,
  targetClient: S3Client,
  sourceBucket: string,
  targetBucket: string,
  sourceKey: string,
  targetKey: string,
  onProgress?: (transferred: number, total: number, speed: number) => void,
  maxRetries = 3,
  fallbackMode?: TransferMode, // Optional fallback mode if streaming fails repeatedly
  tempDir?: string, // Temp directory for fallback to disk mode
  fallbackThreshold = 2 // Number of retries before considering fallback
): Promise<void> {
  // Use random UUID to track this transfer session
  const transferId = uuidv4();

  // Prepare for multipart upload with resumability
  let multipartUploadId: string | undefined;
  let uploadedParts: { ETag: string; PartNumber: number }[] = [];
  let currentPartNumber = 1;
  let lastUploadedByte = 0;
  let retryCount = 0;
  let resuming = false;
  let streamFailureStreak = 0; // Count consecutive streaming failures to trigger fallback

  // Create download command
  const downloadCommand = new GetObjectCommand({
    Bucket: sourceBucket,
    Key: sourceKey,
  });

  // Get content information first for planning
  const headCommand = new HeadObjectCommand({
    Bucket: sourceBucket,
    Key: sourceKey,
  });

  let contentLength = 0;

  try {
    const headResponse = await sourceClient.send(headCommand);
    contentLength = headResponse.ContentLength || 0;
    logVerbose(`Object size: ${formatBytes(contentLength)} for ${sourceKey}`);
  } catch (error) {
    logWarning(`Unable to get content length for ${sourceKey}: ${(error as Error).message}`);
    // Continue anyway, we'll use the content length from the download response
  }

  // Define part size based on file size for optimal performance
  const partSize = calculateOptimalPartSize(contentLength);
  logVerbose(`Using part size of ${formatBytes(partSize)} for transfer of ${sourceKey}`);

  // Main transfer loop with retries
  while (retryCount <= maxRetries) {
    try {
      if (resuming) {
        logVerbose(`Resuming transfer for ${sourceKey} from part ${currentPartNumber} (byte position ${lastUploadedByte})`);
      } else if (retryCount > 0) {
        logWarning(`Retry ${retryCount}/${maxRetries} for ${sourceKey}`);
      }

      // Check if we should fallback to a different transfer mode
      if (fallbackMode && streamFailureStreak >= fallbackThreshold && retryCount >= fallbackThreshold) {
        logWarning(`Stream transfer for ${sourceKey} has failed ${streamFailureStreak} times consecutively.`);
        logWarning(`Falling back to ${fallbackMode} mode for ${sourceKey}`);

        // Abort any existing multipart upload
        if (multipartUploadId) {
          try {
            const abortCommand = new AbortMultipartUploadCommand({
              Bucket: targetBucket,
              Key: targetKey,
              UploadId: multipartUploadId
            });

            await targetClient.send(abortCommand);
            logWarning(`Aborted multipart upload for ${targetKey} to prepare for fallback mode`);
          } catch (abortError) {
            logError(`Failed to abort multipart upload: ${(abortError as Error).message}`);
          }
        }

        // Switch to fallback mode
        if (fallbackMode === TransferMode.MEMORY) {
          logVerbose(`Falling back to memory mode for ${sourceKey}`);

          // Check file size - only use memory mode for reasonably sized files
          const availableMemory = os.freemem();
          if (contentLength > availableMemory * 0.5) {
            logWarning(`File size (${formatBytes(contentLength)}) exceeds 50% of available memory (${formatBytes(availableMemory)})`);
            logWarning(`Automatically switching to disk mode instead of memory mode`);

            // Use disk mode instead
            logVerbose(`Falling back to disk mode for ${sourceKey}`);

            // Ensure temp directory exists
            const tmpDir = tempDir || path.join(process.cwd(), 'tmp');
            await fsExtra.ensureDir(tmpDir);

            // Download the object to disk
            logVerbose(`Downloading ${sourceKey} to disk for fallback transfer`);
            const tempFilePath = await downloadObject(
              sourceClient,
              sourceBucket,
              sourceKey,
              onProgress,
              TransferMode.DISK,
              tmpDir
            ) as string;

            // Upload from disk
            logVerbose(`Uploading ${targetKey} from disk after fallback`);
            await uploadObject(
              targetClient,
              targetBucket,
              targetKey,
              tempFilePath,
              onProgress,
              true, // Remove temp file after upload
              TransferMode.DISK,
              maxRetries - retryCount // Remaining retries
            );

            logSuccess(`Fallback transfer completed using disk mode for ${sourceKey}`);
            return;
          }
        }

        // If memory mode without exceeding memory limits, or already in disk mode
        if (fallbackMode === TransferMode.MEMORY) {
          logVerbose(`Downloading ${sourceKey} to memory for fallback transfer`);
          const buffer = await downloadObject(
            sourceClient,
            sourceBucket,
            sourceKey,
            onProgress,
            TransferMode.MEMORY
          ) as Buffer;

          // Upload from memory
          logVerbose(`Uploading ${targetKey} from memory after fallback`);
          await uploadObject(
            targetClient,
            targetBucket,
            targetKey,
            buffer,
            onProgress,
            false, // No temp file to remove
            TransferMode.MEMORY,
            maxRetries - retryCount // Remaining retries
          );

          logSuccess(`Fallback transfer completed using memory mode for ${sourceKey}`);
          return;
        }
        else if (fallbackMode === TransferMode.DISK) {
          logVerbose(`Falling back to disk mode for ${sourceKey}`);

          // Ensure temp directory exists
          const tmpDir = tempDir || path.join(process.cwd(), 'tmp');
          await fsExtra.ensureDir(tmpDir);

          // Download the object to disk
          logVerbose(`Downloading ${sourceKey} to disk for fallback transfer`);
          const tempFilePath = await downloadObject(
            sourceClient,
            sourceBucket,
            sourceKey,
            onProgress,
            TransferMode.DISK,
            tmpDir
          ) as string;

          // Upload from disk
          logVerbose(`Uploading ${targetKey} from disk after fallback`);
          await uploadObject(
            targetClient,
            targetBucket,
            targetKey,
            tempFilePath,
            onProgress,
            true, // Remove temp file after upload
            TransferMode.DISK,
            maxRetries - retryCount // Remaining retries
          );

          logSuccess(`Fallback transfer completed using disk mode for ${sourceKey}`);
          return;
        }
      }

      // Initialize the multipart upload if we don't have an ID yet
      if (!multipartUploadId) {
        const initCommand = new CreateMultipartUploadCommand({
          Bucket: targetBucket,
          Key: targetKey,
          // Add metadata to help identify this upload session
          Metadata: {
            'x-amz-meta-transfer-id': transferId,
            'x-amz-meta-source-bucket': sourceBucket,
            'x-amz-meta-source-key': sourceKey
          }
        });

        const initResponse = await targetClient.send(initCommand) as CreateMultipartUploadCommandOutput;
        multipartUploadId = initResponse.UploadId;

        if (!multipartUploadId) {
          throw new Error(`Failed to initialize multipart upload for ${targetKey}`);
        }

        logVerbose(`Initiated multipart upload with ID: ${multipartUploadId} for ${targetKey}`);
        uploadedParts = [];
        currentPartNumber = 1;
        lastUploadedByte = 0;
      }

      // Send the download command with a range if we're resuming
      let rangeDownloadCommand;

      if (lastUploadedByte > 0) {
        // Create a new command with range header
        rangeDownloadCommand = new GetObjectCommand({
          Bucket: sourceBucket,
          Key: sourceKey,
          Range: `bytes=${lastUploadedByte}-`
        });
        logVerbose(`Requesting byte range ${lastUploadedByte}- for ${sourceKey}`);
      } else {
        rangeDownloadCommand = downloadCommand;
      }

      // Send the command to get the stream
      const response = await sourceClient.send(rangeDownloadCommand);

      if (!response.Body) {
        throw new Error(`Failed to download object: ${sourceKey}`);
      }

      // Update content length if we didn't get it before
      if (contentLength === 0 && response.ContentLength) {
        contentLength = response.ContentLength;
        if (lastUploadedByte > 0 && response.ContentRange) {
          // If we have a content range, we need to parse the total length
          const matches = response.ContentRange.match(/bytes \d+-\d+\/(\d+)/);
          if (matches && matches[1]) {
            contentLength = parseInt(matches[1], 10);
          }
        }
      }

      const downloadStream = response.Body as Readable;

      // Track progress variables
      let transferredBytes = lastUploadedByte; // Start from where we left off
      let lastReportTime = Date.now();
      let lastReportedBytes = lastUploadedByte;
      let lastProgressTime = Date.now();
      let streamStalled = false;
      let errorOccurred = false;

      // We'll use this buffer to accumulate data for each part
      let currentBuffer: Buffer[] = [];
      let currentBufferSize = 0;

      // Report initial progress if resuming
      if (lastUploadedByte > 0 && onProgress) {
        onProgress(lastUploadedByte, contentLength, 0);
      }

      // Add timeout detection
      const timeoutCheckId = setInterval(() => {
        const now = Date.now();
        if (now - lastProgressTime > 15000 && !streamStalled) {
          streamStalled = true;
          logSilent(`Stream transfer appears to be stalled for ${sourceKey}, no progress for 15 seconds`);
          logSilent(`Transfer speed has dropped to 0 B/s for ${sourceKey}`);

          // Update progress callback with stalled status
          if (onProgress) {
            // Call with current bytes but 0 speed to indicate stalled transfer
            onProgress(transferredBytes, contentLength, 0);
          }
        }

        // If stalled for too long, abort and retry from last good part
        if (streamStalled && now - lastProgressTime > 60000) {
          logError(`Stream transfer for ${sourceKey} has been stalled for over 1 minute. Initiating resume sequence.`);
          logSilent(`Aborting stalled stream transfer for ${sourceKey} after 1 minute of inactivity`);
          errorOccurred = true;
          downloadStream.destroy(new Error('Transfer timeout after 1 minute of inactivity'));
        }
      }, 5000);

      // Process the download stream in chunks
      for await (const chunk of downloadStream) {
        // Reset stall detection
        lastProgressTime = Date.now();

        if (streamStalled) {
          streamStalled = false;
          logSilent(`Stream transfer for ${sourceKey} has resumed after being stalled`);
        }

        // Add chunk to current buffer
        currentBuffer.push(Buffer.from(chunk));
        currentBufferSize += chunk.length;
        transferredBytes += chunk.length;

        // Report progress
        if (onProgress) {
          const now = Date.now();
          const timeDiff = now - lastReportTime;

          if (timeDiff >= 100) {
            const byteDiff = transferredBytes - lastReportedBytes;
            const speed = byteDiff / (timeDiff / 1000);

            onProgress(transferredBytes, contentLength, speed);

            lastReportTime = now;
            lastReportedBytes = transferredBytes;
          }
        }

        // If we've accumulated enough data for a part, upload it
        if (currentBufferSize >= partSize) {
          try {
            const partBuffer = Buffer.concat(currentBuffer);

            // Upload the part
            const uploadPartCommand = new UploadPartCommand({
              Bucket: targetBucket,
              Key: targetKey,
              PartNumber: currentPartNumber,
              UploadId: multipartUploadId,
              Body: partBuffer
            });

            logVerbose(`Uploading part ${currentPartNumber} (${formatBytes(partBuffer.length)}) for ${targetKey}`);
            const uploadPartResponse = await targetClient.send(uploadPartCommand) as UploadPartCommandOutput;

            if (uploadPartResponse.ETag) {
              uploadedParts.push({
                ETag: uploadPartResponse.ETag,
                PartNumber: currentPartNumber
              });

              logVerbose(`Successfully uploaded part ${currentPartNumber} for ${targetKey}`);

              // Update tracking variables
              lastUploadedByte = transferredBytes;
              currentPartNumber++;

              // Reset buffer for next part
              currentBuffer = [];
              currentBufferSize = 0;
            } else {
              throw new Error(`Missing ETag for part ${currentPartNumber}`);
            }
          } catch (error) {
            logError(`Error uploading part ${currentPartNumber} for ${targetKey}: ${(error as Error).message}`);

            // Don't immediately abort, set error flag to start resume process after stream closes
            errorOccurred = true;
            break;
          }
        }
      }

      // Clear timeout check
      clearInterval(timeoutCheckId);

      // Handle any remaining data as the final part if there was no error
      if (!errorOccurred && currentBufferSize > 0) {
        try {
          const partBuffer = Buffer.concat(currentBuffer);

          const uploadPartCommand = new UploadPartCommand({
            Bucket: targetBucket,
            Key: targetKey,
            PartNumber: currentPartNumber,
            UploadId: multipartUploadId,
            Body: partBuffer
          });

          logVerbose(`Uploading final part ${currentPartNumber} (${formatBytes(partBuffer.length)}) for ${targetKey}`);
          const uploadPartResponse = await targetClient.send(uploadPartCommand) as UploadPartCommandOutput;

          if (uploadPartResponse.ETag) {
            uploadedParts.push({
              ETag: uploadPartResponse.ETag,
              PartNumber: currentPartNumber
            });

            logVerbose(`Successfully uploaded final part ${currentPartNumber} for ${targetKey}`);
          } else {
            throw new Error(`Missing ETag for final part ${currentPartNumber}`);
          }
        } catch (error) {
          logError(`Error uploading final part ${currentPartNumber} for ${targetKey}: ${(error as Error).message}`);
          errorOccurred = true;
        }
      }

      // If an error occurred, prepare for resuming
      if (errorOccurred) {
        logWarning(`Preparing to resume upload of ${targetKey} from part ${currentPartNumber}`);
        retryCount++;
        resuming = true;
        streamFailureStreak++; // Increment failure streak counter

        // Short delay before retry
        await new Promise<void>(resolve => {
          setTimeout(() => {
            resolve();
          }, 1000 * retryCount);
        });
        continue;
      }

      // No errors - complete the multipart upload
      try {
        // Sort parts by part number for the complete call
        const sortedParts = [...uploadedParts].sort((a, b) => a.PartNumber - b.PartNumber);

        const completeCommand = new CompleteMultipartUploadCommand({
          Bucket: targetBucket,
          Key: targetKey,
          UploadId: multipartUploadId,
          MultipartUpload: {
            Parts: sortedParts
          }
        });

        logVerbose(`Completing multipart upload for ${targetKey} with ${sortedParts.length} parts`);
        await targetClient.send(completeCommand);

        // Final progress report
        if (onProgress) {
          onProgress(contentLength, contentLength, 0);
        }

        logSuccess(`Stream transfer completed for ${sourceKey} to ${targetKey}`);
        return; // Success!
      } catch (error) {
        logError(`Error completing multipart upload for ${targetKey}: ${(error as Error).message}`);
        streamFailureStreak++; // Increment failure streak counter
        throw error; // Re-throw to trigger retry
      }
    } catch (error) {
      logError(`Error in stream transfer for ${sourceKey}: ${(error as Error).message}`);
      streamFailureStreak++; // Increment failure streak counter

      if (retryCount >= maxRetries) {
        logError(`Exceeded maximum retries (${maxRetries}) for ${sourceKey}. Aborting transfer.`);

        // Try to abort the multipart upload to avoid orphaned parts
        if (multipartUploadId) {
          try {
            const abortCommand = new AbortMultipartUploadCommand({
              Bucket: targetBucket,
              Key: targetKey,
              UploadId: multipartUploadId
            });

            await targetClient.send(abortCommand);
            logWarning(`Aborted multipart upload for ${targetKey}`);
          } catch (abortError) {
            logError(`Failed to abort multipart upload: ${(abortError as Error).message}`);
          }
        }

        throw new Error(`Failed to transfer ${sourceKey} after ${maxRetries} retries: ${(error as Error).message}`);
      }

      retryCount++;
      resuming = true;

      // Exponential backoff before retry
      const backoffTime = Math.min(1000 * Math.pow(2, retryCount), 30000);
      logVerbose(`Retrying in ${backoffTime / 1000} seconds...`);
      await new Promise<void>(resolve => {
        setTimeout(() => {
          resolve();
        }, backoffTime);
      });
    }
  }
}

/**
 * Calculate the optimal part size for multipart upload based on file size
 * This balances overhead with resumability and efficiency
 */
function calculateOptimalPartSize(fileSize: number): number {
  // Default minimum size - 5MB (the minimum allowed by S3)
  const minPartSize = 5 * 1024 * 1024;

  // For small files, use minimum part size
  if (fileSize <= 100 * 1024 * 1024) { // 100MB
    return minPartSize;
  }

  // For medium files, use 10MB parts
  if (fileSize <= 1 * 1024 * 1024 * 1024) { // 1GB
    return 10 * 1024 * 1024;
  }

  // For large files, use 25MB parts
  if (fileSize <= 10 * 1024 * 1024 * 1024) { // 10GB
    return 25 * 1024 * 1024;
  }

  // For very large files, use 50MB parts
  // This balances the 10,000 part limit with the need to avoid large retransmissions
  return 50 * 1024 * 1024;
}

/**
 * Calculate hash of a file
 * @param data File data as a string path, Buffer, or Readable
 * @param algorithm Hash algorithm to use (default: md5)
 * @returns Promise<string> Hash digest in hex format
 */
export async function calculateHash(
  data: string | Readable | Buffer,
  algorithm = 'md5'
): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    try {
      const hash = crypto.createHash(algorithm);

      // If data is a file path
      if (typeof data === 'string') {
        const fileStream = fs.createReadStream(data);

        fileStream.on('error', (error) => {
          reject(error);
        });

        fileStream.on('data', (chunk) => {
          hash.update(chunk);
        });

        fileStream.on('end', () => {
          resolve(hash.digest('hex'));
        });
      }
      // If data is a Buffer
      else if (Buffer.isBuffer(data)) {
        hash.update(data);
        resolve(hash.digest('hex'));
      }
      // If data is a Readable
      else {
        data.on('error', (error) => {
          reject(error);
        });

        data.on('data', (chunk) => {
          hash.update(chunk);
        });

        data.on('end', () => {
          resolve(hash.digest('hex'));
        });
      }
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Get object hash from S3
 * @param client S3 client
 * @param bucket Bucket name
 * @param key Object key
 * @param algorithm Hash algorithm to use (default: md5)
 * @param tempDir Temporary directory for saving downloaded files
 * @returns Promise<string> Hash digest in hex format
 */
export async function getObjectHash(
  client: S3Client,
  bucket: string,
  key: string,
  algorithm = 'md5',
  tempDir?: string
): Promise<string> {
  logVerbose(`Calculating ${algorithm} hash for ${key}`);

  // Download object to a temporary file or buffer based on size
  const headCommand = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });

  try {
    const headResponse = await client.send(headCommand);
    const contentLength = headResponse.ContentLength || 0;

    // For small files (< 10MB), use memory mode
    if (contentLength < 10 * 1024 * 1024) {
      const data = await downloadObject(
        client,
        bucket,
        key,
        undefined,
        TransferMode.MEMORY
      );

      if (Buffer.isBuffer(data)) {
        return calculateHash(data, algorithm);
      } else {
        throw new Error(`Unexpected data type returned from downloadObject: ${typeof data}`);
      }
    }
    // For larger files, use disk mode
    else {
      // Create temp directory if needed
      const tmpDir = tempDir || path.join(process.cwd(), 'tmp');
      await fsExtra.ensureDir(tmpDir);

      const tempFilePath = await downloadObject(
        client,
        bucket,
        key,
        undefined,
        TransferMode.DISK,
        tmpDir
      ) as string;

      try {
        const hash = await calculateHash(tempFilePath, algorithm);

        // Clean up temp file
        try {
          await fs.promises.unlink(tempFilePath);
        } catch (unlinkError) {
          logWarning(`Failed to delete temporary file ${tempFilePath}: ${(unlinkError as Error).message}`);
        }

        return hash;
      } catch (error) {
        // Clean up temp file on error
        try {
          await fs.promises.unlink(tempFilePath);
        } catch {
          // Ignore error if we can't delete the file
        }

        throw error;
      }
    }
  } catch (error) {
    logError(`Failed to calculate hash for ${key}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * List all in-progress multipart uploads for a bucket
 * @param client S3 client
 * @param bucket Bucket name
 * @param prefix Optional key prefix filter
 * @returns Promise with array of in-progress uploads
 */
export async function listMultipartUploads(
  client: S3Client,
  bucket: string,
  prefix?: string
): Promise<Array<{ Key: string; UploadId: string; Initiated?: Date }>> {
  try {
    const command = new ListMultipartUploadsCommand({
      Bucket: bucket,
      Prefix: prefix
    });

    const response: ListMultipartUploadsCommandOutput = await client.send(command);

    // Extract the uploads
    const uploads = response.Uploads || [];
    return uploads.map(upload => ({
      Key: upload.Key || '',
      UploadId: upload.UploadId || '',
      Initiated: upload.Initiated
    })).filter(upload => upload.Key && upload.UploadId);
  } catch (error) {
    logError(`Failed to list multipart uploads for ${bucket}: ${(error as Error).message}`);
    return [];
  }
}

/**
 * Abort a specific multipart upload
 * @param client S3 client
 * @param bucket Bucket name
 * @param key Object key
 * @param uploadId The ID of the multipart upload to abort
 * @returns Promise<boolean> indicates if abort was successful
 */
export async function abortMultipartUpload(
  client: S3Client,
  bucket: string,
  key: string,
  uploadId: string
): Promise<boolean> {
  try {
    const command = new AbortMultipartUploadCommand({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId
    });

    await client.send(command);
    logVerbose(`Successfully aborted multipart upload for ${key} (UploadId: ${uploadId})`);
    return true;
  } catch (error) {
    logError(`Failed to abort multipart upload for ${key}: ${(error as Error).message}`);
    return false;
  }
}

/**
 * Abort all in-progress multipart uploads for a set of keys
 * @param client S3 client
 * @param bucket Bucket name
 * @param keys Array of keys to check for in-progress uploads
 * @returns Promise with results of abort operations
 */
export async function abortMultipartUploadsForKeys(
  client: S3Client,
  bucket: string,
  keys: string[]
): Promise<{
  totalFound: number;
  aborted: number;
  failed: number;
  details: Array<{ key: string; uploadId: string; success: boolean }>
}> {
  const result = {
    totalFound: 0,
    aborted: 0,
    failed: 0,
    details: [] as Array<{ key: string; uploadId: string; success: boolean }>
  };

  try {
    // Get all multipart uploads
    const uploads = await listMultipartUploads(client, bucket);

    // Filter for uploads that match our keys
    const matchingUploads = uploads.filter(upload => keys.includes(upload.Key));
    result.totalFound = matchingUploads.length;

    if (matchingUploads.length === 0) {
      return result;
    }

    logVerbose(`Found ${matchingUploads.length} in-progress multipart uploads to abort`);

    // Abort each matching upload
    for (const upload of matchingUploads) {
      const success = await abortMultipartUpload(client, bucket, upload.Key, upload.UploadId);

      if (success) {
        result.aborted++;
      } else {
        result.failed++;
      }

      result.details.push({
        key: upload.Key,
        uploadId: upload.UploadId,
        success
      });
    }

    return result;
  } catch (error) {
    logError(`Error in abortMultipartUploadsForKeys: ${(error as Error).message}`);
    return result;
  }
}
