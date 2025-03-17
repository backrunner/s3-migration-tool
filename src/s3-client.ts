import type {
  ListObjectsV2CommandOutput
} from '@aws-sdk/client-s3';
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import type { Readable } from 'stream';
import * as fs from 'fs';
import * as fsExtra from 'fs-extra';
import * as path from 'path';
import { v4 as uuidv4 } from 'uuid';
import type { S3Credentials } from './types';
import { TransferMode } from './types';

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
    console.error(`Failed to get size for ${key}: ${(error as Error).message}`);
    return 0;
  }
}

/**
 * Download an object from S3 with progress tracking
 * Can work in memory, disk or stream mode depending on the transferMode parameter
 */
export async function downloadObject(
  client: S3Client,
  bucket: string,
  key: string,
  onProgress?: (downloaded: number, total: number, speed: number) => void,
  transferMode: TransferMode = TransferMode.MEMORY,
  tempDir?: string
): Promise<Buffer | string | Readable> {
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

  // Memory mode (default) - same as before
  // If no progress callback, just return the data directly
  if (!onProgress || contentLength <= 0) {
    const data = await responseBody.transformToByteArray();
    return Buffer.from(data);
  }

  // With progress tracking
  return new Promise<Buffer>((resolve, reject) => {
    try {
      let lastReportTime = Date.now();
      let lastReportedBytes = 0;

      // Simulate progress updates since we can't get real-time progress
      // This is a workaround until we can find a better way to track progress
      const startTime = Date.now();
      const progressInterval = setInterval(() => {
        const elapsedMs = Date.now() - startTime;
        // Estimate progress based on elapsed time (this is not accurate)
        const estimatedProgress = Math.min(0.95, elapsedMs / 10000); // Assume 10s for full download
        const estimatedBytes = Math.floor(contentLength * estimatedProgress);

        if (estimatedBytes > lastReportedBytes) {
          const timeDiff = Date.now() - lastReportTime;
          const byteDiff = estimatedBytes - lastReportedBytes;
          const speed = byteDiff / (timeDiff / 1000);

          onProgress(estimatedBytes, contentLength, speed);

          lastReportTime = Date.now();
          lastReportedBytes = estimatedBytes;
        }
      }, 100);

      // Get the actual data
      responseBody.transformToByteArray()
        .then(data => {
          // Clear the progress interval
          clearInterval(progressInterval);

          // Report final progress
          onProgress(contentLength, contentLength, 0);

          resolve(Buffer.from(data));
        })
        .catch(error => {
          clearInterval(progressInterval);
          reject(error);
        });
    } catch (error) {
      reject(error);
    }
  });
}

/**
 * Upload an object to S3 with progress tracking
 * Can work with memory buffer, file path or stream depending on the input
 */
export async function uploadObject(
  client: S3Client,
  bucket: string,
  key: string,
  body: Buffer | string | Readable,
  onProgress?: (uploaded: number, total: number, speed: number) => void,
  removeSourceAfterUpload = false
): Promise<void> {
  let uploadBody: Buffer | Readable;
  let totalBytes: number;

  // If body is a string, assume it's a file path
  if (typeof body === 'string') {
    totalBytes = fs.statSync(body).size;
    uploadBody = fs.createReadStream(body);
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
    uploadBody = body;
  }

  let uploadedBytes = 0;
  let lastReportTime = Date.now();
  let lastReportedBytes = 0;

  const upload = new Upload({
    client,
    params: {
      Bucket: bucket,
      Key: key,
      Body: uploadBody,
    },
    queueSize: 4, // Adjust based on your needs
  });

  // Add progress listener
  if (onProgress) {
    upload.on('httpUploadProgress', (progress) => {
      if (progress.loaded !== undefined) {
        uploadedBytes = progress.loaded;

        const now = Date.now();
        const timeDiff = now - lastReportTime;

        // Update progress every 100ms
        if (timeDiff >= 100) {
          const byteDiff = uploadedBytes - lastReportedBytes;
          const speed = byteDiff / (timeDiff / 1000); // bytes per second

          onProgress(uploadedBytes, totalBytes, speed);

          lastReportTime = now;
          lastReportedBytes = uploadedBytes;
        }
      }
    });
  }

  await upload.done();

  // Final progress report
  if (onProgress) {
    onProgress(totalBytes, totalBytes, 0);
  }

  // Remove source file if it's a file path and removeSourceAfterUpload is true
  if (removeSourceAfterUpload && typeof body === 'string') {
    try {
      await fs.promises.unlink(body);
    } catch (error) {
      console.error(`Failed to remove temp file ${body}: ${(error as Error).message}`);
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
