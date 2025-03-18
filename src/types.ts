export interface S3Credentials {
  endpoint: string;
  accessKey: string;
  secretKey: string;
  region: string;
  bucket: string;
  // Optional parameters
  forcePathStyle?: boolean;
  useAccelerateEndpoint?: boolean;
  signatureVersion?: 'v2' | 'v4'; // AWS signature version
}

// Transfer mode enumeration
export enum TransferMode {
  AUTO = 'auto',           // Automatically select the optimal transfer mode based on file size
  DISK = 'disk',           // Use disk as intermediate storage (balance between performance and memory usage)
  MEMORY = 'memory',       // Use memory as intermediate storage (best for small files, highest performance)
  STREAM = 'stream'        // Use streaming transfer (best for large files, minimal memory usage)
}

export interface MigrationConfig {
  source: S3Credentials;
  target: S3Credentials;
  // Optional parameters
  concurrency?: number;
  prefix?: string;
  exclude?: string[];
  include?: string[];
  dryRun?: boolean;
  maxRetries?: number; // Maximum number of retry attempts for failed transfers
  verifyAfterMigration?: boolean; // Verify objects exist in target after migration
  verifyFileContentAfterMigration?: boolean; // Verify file content integrity by comparing hashes after migration
  purgeSourceAfterMigration?: boolean; // Delete objects from source after successful migration
  skipConfirmation?: boolean; // Skip confirmation prompts
  verbose?: boolean; // Enable verbose logging
  logFile?: string; // Log file path for saving detailed logs
  transferMode?: TransferMode; // File transfer mode
  tempDir?: string; // Temporary directory for disk transfer mode
  largeFileSizeThreshold?: number; // Large file threshold (bytes), files larger than this will use alternative transfer modes
  skipTargetExistenceCheck?: boolean; // Skip checking if files already exist in target bucket
}

// File transfer status
export enum TransferStatus {
  PENDING = 'pending',
  DOWNLOADING = 'downloading',
  UPLOADING = 'uploading',
  STREAMING = 'streaming',
  SUCCEEDED = 'succeeded',
  FAILED = 'failed',
  SKIPPED = 'skipped',
  RETRYING = 'retrying',
  VERIFICATION_FAILED = 'verification_failed',
  CONTENT_VERIFICATION_FAILED = 'content_verification_failed'
}

// File transfer information
export interface FileTransfer {
  key: string;
  size?: number;
  status: TransferStatus;
  retryCount: number;
  error?: string;
  downloadSpeed?: number; // bytes per second
  uploadSpeed?: number; // bytes per second
  progress?: number; // 0-100
  verified?: boolean; // Whether the file was verified in the target bucket
  contentVerified?: boolean; // Whether the file content was verified by hash comparison
  sourceHash?: string; // Hash of the source file content
  targetHash?: string; // Hash of the target file content
  transferMode?: TransferMode; // Transfer mode used
  tempFilePath?: string; // Temporary file path (for disk transfer mode)
}
