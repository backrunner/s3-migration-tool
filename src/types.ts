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
  purgeSourceAfterMigration?: boolean; // Delete objects from source after successful migration
  skipConfirmation?: boolean; // Skip confirmation prompts
  verbose?: boolean; // Enable verbose logging
  logFile?: string; // Log file path for saving detailed logs
}

// File transfer status
export enum TransferStatus {
  PENDING = 'pending',
  DOWNLOADING = 'downloading',
  UPLOADING = 'uploading',
  SUCCEEDED = 'succeeded',
  FAILED = 'failed',
  SKIPPED = 'skipped',
  RETRYING = 'retrying',
  VERIFICATION_FAILED = 'verification_failed'
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
}
