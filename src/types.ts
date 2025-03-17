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

// 添加传输模式枚举
export enum TransferMode {
  AUTO = 'auto',           // 自动选择最佳模式
  MEMORY = 'memory',       // 使用内存中转
  DISK = 'disk',           // 使用磁盘中转
  STREAM = 'stream'        // 使用流传输（不缓存整个文件）
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
  transferMode?: TransferMode; // 文件传输模式
  tempDir?: string; // 临时文件目录（用于磁盘传输模式）
  largeFileSizeThreshold?: number; // 大文件阈值（字节），超过此大小将使用其他传输模式
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
  transferMode?: TransferMode; // 使用的传输模式
  tempFilePath?: string; // 临时文件路径（用于磁盘传输模式）
}
