// Node.js built-in modules
import fs from 'node:fs';
import path from 'node:path';

// Third-party dependencies
import yaml from 'js-yaml';

// Types
import type { MigrationConfig, S3Credentials } from './types';

/**
 * Load and parse the migration configuration file
 * @param configPath Path to the configuration file
 * @returns Parsed migration configuration
 */
export function loadConfig(configPath: string): MigrationConfig {
  const resolvedPath = path.resolve(configPath);

  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`Configuration file not found: ${resolvedPath}`);
  }

  const fileContent = fs.readFileSync(resolvedPath, 'utf-8');
  const fileExt = path.extname(resolvedPath).toLowerCase();

  let config: MigrationConfig;

  try {
    if (fileExt === '.json') {
      config = JSON.parse(fileContent) as MigrationConfig;
    } else if (fileExt === '.yaml' || fileExt === '.yml') {
      config = yaml.load(fileContent) as MigrationConfig;
    } else {
      throw new Error(`Unsupported configuration file format: ${fileExt}`);
    }
  } catch (error) {
    throw new Error(`Failed to parse configuration file: ${(error as Error).message}`);
  }

  // Validate required fields
  validateConfig(config);

  return config;
}

/**
 * Validate the migration configuration
 * @param config Migration configuration to validate
 */
function validateConfig(config: MigrationConfig): void {
  if (!config.source) {
    throw new Error('Source configuration is missing');
  }

  if (!config.target) {
    throw new Error('Target configuration is missing');
  }

  const requiredCredFields: Array<keyof S3Credentials> = [
    'endpoint', 'accessKey', 'secretKey', 'region', 'bucket'
  ];

  for (const field of requiredCredFields) {
    if (!config.source[field]) {
      throw new Error(`Source ${field} is missing`);
    }

    if (!config.target[field]) {
      throw new Error(`Target ${field} is missing`);
    }
  }

  // Set default values
  if (typeof config.concurrency !== 'number' || config.concurrency <= 0) {
    config.concurrency = 5; // Default concurrency
  }

  // Set default max retries
  if (typeof config.maxRetries !== 'number' || config.maxRetries < 0) {
    config.maxRetries = 3; // Default max retries
  }

  // Set default verification option
  if (typeof config.verifyAfterMigration !== 'boolean') {
    config.verifyAfterMigration = true; // Default to verify after migration
  }

  // Set default content verification option
  if (typeof config.verifyFileContentAfterMigration !== 'boolean') {
    config.verifyFileContentAfterMigration = false; // Default to not verify file content
  }

  // Set default purge source option
  if (typeof config.purgeSourceAfterMigration !== 'boolean') {
    config.purgeSourceAfterMigration = false; // Default to not purge source
  }

  // Set default skip confirmation option
  if (typeof config.skipConfirmation !== 'boolean') {
    config.skipConfirmation = false; // Default to require confirmation
  }

  // Set default verbose option
  if (typeof config.verbose !== 'boolean') {
    config.verbose = false; // Default to non-verbose mode
  }

  // Ensure logFile is a string if provided
  if (config.logFile !== undefined && typeof config.logFile !== 'string') {
    throw new Error('Log file path must be a string');
  }

  // Ensure include is an array if provided
  if (config.include !== undefined && !Array.isArray(config.include)) {
    throw new Error('Include patterns must be an array');
  }

  // Ensure exclude is an array if provided
  if (config.exclude !== undefined && !Array.isArray(config.exclude)) {
    throw new Error('Exclude patterns must be an array');
  }

  // Ensure prefix is a string if provided
  if (config.prefix !== undefined && typeof config.prefix !== 'string') {
    throw new Error('Prefix must be a string');
  }

  // Validate signature version if provided
  if (config.source.signatureVersion !== undefined &&
    !['v2', 'v4'].includes(config.source.signatureVersion)) {
    throw new Error('Source signature version must be either "v2" or "v4"');
  }

  if (config.target.signatureVersion !== undefined &&
    !['v2', 'v4'].includes(config.target.signatureVersion)) {
    throw new Error('Target signature version must be either "v2" or "v4"');
  }
}
