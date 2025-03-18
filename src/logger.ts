// Node.js built-in modules
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// Third-party dependencies
import chalk from 'chalk';

// Types
import type { MigrationConfig } from './types';

// Log levels
export enum LogLevel {
  INFO = 'INFO',
  SUCCESS = 'SUCCESS',
  WARNING = 'WARNING',
  ERROR = 'ERROR',
  DEBUG = 'DEBUG',
}

let config: MigrationConfig | null = null;
let logStream: fs.WriteStream | null = null;
let executionId: string;

/**
 * Generate a unique execution ID
 */
function generateExecutionId(): string {
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 10000);
  return `${timestamp}-${random}`;
}

/**
 * Initialize the logger with the given configuration
 */
export function initLogger(migrationConfig: MigrationConfig): void {
  config = migrationConfig;
  executionId = generateExecutionId();

  // If log file is specified, create or append to the file
  if (config.logFile) {
    try {
      const logDir = path.dirname(config.logFile);

      // Create directory if it doesn't exist
      if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir, { recursive: true });
      }

      // Open log file for writing (append mode)
      logStream = fs.createWriteStream(config.logFile, { flags: 'a' });

      // Write initial log entry with an eye-catching separator
      const timestamp = new Date().toISOString();
      const processInfo = `PID: ${process.pid}, User: ${process.env.USERNAME || process.env.USER || 'unknown'}`;
      const systemInfo = `OS: ${os.platform()} ${os.release()}, Hostname: ${os.hostname()}`;

      logStream.write('\n');
      logStream.write('='.repeat(80) + '\n');
      logStream.write(`== S3 MIGRATION EXECUTION STARTED AT ${timestamp} ==\n`);
      logStream.write(`== Execution ID: ${executionId} ==\n`);
      logStream.write(`== ${processInfo} ==\n`);
      logStream.write(`== ${systemInfo} ==\n`);
      logStream.write('='.repeat(80) + '\n\n');

      // Log initial configuration
      log(LogLevel.INFO, 'Migration configuration:');
      log(LogLevel.INFO, `Source: ${config.source.endpoint}/${config.source.bucket}`);
      log(LogLevel.INFO, `Target: ${config.target.endpoint}/${config.target.bucket}`);
      log(LogLevel.INFO, `Concurrency: ${config.concurrency}`);
      log(LogLevel.INFO, `Dry run: ${config.dryRun}`);
      if (config.prefix) {
        log(LogLevel.INFO, `Prefix: ${config.prefix}`);
      }
    } catch (error) {
      console.error(chalk.red(`Failed to open log file: ${(error as Error).message}`));
      // Continue without file logging
    }
  }
}

/**
 * Close the logger and release resources
 */
export function closeLogger(): void {
  if (logStream) {
    const timestamp = new Date().toISOString();

    logStream.write('\n');
    logStream.write('='.repeat(80) + '\n');
    logStream.write(`== S3 MIGRATION EXECUTION COMPLETED AT ${timestamp} ==\n`);
    logStream.write(`== Execution ID: ${executionId} ==\n`);
    logStream.write('='.repeat(80) + '\n');

    logStream.end();
    logStream = null;
  }
  config = null;
}

/**
 * Log a message with the specified level
 */
export function log(level: LogLevel, message: string, skipConsole = false): void {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] [${level}] [${executionId}] ${message}`;

  // Log to file if enabled
  if (logStream) {
    logStream.write(logMessage + '\n');
  }

  // Skip console output if requested
  if (skipConsole) {
    return;
  }

  // Only show verbose/debug logs in console if verbose mode is enabled
  if (level === LogLevel.DEBUG && !(config && config.verbose)) {
    return;
  }

  // Format message for console output with level prefix and appropriate color
  let consoleMessage: string;

  switch (level) {
    case LogLevel.INFO:
      consoleMessage = chalk.blue(`[INFO] ${message}`);
      break;
    case LogLevel.SUCCESS:
      consoleMessage = chalk.green(`[SUCCESS] ${message}`);
      break;
    case LogLevel.WARNING:
      consoleMessage = chalk.yellow(`[WARNING] ${message}`);
      break;
    case LogLevel.ERROR:
      consoleMessage = chalk.red(`[ERROR] ${message}`);
      break;
    case LogLevel.DEBUG:
      consoleMessage = chalk.gray(`[DEBUG] ${message}`);
      break;
    default:
      consoleMessage = message;
  }

  console.log(consoleMessage);
}

/**
 * Log an error with optional error object details
 */
export function logError(message: string, error?: Error, skipConsole = false): void {
  log(LogLevel.ERROR, message, skipConsole);

  // Log detailed error information in verbose mode or to file
  if (error) {
    const errorDetails = `${error.name}: ${error.message}\n${error.stack || '(No stack trace)'}`;

    // Always log error details to file
    if (logStream) {
      logStream.write(`[${new Date().toISOString()}] [ERROR_DETAILS] [${executionId}] ${errorDetails}\n`);
    }

    // Log to console only in verbose mode
    if (config && config.verbose && !skipConsole) {
      console.log(chalk.red(errorDetails));
    }
  }
}

/**
 * Log verbose information (only in verbose mode or to file)
 */
export function logVerbose(message: string): void {
  log(LogLevel.DEBUG, message);
}

/**
 * Log a success message
 */
export function logSuccess(message: string): void {
  log(LogLevel.SUCCESS, message);
}

/**
 * Log a warning message
 */
export function logWarning(message: string): void {
  log(LogLevel.WARNING, message);
}

/**
 * Log an info message with custom color
 */
export function logInfo(message: string, color?: (message: string) => string): void {
  if (color) {
    // Use custom color if provided
    const customMessage = color(message);
    log(LogLevel.INFO, customMessage, false);
  } else {
    // Use default info color
    log(LogLevel.INFO, message);
  }
}

/**
 * Log message only to file, never to console (for stalled transfer detection)
 * This is used for logging stalled transfers without cluttering the console
 */
export function logSilent(message: string, level: LogLevel = LogLevel.WARNING): void {
  // Write to log file only
  if (logStream) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${level}] [${executionId}] ${message}`;
    logStream.write(logMessage + '\n');
  }
}
