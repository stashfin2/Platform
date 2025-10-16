import 'reflect-metadata';
import { Service } from 'typedi';
import pino from 'pino';

@Service()
export class LoggerService {
  private logger: pino.Logger;

  constructor() {
    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      transport: process.env.NODE_ENV !== 'production' ? {
        target: 'pino-pretty',
        options: {
          colorize: true,
          translateTime: 'SYS:standard',
          ignore: 'pid,hostname',
        },
      } : undefined,
    });
  }

  info(message: string, context?: Record<string, any>): void {
    if (context) {
      this.logger.info(context, message);
    } else {
      this.logger.info(message);
    }
  }

  error(message: string, error?: Error | unknown, context?: Record<string, any>): void {
    const logContext = {
      ...context,
      error: error instanceof Error ? {
        message: error.message,
        stack: error.stack,
        name: error.name,
      } : error,
    };
    this.logger.error(logContext, message);
  }

  warn(message: string, context?: Record<string, any>): void {
    if (context) {
      this.logger.warn(context, message);
    } else {
      this.logger.warn(message);
    }
  }

  debug(message: string, context?: Record<string, any>): void {
    if (context) {
      this.logger.debug(context, message);
    } else {
      this.logger.debug(message);
    }
  }

  fatal(message: string, error?: Error | unknown, context?: Record<string, any>): void {
    const logContext = {
      ...context,
      error: error instanceof Error ? {
        message: error.message,
        stack: error.stack,
        name: error.name,
      } : error,
    };
    this.logger.fatal(logContext, message);
  }
}

