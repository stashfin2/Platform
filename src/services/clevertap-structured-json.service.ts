import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { CleverTapJsonS3ClientFactory } from '../config/clevertap-json-s3.config';
import { LoggerService } from './logger.service';
import { CleverTapEvent } from './clevertap-redshift.service';

/**
 * CleverTap Structured JSON Service
 * Writes events to structured S3 paths: date=YYYY-MM-DD/type=events|profiles/event_name.json
 */
@Service()
export class CleverTapStructuredJsonService {
  constructor(
    @Inject() private readonly s3ClientFactory: CleverTapJsonS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Write events to structured S3 location
   * Groups by event_date and event_name
   * Returns S3 paths of created files
   * SKIPS profile_update events (they contain mostly null data)
   */
  async writeStructuredJson(events: CleverTapEvent[]): Promise<string[]> {
    if (events.length === 0) {
      return [];
    }

    // Filter out profile_update events (they have mostly null data)
    const eventDataOnly = events.filter(e => e.event_name !== 'profile_update');
    
    if (eventDataOnly.length === 0) {
      const sourceFile = events[0]?.source_file || 'unknown';
      this.logger.info('⏭️  Skipping profile file (contains only profile_update events with null data)', {
        sourceFile,
        totalEvents: events.length,
        skipped: events.length,
      });
      return [];
    }

    if (eventDataOnly.length < events.length) {
      this.logger.info('🔽 Filtered out profile_update events', {
        original: events.length,
        afterFilter: eventDataOnly.length,
        removed: events.length - eventDataOnly.length,
      });
    }

    const client = this.s3ClientFactory.getClient();
    const config = this.s3ClientFactory.getConfig();

    // Group events by date and event_name
    const grouped = this.groupEvents(eventDataOnly);
    const s3Paths: string[] = [];

    for (const [key, groupedEvents] of Object.entries(grouped)) {
      const [eventDate, eventName] = key.split('|');
      
      // All are event files now (profile_update filtered out)
      const type = 'events';
      
      // Build S3 path: date=YYYY-MM-DD/type=events/event_name.json
      const s3Key = `date=${eventDate}/type=${type}/${eventName}.json`;
      
      // Convert to newline-delimited JSON
      const jsonLines = groupedEvents.map(event => JSON.stringify(event));
      const jsonContent = jsonLines.join('\n') + '\n';

      try {
        this.logger.info(`📤 Uploading structured JSON to S3`, {
          s3Key,
          rowCount: groupedEvents.length,
          sizeKB: Math.floor(Buffer.byteLength(jsonContent, 'utf8') / 1024),
        });

        await client.send(new PutObjectCommand({
          Bucket: config.bucket,
          Key: s3Key,
          Body: jsonContent,
          ContentType: 'application/json',
        }));

        const fullPath = `s3://${config.bucket}/${s3Key}`;
        s3Paths.push(fullPath);

        this.logger.info(`✅ Uploaded structured JSON`, {
          s3Path: fullPath,
          rowCount: groupedEvents.length,
        });
      } catch (error) {
        this.logger.error(`❌ Failed to upload structured JSON`, error, {
          s3Key,
          rowCount: groupedEvents.length,
        });
        throw error;
      }
    }

    return s3Paths;
  }

  /**
   * Group events by event_date and event_name
   */
  private groupEvents(events: CleverTapEvent[]): { [key: string]: CleverTapEvent[] } {
    const grouped: { [key: string]: CleverTapEvent[] } = {};

    for (const event of events) {
      const key = `${event.event_date}|${event.event_name}`;
      if (!grouped[key]) {
        grouped[key] = [];
      }
      grouped[key].push(event);
    }

    return grouped;
  }
}



