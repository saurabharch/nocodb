import path from 'path';
import { Readable } from 'stream';
import { Process, Processor } from '@nestjs/bull';
import { Job } from 'bull';
import { Logger } from '@nestjs/common';
import sharp from 'sharp';
import type { AttachmentResType } from 'nocodb-sdk';
import type { ThumbnailGeneratorJobData } from '~/interface/Jobs';
import { JOBS_QUEUE, JobTypes } from '~/interface/Jobs';
import NcPluginMgrv2 from '~/helpers/NcPluginMgrv2';
import { PresignedUrl } from '~/models';
import { AttachmentsService } from '~/modules/jobs/jobs/thumbnail-generator/attachments.service';

@Processor(JOBS_QUEUE)
export class ThumbnailGeneratorProcessor {
  constructor(private readonly attachmentsService: AttachmentsService) {}

  private logger = new Logger(ThumbnailGeneratorProcessor.name);

  @Process(JobTypes.ThumbnailGenerator)
  async job(job: Job<ThumbnailGeneratorJobData>) {
    try {
      const { attachments } = job.data;

      for (const attachment of attachments) {
        if (attachment.mimetype.startsWith('image/')) {
          await this.generateThumbnail(attachment);
        }
      }
    } catch (error) {
      console.log(error);
    }
  }

  private async generateThumbnail(
    attachment: AttachmentResType,
  ): Promise<{ [key: string]: string }> {
    let url;
    let signedUrl;
    let file;

    let relativePath;

    if (attachment?.path) {
      relativePath = attachment.path.replace(/^download\//, '');
      url = await PresignedUrl.getSignedUrl({
        path: attachment.path.replace(/^download\//, ''),
      });
    } else if (attachment?.url) {
      if (attachment.url.includes('.amazonaws.com/')) {
        relativePath = decodeURI(attachment.url.split('.amazonaws.com/')[1]);

        signedUrl = await PresignedUrl.getSignedUrl({
          s3: true,
          path: relativePath,
        });
      } else {
        relativePath = attachment.url;
      }

      file = (
        await fetch(signedUrl, {
          method: 'GET',
        })
      ).arrayBuffer();
    }

    if (url && !signedUrl) {
      const fullPath = await PresignedUrl.getPath(`${url}`);
      const queryHelper = fullPath.split('?');

      const fpath = queryHelper[0];

      const tempPath = await this.attachmentsService.getFile({
        path: path.join('nc', 'uploads', fpath),
      });

      file = tempPath.path;
    }

    const thumbnailPaths = {
      card_cover: path.join(
        'nc',
        'uploads',
        'thumbnails',
        relativePath,
        'card_cover.jpg',
      ),
      small: path.join(
        'nc',
        'uploads',
        'thumbnails',
        relativePath,
        'small.jpg',
      ),
      tiny: path.join('nc', 'uploads', 'thumbnails', relativePath, 'tiny.jpg'),
    };

    try {
      const storageAdapter = await NcPluginMgrv2.storageAdapter();

      await Promise.all(
        Object.entries(thumbnailPaths).map(async ([size, thumbnailPath]) => {
          let height;
          switch (size) {
            case 'card_cover':
              height = 512;
              break;
            case 'small':
              height = 128;
              break;
            case 'tiny':
              height = 64;
              break;
            default:
              throw new Error(`Unknown thumbnail size: ${size}`);
          }

          const resizedImage = await sharp(file)
            .resize(undefined, height, {
              fit: sharp.fit.cover,
              kernel: 'lanczos3',
            })
            .toBuffer();

          await (storageAdapter as any).fileCreateByStream(
            thumbnailPath,
            Readable.from(resizedImage),
          );
        }),
      );

      return thumbnailPaths;
    } catch (error) {
      this.logger.error(
        `Failed to generate thumbnails for ${attachment.path}`,
        error,
      );
      throw error;
    }
  }
}
