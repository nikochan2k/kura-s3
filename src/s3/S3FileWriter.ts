import { AbstractFileWriter, FileWriter } from "kura";
import {
  CompletedMultipartUpload,
  CompleteMultipartUploadRequest
} from "aws-sdk/clients/s3";
import { getKey } from "./S3Util";
import { S3FileEntry } from "./S3FileEntry";

const LIMIT = 1024 * 1024 * 5;

export class S3FileWriter extends AbstractFileWriter<S3FileEntry>
  implements FileWriter {
  constructor(s3FileEntry: S3FileEntry, file: File) {
    super(s3FileEntry, file);
  }

  multipartUpload(file: File, onsuccess: () => void) {
    const filesystem = this.fileEntry.filesystem;
    const s3 = filesystem.s3;
    const bucket = filesystem.bucket;
    const key = getKey(this.fileEntry.fullPath);
    s3.createMultipartUpload(
      {
        Bucket: bucket,
        Key: key,
        ContentType: "application/octet-stream"
      },
      async (err, res) => {
        if (err) {
          this.handleError(err);
          return;
        }

        try {
          const uploadId = res.UploadId;
          const partSize = LIMIT;
          const allSize = file.size;

          if (this.onwritestart) {
            this.onwritestart(null); // TODO
          }

          const multipartMap: CompletedMultipartUpload = {
            Parts: []
          };

          for (
            let rangeStart = 0, partNum = 1;
            rangeStart < allSize;
            rangeStart += partSize, partNum++
          ) {
            const end = Math.min(rangeStart + partSize, allSize);

            let body: any;
            if (typeof process === "object") {
              // Node
              const sendData = await new Promise<Uint8Array>(resolve => {
                const fileReader = new FileReader();
                fileReader.onloadend = event => {
                  const data = event.target.result as ArrayBuffer;
                  const byte = new Uint8Array(data);
                  resolve(byte);
                };
                const blob = file.slice(rangeStart, end);
                fileReader.readAsArrayBuffer(blob);
              });
              body = new Buffer(sendData);
            } else {
              // Browser
              body = file.slice(rangeStart, end);
            }
            const partUpload = await s3
              .uploadPart({
                Bucket: bucket,
                Key: key,
                Body: body,
                PartNumber: partNum,
                UploadId: uploadId
              })
              .promise();

            multipartMap.Parts.push({
              ETag: partUpload.ETag,
              PartNumber: partNum
            });
          }

          const doneParams: CompleteMultipartUploadRequest = {
            Bucket: filesystem.bucket,
            Key: key,
            MultipartUpload: multipartMap,
            UploadId: uploadId
          };

          await s3.completeMultipartUpload(doneParams).promise();

          const data = await s3
            .headObject({ Bucket: bucket, Key: key })
            .promise();
          this.fileEntry.params.lastModified = data.LastModified.getTime();
          onsuccess();
          if (this.onwriteend) {
            const evt: ProgressEvent<EventTarget> = {
              loaded: this.position,
              total: this.length,
              lengthComputable: true
            } as any;
            this.onwriteend(evt);
          }
        } catch (e) {
          this.handleError(e);
        }
      }
    );
  }

  async upload(file: File, onsuccess: () => void) {
    const filesystem = this.fileEntry.filesystem;
    const s3 = filesystem.s3;
    const bucket = filesystem.bucket;
    const key = getKey(this.fileEntry.fullPath);

    let body: any;
    if (typeof process === "object") {
      // Node
      const sendData = await new Promise<Uint8Array>(resolve => {
        const fileReader = new FileReader();
        fileReader.onloadend = event => {
          const data = event.target.result as ArrayBuffer;
          const byte = new Uint8Array(data);
          resolve(byte);
        };
        fileReader.readAsArrayBuffer(file);
      });
      body = new Buffer(sendData);
    } else {
      // Browser
      body = file;
    }
    s3.putObject({ Bucket: bucket, Key: key, Body: body }, async err => {
      if (err) {
        this.handleError(err);
        return;
      }

      const data = await s3.headObject({ Bucket: bucket, Key: key }).promise();
      this.fileEntry.params.lastModified = data.LastModified.getTime();
      onsuccess();
      if (this.onwriteend) {
        const evt: ProgressEvent<EventTarget> = {
          loaded: this.position,
          total: this.length,
          lengthComputable: true
        } as any;
        this.onwriteend(evt);
      }
    });
  }

  doWrite(file: File, onsuccess: () => void) {
    if (file.size <= LIMIT) {
      this.upload(file, onsuccess);
    } else {
      this.multipartUpload(file, onsuccess);
    }
  }
}
