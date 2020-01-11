import {
  AbstractFileEntry,
  blobToFile,
  ErrorCallback,
  FileCallback,
  FileSystemParams,
  FileWriterCallback,
  onError
} from "kura";
import { getKey } from "./S3Util";
import { S3EntrySupport } from "./S3EntrySupport";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileWriter } from "./S3FileWriter";

export interface S3FileParams extends FileSystemParams<S3FileSystem> {
  size: number;
}

export class S3FileEntry extends AbstractFileEntry<S3FileSystem> {
  private s3FileWriter: S3FileWriter;

  constructor(params: FileSystemParams<S3FileSystem>) {
    super(params, new S3EntrySupport(params));
  }

  createWriter(
    successCallback: FileWriterCallback,
    errorCallback?: ErrorCallback
  ): void {
    if (!this.s3FileWriter) {
      this.file(file => {
        successCallback(this.s3FileWriter);
      }, errorCallback);
    } else {
      successCallback(this.s3FileWriter);
    }
  }

  async delete(): Promise<void> {
    const key = getKey(this.fullPath);
    await this.filesystem.s3
      .deleteObject({ Bucket: this.filesystem.bucket, Key: key })
      .promise();
  }

  file(successCallback: FileCallback, errorCallback?: ErrorCallback): void {
    if (this.s3FileWriter) {
      successCallback(this.s3FileWriter.file);
      return;
    }
    if (this.size === 0) {
      const file = blobToFile([], this.name, this.params.lastModified);
      this.s3FileWriter = new S3FileWriter(this, file);
      successCallback(file);
      return;
    }
    const filesystem = this.filesystem;
    filesystem.s3.getObject(
      { Bucket: filesystem.bucket, Key: getKey(this.fullPath) },
      (err, data) => {
        if (err) {
          onError(err, errorCallback);
        } else {
          this.params.size = data.ContentLength;
          const body = data.Body;
          if (
            body instanceof Buffer ||
            body instanceof ArrayBuffer ||
            body instanceof Blob ||
            typeof body === "string"
          ) {
            const file = blobToFile(
              [body],
              this.name,
              this.params.lastModified,
              data.ContentType
            );
            this.s3FileWriter = new S3FileWriter(this, file);
            successCallback(file);
          } else {
            onError(new Error("Unknown data type"), errorCallback);
          }
        }
      }
    );
  }
}
