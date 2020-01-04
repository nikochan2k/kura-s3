import {
  blobToFile,
  ErrorCallback,
  FileCallback,
  FileEntry,
  FileSystemParams,
  FileWriterCallback,
  onError,
  VoidCallback
} from "kura";
import { getKey } from "./S3Util";
import { S3Entry } from "./S3Entry";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileWriter } from "./S3FileWriter";

export interface S3FileParams extends FileSystemParams<S3FileSystem> {
  size: number;
}

export class S3FileEntry extends S3Entry implements FileEntry {
  isFile = true;
  isDirectory = false;
  get size() {
    return this.params.size;
  }
  private s3FileWriter: S3FileWriter;

  constructor(params: S3FileParams) {
    super(params);
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

  remove(successCallback: VoidCallback, errorCallback?: ErrorCallback): void {
    const key = getKey(this.fullPath);
    this.filesystem.s3.deleteObject(
      { Bucket: this.filesystem.bucket, Key: key },
      err => {
        if (err) {
          if (err.statusCode !== 404) {
            onError(err, errorCallback);
            return;
          }
        }
        successCallback();
      }
    );
  }
}
