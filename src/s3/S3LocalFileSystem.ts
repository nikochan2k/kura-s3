import {
  AbstractLocalFileSystem,
  EntryCallback,
  ErrorCallback,
  FileSystemCallback,
  NotImplementedError
} from "kura";
import { S3 } from "aws-sdk";
import { S3FileSystem } from "./S3FileSystem";

if (window.TEMPORARY == null) {
  window.TEMPORARY = 0;
}
if (window.PERSISTENT == null) {
  window.PERSISTENT = 1;
}

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  constructor(bucket: string, private options: S3.ClientConfiguration) {
    super(bucket);
  }

  requestFileSystem(
    type: number,
    size: number,
    successCallback: FileSystemCallback,
    errorCallback?: ErrorCallback
  ): void {
    if (type === this.TEMPORARY) {
      throw new Error("No temporary storage");
    }

    try {
      const s3FileSystem = new S3FileSystem(this.options, this.bucket);
      successCallback(s3FileSystem);
    } catch (err) {
      errorCallback(err);
    }
  }

  resolveLocalFileSystemURL(
    url: string,
    successCallback: EntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    throw new NotImplementedError("", url);
  }
}
