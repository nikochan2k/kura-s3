import { DIR_SEPARATOR, DirectoryEntry, FileSystem } from "kura";
import { S3 } from "aws-sdk";
import { S3DirectoryEntry } from "./S3DirectoryEntry";

export class S3FileSystem implements FileSystem {
  get name() {
    return this.bucket;
  }
  root: DirectoryEntry;
  s3: S3;

  constructor(public options: S3.ClientConfiguration, public bucket: string) {
    this.s3 = new S3(options);
    this.root = new S3DirectoryEntry({
      filesystem: this,
      name: "",
      fullPath: DIR_SEPARATOR,
      lastModified: null,
      size: null
    });
  }
}
