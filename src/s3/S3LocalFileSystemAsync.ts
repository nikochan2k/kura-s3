import { S3 } from "aws-sdk";
import { LocalFileSystemAsync } from "kura";
import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { S3LocalFileSystem } from "./S3LocalFileSystem";
import { S3FileSystemOptions } from "./S3FileSystemOption";

export class S3LocalFileSystemAsync extends LocalFileSystemAsync {
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    rootDir: string,
    s3Option?: S3FileSystemOptions
  ) {
    super(new S3LocalFileSystem(config, bucket, rootDir, s3Option));
  }
}
