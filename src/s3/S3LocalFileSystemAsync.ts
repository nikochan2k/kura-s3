import { S3 } from "aws-sdk";
import { LocalFileSystemAsync } from "kura";
import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { S3LocalFileSystem } from "./S3LocalFileSystem";

export class S3LocalFileSystemAsync extends LocalFileSystemAsync {
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    rootDir: string,
    options?: FileSystemOptions
  ) {
    super(new S3LocalFileSystem(config, bucket, rootDir, options));
  }
}
