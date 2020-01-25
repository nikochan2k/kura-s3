import { LocalFileSystemAsync } from "kura";
import { S3 } from "aws-sdk";
import { S3LocalFileSystem } from "./S3LocalFileSystem";

export class S3LocalFileSystemAsync extends LocalFileSystemAsync {
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    useIndex?: boolean
  ) {
    super(new S3LocalFileSystem(config, bucket, useIndex));
  }
}
