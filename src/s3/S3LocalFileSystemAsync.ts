import { LocalFileSystemAsync } from "kura";
import { S3 } from "aws-sdk";
import { S3LocalFileSystem } from "./S3LocalFileSystem";

export class S3LocalFileSystemAsync extends LocalFileSystemAsync {
  constructor(bucket: string, private options: S3.ClientConfiguration) {
    super(new S3LocalFileSystem(bucket, options));
  }
}
