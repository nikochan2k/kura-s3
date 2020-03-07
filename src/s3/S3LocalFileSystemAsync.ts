import { LocalFileSystemAsync, Permission } from "kura";
import { S3 } from "aws-sdk";
import { S3LocalFileSystem } from "./S3LocalFileSystem";

export class S3LocalFileSystemAsync extends LocalFileSystemAsync {
  constructor(config: S3.ClientConfiguration, bucket: string, rootDir: string);
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    rootDir: string,
    useIndex: boolean
  );
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    rootDir: string,
    permission: Permission
  );
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    rootDir: string,
    value?: any
  ) {
    super(new S3LocalFileSystem(config, bucket, rootDir, value));
  }
}
