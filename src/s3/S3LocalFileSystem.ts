import { S3 } from "aws-sdk";
import {
  AbstractAccessor,
  AbstractLocalFileSystem,
  normalizePath,
  Permission
} from "kura";
import { S3Accessor } from "./S3Accessor";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  private rootDir: string;

  constructor(config: S3.ClientConfiguration, bucket: string, rootDir: string);
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    roorDir: string,
    useIndex: boolean
  );
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    roorDir: string,
    permission: Permission
  );
  constructor(
    private config: S3.ClientConfiguration,
    private bucket: string,
    rootDir: string,
    value?: any
  ) {
    super(value);
    this.rootDir = normalizePath(rootDir);
  }

  protected createAccessor(): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>(resolve => {
      const accessor = new S3Accessor(
        this.config,
        this.bucket,
        this.rootDir,
        this.permission
      );
      resolve(accessor);
    });
  }
}
