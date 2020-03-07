import {
  AbstractAccessor,
  AbstractLocalFileSystem,
  Permission,
  LAST_DIR_SEPARATORS
} from "kura";
import { S3 } from "aws-sdk";
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
    this.rootDir = rootDir.replace(LAST_DIR_SEPARATORS, "");
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
