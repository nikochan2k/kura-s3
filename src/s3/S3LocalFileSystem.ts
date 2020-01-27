import { AbstractAccessor, AbstractLocalFileSystem, Permission } from "kura";
import { S3 } from "aws-sdk";
import { S3Accessor } from "./S3Accessor";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  constructor(config: S3.ClientConfiguration, bucket: string);
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    useIndex: boolean
  );
  constructor(
    config: S3.ClientConfiguration,
    bucket: string,
    permission: Permission
  );
  constructor(
    private config: S3.ClientConfiguration,
    private bucket: string,
    value?: any
  ) {
    super(value);
  }

  protected createAccessor(): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>(resolve => {
      const accessor = new S3Accessor(
        this.config,
        this.bucket,
        this.permission
      );
      resolve(accessor);
    });
  }
}
