import { AbstractAccessor, AbstractLocalFileSystem } from "kura";
import { S3 } from "aws-sdk";
import { S3Accessor } from "./S3Accessor";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  constructor(
    private config: S3.ClientConfiguration,
    bucket: string,
    useIndex: boolean
  ) {
    super(bucket, useIndex);
  }

  protected createAccessor(useIndex: boolean): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>(resolve => {
      const accessor = new S3Accessor(this.config, this.bucket, useIndex);
      resolve(accessor);
    });
  }
}
