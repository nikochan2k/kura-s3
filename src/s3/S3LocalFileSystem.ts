import { AbstractAccessor, AbstractLocalFileSystem } from "kura";
import { S3 } from "aws-sdk";
import { S3Accessor } from "./S3Accessor";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  constructor(bucket: string, private options: S3.ClientConfiguration) {
    super(bucket);
  }

  protected createAccessor(useIndex: boolean): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>(resolve => {
      const accessor = new S3Accessor(this.options, this.bucket, useIndex);
      resolve(accessor);
    });
  }
}
