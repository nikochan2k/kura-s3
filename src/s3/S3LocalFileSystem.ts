import { S3 } from "aws-sdk";
import { AbstractAccessor, AbstractLocalFileSystem, normalizePath } from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3FileSystemOptions } from "./S3FileSystemOption";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  private rootDir: string;

  constructor(
    private config: S3.ClientConfiguration,
    private bucket: string,
    roorDir: string,
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    this.rootDir = normalizePath(roorDir);
  }

  protected createAccessor(): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>((resolve, reject) => {
      const accessor = new S3Accessor(
        this.config,
        this.bucket,
        this.rootDir,
        this.s3Options
      );
      accessor
        .init()
        .then(() => resolve(accessor))
        .catch((e) => reject(e));
    });
  }
}
