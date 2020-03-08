import { S3 } from "aws-sdk";
import { AbstractAccessor, AbstractLocalFileSystem, normalizePath } from "kura";
import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { S3Accessor } from "./S3Accessor";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  private rootDir: string;

  constructor(
    private config: S3.ClientConfiguration,
    private bucket: string,
    roorDir: string,
    options?: FileSystemOptions
  ) {
    super(options);
    this.rootDir = normalizePath(roorDir);
  }

  protected createAccessor(): Promise<AbstractAccessor> {
    return new Promise<S3Accessor>(resolve => {
      const accessor = new S3Accessor(
        this.config,
        this.bucket,
        this.rootDir,
        this.options
      );
      resolve(accessor);
    });
  }
}
