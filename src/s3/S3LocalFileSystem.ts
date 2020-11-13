import { S3 } from "aws-sdk";
import { AbstractAccessor, AbstractLocalFileSystem, normalizePath } from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3FileSystemOptions } from "./S3FileSystemOption";

export class S3LocalFileSystem extends AbstractLocalFileSystem {
  // #region Properties (1)

  private rootDir: string;

  // #endregion Properties (1)

  // #region Constructors (1)

  constructor(
    private config: S3.ClientConfiguration,
    private bucket: string,
    roorDir: string,
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    this.rootDir = normalizePath(roorDir);
  }

  // #endregion Constructors (1)

  // #region Protected Methods (1)

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

  // #endregion Protected Methods (1)
}
