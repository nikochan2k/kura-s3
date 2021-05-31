import { AWSError, config } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/acm";
import S3, {
  CompletedMultipartUpload,
  CompleteMultipartUploadRequest,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  ListObjectsV2Request,
  UploadPartRequest,
} from "aws-sdk/clients/s3";
import {
  AbstractAccessor,
  AbstractFileError,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject,
  FileSystemOptions,
  hasBuffer,
  INDEX_DIR,
  InvalidModificationError,
  normalizePath,
  NotFoundError,
  NotReadableError,
  toArrayBuffer,
  toBlob,
  toBuffer,
  XHR,
} from "kura";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileSystemOptions } from "./S3FileSystemOption";
import { getKey, getPrefix } from "./S3Util";

const EXPIRES = 60 * 60 * 24 * 7;

export class S3Accessor extends AbstractAccessor {
  // #region Properties (3)

  public filesystem: FileSystem;
  public name: string;
  public s3: S3;

  // #endregion Properties (3)

  // #region Constructors (1)

  constructor(
    config: ClientConfiguration,
    private bucket: string,
    private rootDir: string,
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    if (!config.httpOptions) {
      config.httpOptions = {};
    }
    config.maxRetries = 0;
    if (config.httpOptions.timeout == null) {
      config.httpOptions.timeout = 2000;
      config.httpOptions.connectTimeout = 2000;
    }
    config.signatureVersion = "v4";
    this.s3 = new S3(config);
    this.filesystem = new S3FileSystem(this);
    if (!this.rootDir.startsWith(DIR_SEPARATOR)) {
      this.rootDir = DIR_SEPARATOR + this.rootDir;
    }
    this.name = this.bucket + this.rootDir;
  }

  // #endregion Constructors (1)

  // #region Public Methods (7)

  public async createIndexDir(dirPath: string) {
    let indexDir = INDEX_DIR + dirPath;
    if (!indexDir.endsWith(DIR_SEPARATOR)) {
      indexDir += DIR_SEPARATOR;
    }

    return indexDir;
  }

  public async doDelete(fullPath: string, isFile: boolean) {
    if (!isFile) {
      return;
    }
    const key = this.getKey(fullPath);
    const params: DeleteObjectRequest = {
      Bucket: this.bucket,
      Key: key,
    };
    try {
      await this.s3.deleteObject(params).promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  public async doGetObject(fullPath: string): Promise<FileSystemObject> {
    const key = this.getKey(fullPath);
    try {
      const data = await this.s3
        .headObject({
          Bucket: this.bucket,
          Key: key,
        })
        .promise();
      const name = key.split(DIR_SEPARATOR).pop();
      const url = await this.getSignedUrl(fullPath, "getObject");
      return {
        name,
        fullPath: fullPath,
        lastModified: data.LastModified.getTime(),
        size: data.ContentLength,
        url,
      };
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  public async doGetObjects(dirPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + dirPath);
    const prefix = getPrefix(path);
    const params: ListObjectsV2Request = {
      Bucket: this.bucket,
      Delimiter: DIR_SEPARATOR,
      Prefix: prefix,
      ContinuationToken: null,
    };

    const objects: FileSystemObject[] = [];
    await this.doReadObjectsFromS3(params, dirPath, path, objects);
    return objects;
  }

  public async doMakeDirectory(obj: FileSystemObject) {
    // NOOP
  }

  public async doReadContent(
    fullPath: string
  ): Promise<Blob | BufferSource | string> {
    if (this.s3Options.methodOfDoGetContent === "xhr") {
      return await this.doReadContentUsingXHR(
        fullPath,
        hasBuffer ? "arraybuffer" : "blob"
      );
    } else {
      return await this.doReadContentUsingGetObject(fullPath);
    }
  }

  // #endregion Public Methods (7)

  // #region Protected Methods (6)

  protected doWriteArrayBuffer(
    fullPath: string,
    buffer: ArrayBuffer
  ): Promise<void> {
    return this.doWriteContentToS3(fullPath, buffer);
  }

  protected async doWriteBase64(
    fullPath: string,
    base64: string
  ): Promise<void> {
    const buffer = await toArrayBuffer(base64);
    return this.doWriteContentToS3(fullPath, buffer);
  }

  protected doWriteBlob(fullPath: string, blob: Blob): Promise<void> {
    return this.doWriteContentToS3(fullPath, blob);
  }

  protected async doWriteBuffer(
    fullPath: string,
    buffer: Buffer
  ): Promise<void> {
    return this.doWriteContentToS3(fullPath, buffer);
  }

  protected initialize(options: FileSystemOptions) {
    this.initializeIndexOptions(options);

    if (options.contentsCache == null) {
      options.contentsCache = false;
    }
    this.initializeContentsCacheOptions(options);

    this.debug("S3Accessor#initialize", JSON.stringify(options));
  }

  protected initializeIndexOptions(options: FileSystemOptions) {
    if (!options.index) {
      return;
    }

    if (options.indexOptions == null) {
      options.indexOptions = {};
    }

    const indexOptions = options.indexOptions;
    if (indexOptions.noCache == null) {
      indexOptions.noCache = true;
    }
    if (indexOptions.logicalDelete == null) {
      indexOptions.logicalDelete = false;
    }
  }

  // #endregion Protected Methods (6)

  // #region Private Methods (12)

  private async doReadContentUsingGetObject(fullPath: string) {
    try {
      const key = this.getKey(fullPath);
      const data = await this.s3
        .getObject({ Bucket: this.bucket, Key: key })
        .promise();
      return this.fromBody(data.Body);
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doReadContentUsingXHR(
    fullPath: string,
    responseType: XMLHttpRequestResponseType
  ) {
    try {
      const obj = await this.doGetObject(fullPath);
      const xhr = new XHR({ timeout: config.httpOptions.timeout });
      return xhr.get(obj.url, responseType);
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doReadObjectsFromS3(
    params: ListObjectsV2Request,
    dirPath: string,
    path: string,
    objects: FileSystemObject[]
  ) {
    try {
      var data = await this.s3.listObjectsV2(params).promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, dirPath, err);
      }
      throw new NotReadableError(this.name, dirPath, err);
    }
    for (const content of data.CommonPrefixes) {
      const parts = content.Prefix.split(DIR_SEPARATOR);
      const name = parts[parts.length - 2];
      const fullPath = normalizePath(dirPath + DIR_SEPARATOR + name);
      objects.push({
        name: name,
        fullPath: fullPath,
        lastModified: null,
        size: null,
      });
    }
    for (const content of data.Contents) {
      const parts = content.Key.split(DIR_SEPARATOR);
      const name = parts[parts.length - 1];
      const fullPath = normalizePath(dirPath + DIR_SEPARATOR + name);
      objects.push({
        name: name,
        fullPath: fullPath,
        lastModified: content.LastModified.getTime(),
        size: content.Size,
      });
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this.doReadObjectsFromS3(params, dirPath, path, objects);
    }
  }

  private async doWriteContentToS3(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    const method = this.s3Options.methodOfDoPutContent;

    if (method === "xhr") {
      if (hasBuffer) {
        content = await toArrayBuffer(content);
      } else {
        content = toBlob(content);
      }

      await this.doWriteContentUsingXHR(fullPath, content);
    } else if (method === "uploadPart") {
      content = await toArrayBuffer(content);
      await this.doWriteContentUsingUploadPart(fullPath, content);
    } else {
      if (hasBuffer) {
        content = await toBuffer(content);
      } else {
        content = toBlob(content);
      }

      if (method === "upload") {
        await this.doWriteContentUsingUpload(fullPath, content);
      } else {
        await this.doWriteContentUsingPutObject(fullPath, content);
      }
    }
  }

  private async doWriteContentUsingPutObject(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    const key = this.getKey(fullPath);
    try {
      await this.s3
        .putObject({
          Bucket: this.bucket,
          Key: key,
          Body: content,
        })
        .promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async doWriteContentUsingUpload(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    const key = this.getKey(fullPath);
    await this.s3
      .upload({
        Bucket: this.bucket,
        Key: key,
        Body: content,
      })
      .promise();
  }

  private async doWriteContentUsingUploadPart(
    fullPath: string,
    content: ArrayBuffer
  ) {
    const key = this.getKey(fullPath);

    const view = new Uint8Array(content);
    const allSize = view.byteLength;
    const partSize = 1024 * 1024; // 1MB chunk
    const multipartMap: CompletedMultipartUpload = {
      Parts: [],
    };

    const createReq: CreateMultipartUploadRequest = {
      Bucket: this.bucket,
      Key: key,
    };
    const multiPartUpload = await this.s3
      .createMultipartUpload(createReq)
      .promise();
    const uploadId = multiPartUpload.UploadId;

    let partNum = 0;
    const { ContentType, ...otherParams } = createReq;
    for (let rangeStart = 0; rangeStart < allSize; rangeStart += partSize) {
      partNum++;
      const end = Math.min(rangeStart + partSize, allSize);
      const chunk = view.slice(rangeStart, end);
      const partParams: UploadPartRequest = {
        Body: chunk,
        PartNumber: partNum,
        UploadId: uploadId,
        ...otherParams,
      };
      const uploadPart = await this.s3.uploadPart(partParams).promise();

      multipartMap.Parts[partNum - 1] = {
        ETag: uploadPart.ETag,
        PartNumber: partNum,
      };
    }

    const completeReq: CompleteMultipartUploadRequest = {
      ...otherParams,
      MultipartUpload: multipartMap,
      UploadId: uploadId,
    };

    await this.s3.completeMultipartUpload(completeReq).promise();
  }

  private async doWriteContentUsingXHR(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    try {
      const url = await this.getSignedUrl(fullPath, "putObject");
      const xhr = new XHR({ timeout: config.httpOptions.timeout });
      await xhr.put(url, content);
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async fromBody(body: S3.Body): Promise<BufferSource | Blob | string> {
    if (typeof body === "string") {
      return body;
    }
    if (this.isReadable(body)) {
      const readable: any = body;
      return new Promise((resolve, reject) => {
        const bufs: Buffer[] = [];
        readable.on("data", (chunk: any) => {
          bufs.push(chunk);
        });
        readable.on("end", () => {
          resolve(Buffer.concat(bufs));
        });
        readable.on("error", (error: Error) => {
          reject(error);
        });
      });
    }
    if (hasBuffer) {
      return toBuffer(body as any);
    }
    return body as any;
  }

  private getKey(fullPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    return key;
  }

  private async getSignedUrl(fullPath: string, operation: string) {
    const key = this.getKey(fullPath);
    const url = await this.s3.getSignedUrlPromise(operation, {
      Bucket: this.bucket,
      Key: key,
      Expires: EXPIRES,
    });
    return url;
  }

  private isReadable(value: any) {
    return typeof value.on === "function";
  }

  // #endregion Private Methods (12)
}
