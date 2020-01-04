import {
  DIR_SEPARATOR,
  DirectoryReader,
  EntriesCallback,
  ErrorCallback,
  onError
} from "kura";
import { getPrefix } from "./S3Util";
import { ListObjectsV2Request } from "aws-sdk/clients/s3";
import { S3DirectoryEntry } from "./S3DirectoryEntry";
import { S3Entry } from "./S3Entry";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryReader implements DirectoryReader {
  constructor(public dirEntry: S3DirectoryEntry, public used = false) {}

  doReadEntries(
    params: ListObjectsV2Request,
    entries: S3Entry[],
    successCallback: EntriesCallback,
    errorCallback?: ErrorCallback
  ) {
    const fullPath = this.dirEntry.fullPath;
    const filesystem = this.dirEntry.filesystem;
    filesystem.s3.listObjectsV2(params, (err, data) => {
      if (err) {
        onError(err, errorCallback);
        return;
      }
      for (const content of data.CommonPrefixes) {
        const parts = content.Prefix.split(DIR_SEPARATOR);
        const name = parts[parts.length - 2];
        const newDirEntry = new S3DirectoryEntry({
          filesystem: filesystem,
          name: name,
          fullPath: (fullPath === "/" ? "" : fullPath) + DIR_SEPARATOR + name,
          lastModified: null,
          size: null
        });
        entries.push(newDirEntry);
      }
      for (const content of data.Contents) {
        const parts = content.Key.split(DIR_SEPARATOR);
        const name = parts[parts.length - 1];
        const newFileEntry = new S3FileEntry({
          filesystem: filesystem,
          name: name,
          fullPath: (fullPath === "/" ? "" : fullPath) + DIR_SEPARATOR + name,
          lastModified: content.LastModified.getTime(),
          size: content.Size
        });
        entries.push(newFileEntry);
      }

      if (data.IsTruncated) {
        params.ContinuationToken = data.NextContinuationToken;
        this.doReadEntries(params, entries, successCallback, errorCallback);
        return;
      }
      successCallback(entries);
    });
  }

  readEntries(
    successCallback: EntriesCallback,
    errorCallback?: ErrorCallback
  ): void {
    const fullPath = this.dirEntry.fullPath;
    const prefix = getPrefix(fullPath);
    const params: ListObjectsV2Request = {
      Bucket: this.dirEntry.filesystem.bucket,
      Delimiter: DIR_SEPARATOR,
      Prefix: prefix,
      ContinuationToken: null
    };

    const entries: S3Entry[] = [];
    this.doReadEntries(params, entries, successCallback, errorCallback);
  }
}
