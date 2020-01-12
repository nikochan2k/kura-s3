import {
  AbstractDirectoryEntry,
  DirectoryEntry,
  DirectoryReader,
  FileEntry,
  FileSystemObject,
  FileSystemParams,
  Flags,
  DirectoryEntryCallback,
  ErrorCallback,
  resolveToFullPath,
  DIR_SEPARATOR
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryReader } from "./S3DirectoryReader";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryEntry extends AbstractDirectoryEntry<S3Accessor> {
  public static CheckDirectoryExistance = false;

  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  createReader(): DirectoryReader {
    return new S3DirectoryReader(this);
  }

  getDirectory(
    path: string,
    options?: Flags | undefined,
    successCallback?: DirectoryEntryCallback | undefined,
    errorCallback?: ErrorCallback | undefined
  ): void {
    if (!S3DirectoryEntry.CheckDirectoryExistance) {
      if (successCallback) {
        path = resolveToFullPath(this.fullPath, path);
        const name = path.split(DIR_SEPARATOR).pop();
        successCallback(
          new S3DirectoryEntry({
            accessor: this.params.accessor,
            name: name,
            fullPath: path
          })
        );
      }
      return;
    }
    super.getDirectory(path, options, successCallback, errorCallback);
  }

  toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      accessor: this.params.accessor,
      ...obj
    });
  }

  toFileEntry(obj: FileSystemObject): FileEntry {
    return new S3FileEntry({
      accessor: this.params.accessor,
      ...obj
    });
  }
}
