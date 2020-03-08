import {
  AbstractDirectoryEntry,
  DirectoryEntry,
  DirectoryEntryCallback,
  DirectoryReader,
  DIR_SEPARATOR,
  ErrorCallback,
  FileEntry,
  FileSystemObject,
  FileSystemParams,
  Flags,
  InvalidModificationError,
  onError,
  resolveToFullPath
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryReader } from "./S3DirectoryReader";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryEntry extends AbstractDirectoryEntry<S3Accessor> {
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
    const fullPath = resolveToFullPath(this.fullPath, path);

    this.getDirectoryObject(fullPath)
      .then(async obj => {
        if (!options) {
          options = {};
        }
        if (!successCallback) {
          successCallback = () => {};
        }

        if (obj) {
          if (obj.size != null) {
            onError(
              new InvalidModificationError(
                this.filesystem.name,
                fullPath,
                `${fullPath} is not a directory`
              ),
              errorCallback
            );
            return;
          }

          if (options.create) {
            if (options.exclusive) {
              onError(
                new InvalidModificationError(
                  fullPath,
                  `${fullPath} already exists`
                ),
                errorCallback
              );
              return;
            }
          }
          successCallback(this.toDirectoryEntry(obj));
        } else {
          const name = fullPath.split(DIR_SEPARATOR).pop();
          const accessor = this.params.accessor;
          const entry = new S3DirectoryEntry({
            accessor: accessor,
            name: name,
            fullPath: fullPath
          });
          if (accessor.options.useIndex) {
            accessor
              .updateIndex({
                name: name,
                fullPath: fullPath
              })
              .then(() => {
                successCallback(entry);
              })
              .catch(err => {
                onError(err, errorCallback);
              });
          } else {
            successCallback(entry);
          }
        }
      })
      .catch(err => {
        onError(err, errorCallback);
      });
  }

  protected toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      accessor: this.params.accessor,
      ...obj
    });
  }

  protected toFileEntry(obj: FileSystemObject): FileEntry {
    return new S3FileEntry({
      accessor: this.params.accessor,
      ...obj
    });
  }
}
