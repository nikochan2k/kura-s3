import {
  AbstractDirectoryEntry,
  DIR_SEPARATOR,
  DirectoryEntry,
  DirectoryEntryCallback,
  DirectoryReader,
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
    path = resolveToFullPath(this.fullPath, path);

    this.getDirectoryObject(path)
      .then(obj => {
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
                path,
                `${path} is not a directory`
              ),
              errorCallback
            );
            return;
          }

          if (options.create) {
            if (options.exclusive) {
              onError(
                new InvalidModificationError(path, `${path} already exists`),
                errorCallback
              );
              return;
            }
          }
          successCallback(this.toDirectoryEntry(obj));
        } else {
          const name = path.split(DIR_SEPARATOR).pop();
          const accessor = this.params.accessor;
          const entry = new S3DirectoryEntry({
            accessor: accessor,
            name: name,
            fullPath: path
          });
          if (accessor.hasIndex) {
            accessor
              .updateIndex({
                name: name,
                fullPath: path
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
