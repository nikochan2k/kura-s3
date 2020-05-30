import {
  AbstractDirectoryEntry,
  DirectoryEntry,
  DirectoryEntryCallback,
  DIR_SEPARATOR,
  ErrorCallback,
  FileEntry,
  FileSystemObject,
  FileSystemParams,
  Flags,
  InvalidModificationError,
  NotFoundError,
  onError,
  resolveToFullPath,
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryEntry extends AbstractDirectoryEntry<S3Accessor> {
  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  getDirectory(
    path: string,
    options?: Flags | undefined,
    successCallback?: DirectoryEntryCallback | undefined,
    errorCallback?: ErrorCallback | undefined
  ): void {
    const fullPath = resolveToFullPath(this.fullPath, path);

    this.params.accessor
      .getObject(fullPath)
      .then(async (obj) => {
        if (fullPath === "/") {
          successCallback(this.filesystem.root);
          return;
        }

        if (!options) {
          options = {};
        }
        if (!successCallback) {
          successCallback = () => {};
        }

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
      })
      .catch((err) => {
        if (err instanceof NotFoundError) {
          const name = fullPath.split(DIR_SEPARATOR).pop();
          const accessor = this.params.accessor;
          const entry = new S3DirectoryEntry({
            accessor: accessor,
            name: name,
            fullPath: fullPath,
          });
          if (accessor.options.index) {
            const obj: FileSystemObject = {
              name: name,
              fullPath: fullPath,
            };
            const record = accessor.createRecord(obj);
            accessor
              .updateIndex(record, true)
              .then(() => {
                successCallback(entry);
              })
              .catch((err) => {
                onError(err, errorCallback);
              });
          } else {
            successCallback(entry);
          }
        } else {
          onError(err, errorCallback);
        }
      });
  }

  protected createEntry(obj: FileSystemObject) {
    return obj.size != null
      ? new S3FileEntry({
          accessor: this.params.accessor,
          ...obj,
        })
      : new S3DirectoryEntry({
          accessor: this.params.accessor,
          ...obj,
        });
  }

  protected toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      accessor: this.params.accessor,
      ...obj,
    });
  }

  protected toFileEntry(obj: FileSystemObject): FileEntry {
    return new S3FileEntry({
      accessor: this.params.accessor,
      ...obj,
    });
  }
}
