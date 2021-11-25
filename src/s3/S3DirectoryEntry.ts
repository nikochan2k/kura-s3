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
  // #region Constructors (1)

  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  // #endregion Constructors (1)

  // #region Public Methods (1)

  public getDirectory(
    path: string,
    options?: Flags | undefined,
    successCallback?: DirectoryEntryCallback | undefined,
    errorCallback?: ErrorCallback | undefined
  ): void {
    const fullPath = resolveToFullPath(this.fullPath, path);

    this.params.accessor
      .getObject(fullPath, false)
      .then(async (obj) => {
        if (fullPath === DIR_SEPARATOR) {
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
        const name = fullPath.split(DIR_SEPARATOR).pop();
        const accessor = this.params.accessor;
        const entry = new S3DirectoryEntry({
          accessor: accessor,
          name: name,
          fullPath: fullPath,
        });
        if (err instanceof NotFoundError) {
          if (accessor.options.index) {
            const obj: FileSystemObject = {
              name: name,
              fullPath: fullPath,
            };
            let needUpdate = false;
            accessor
              .getRecord(fullPath)
              .then((value) => {
                needUpdate = !value.record;
              })
              .catch((e) => {
                if (e instanceof NotFoundError) {
                  needUpdate = true;
                } else {
                  onError(e);
                }
              })
              .finally(() => {
                if (needUpdate) {
                  accessor.updateIndex(obj).catch((e) => {
                    onError(e);
                  });
                }
              });
          }
        } else {
          onError(err);
        }
        successCallback(entry);
      });
  }

  // #endregion Public Methods (1)

  // #region Protected Methods (3)

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

  // #endregion Protected Methods (3)
}
