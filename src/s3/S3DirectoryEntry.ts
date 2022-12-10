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
  NotFoundError,
  onError,
  PathExistsError,
  resolveToFullPath,
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryEntry extends AbstractDirectoryEntry<S3Accessor> {
  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  public getDirectory(
    path: string,
    options?: Flags | undefined,
    successCallback?: DirectoryEntryCallback | undefined,
    errorCallback?: ErrorCallback | undefined
  ): void {
    const fullPath = resolveToFullPath(this.fullPath, path);

    this.params.accessor
      .getObject(fullPath, false)
      .then((obj) => {
        if (fullPath === DIR_SEPARATOR) {
          successCallback(this.filesystem.root);
          return;
        }

        if (!options) {
          options = {};
        }
        if (!successCallback) {
          successCallback = () => {
            // noop
          };
        }

        if (obj.size != null && 0 < obj.size) {
          onError(
            new PathExistsError(
              this.filesystem.name,
              fullPath,
              `${fullPath} is not a directory`
            ),
            errorCallback
          );
          return;
        }

        successCallback(this.toDirectoryEntry(obj));
      })
      .catch((err) => {
        const name = fullPath.split(DIR_SEPARATOR).pop();
        const accessor = this.params.accessor;
        if (err instanceof NotFoundError) {
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
            accessor
              .getRecord(fullPath)
              .then(() => {
                successCallback(entry);
              })
              .catch(async (e) => {
                if (e instanceof NotFoundError) {
                  try {
                    const record = await accessor.createRecord(obj);
                    await accessor.saveRecord(obj.fullPath, record);
                    successCallback(entry);
                  } catch (e) {
                    onError(e, errorCallback);
                  }
                } else {
                  onError(e, errorCallback);
                }
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
