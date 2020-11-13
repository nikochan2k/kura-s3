import {
  AbstractDirectoryEntry,
  AbstractFileSystem,
  FileSystemParams,
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryEntry } from "./S3DirectoryEntry";

export class S3FileSystem extends AbstractFileSystem<S3Accessor> {
  // #region Properties (1)

  public root: S3DirectoryEntry;

  // #endregion Properties (1)

  // #region Constructors (1)

  constructor(accessor: S3Accessor) {
    super(accessor);
  }

  // #endregion Constructors (1)

  // #region Protected Methods (1)

  protected createRoot(
    params: FileSystemParams<S3Accessor>
  ): AbstractDirectoryEntry<S3Accessor> {
    return new S3DirectoryEntry(params);
  }

  // #endregion Protected Methods (1)
}
