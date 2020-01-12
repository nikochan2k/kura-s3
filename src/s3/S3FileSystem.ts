import {
  AbstractDirectoryEntry,
  AbstractFileSystem,
  FileSystemParams
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryEntry } from "./S3DirectoryEntry";

export class S3FileSystem extends AbstractFileSystem<S3Accessor> {
  root: S3DirectoryEntry;

  constructor(accessor: S3Accessor) {
    super(accessor);
  }

  protected createRoot(
    params: FileSystemParams<S3Accessor>
  ): AbstractDirectoryEntry<S3Accessor> {
    return new S3DirectoryEntry(params);
  }
}
