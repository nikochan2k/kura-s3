import { FileSystemOptions } from "kura/lib/FileSystemOptions";

export interface S3FileSystemOptions extends FileSystemOptions {
  methodOfDoGetContent?: "xhr" | "getObject";
  methodOfDoPutContent?: "upload" | "uploadPart" | "putObject";
}
