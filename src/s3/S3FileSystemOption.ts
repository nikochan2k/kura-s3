import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { XHROptions } from "kura";

export interface S3FileSystemOptions extends FileSystemOptions {
  methodOfDoGetContent?: "xhr" | "getObject";
  methodOfDoPutContent?: "xhr" | "upload" | "uploadPart" | "putObject";
  xhrOptions?: XHROptions;
}
