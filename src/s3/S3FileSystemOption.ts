import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { XHROptions } from "kura";

export interface S3FileSystemOptions extends FileSystemOptions {
  // #region Properties (3)

  methodOfDoGetContent?: "xhr" | "getObject";
  methodOfDoPutContent?: "xhr" | "upload" | "uploadPart" | "putObject";
  xhrOptions?: XHROptions;

  // #endregion Properties (3)
}
