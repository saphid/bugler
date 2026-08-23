#!/usr/bin/env bun
import { main } from "./cli";
import { asErrorMessage } from "./util";

main().catch((error) => {
  process.stderr.write(`bugler: ${asErrorMessage(error)}\n`);
  process.exit(1);
});

