import type { WorkersEnv } from "../../src/workers/index";

declare global {
  namespace Cloudflare {
    interface Env extends WorkersEnv {}
    interface GlobalProps {
      mainModule: typeof import("../../src/workers/index");
    }
  }
}

export {};
