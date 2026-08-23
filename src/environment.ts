type Environment = Record<string, unknown>;

export type ProductEnvironmentReader = (suffix: string) => string | undefined;

let legacyWarningIssued = false;

function warnForLegacyEnvironment(message: string): void {
  if (legacyWarningIssued) return;
  legacyWarningIssued = true;
  process.stderr.write(`${message}\n`);
}

export function createProductEnvironmentReader(
  environment: Environment,
  warn: (message: string) => void = warnForLegacyEnvironment,
): ProductEnvironmentReader {
  let warned = false;
  return (suffix) => {
    const current = environment[`BUGLER_${suffix}`];
    if (typeof current === "string") return current;
    const legacy = environment[`REVEILLE_${suffix}`];
    if (typeof legacy !== "string") return undefined;
    if (!warned) {
      warned = true;
      warn("BUGLER: REVEILLE_* environment variables are deprecated; use BUGLER_* instead.");
    }
    return legacy;
  };
}

export const productEnvironment = createProductEnvironmentReader(process.env);

export function productEnvironmentVariable(
  name: string,
  read: ProductEnvironmentReader = productEnvironment,
  environment: Environment = process.env,
): string | undefined {
  if (name.startsWith("BUGLER_")) return read(name.slice("BUGLER_".length));
  if (name.startsWith("REVEILLE_")) return read(name.slice("REVEILLE_".length));
  const value = environment[name];
  return typeof value === "string" ? value : undefined;
}
