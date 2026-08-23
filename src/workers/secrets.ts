import type { SecretResolver } from "../secrets";
import { SecretReferenceError } from "../secrets";
import { createProductEnvironmentReader, productEnvironmentVariable } from "../environment";

export class WorkersSecretResolver implements SecretResolver {
  private readonly readProductEnvironment;

  constructor(private readonly bindings: Record<string, unknown>) {
    this.readProductEnvironment = createProductEnvironmentReader(bindings);
  }

  async resolve(reference: string): Promise<string> {
    if (!reference.startsWith("env:")) throw new SecretReferenceError(reference, "Workers supports env: secret bindings only");
    const name = reference.slice(4);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new SecretReferenceError(reference, "invalid Workers secret binding reference");
    const value = productEnvironmentVariable(name, this.readProductEnvironment, this.bindings);
    if (typeof value !== "string" || !value) throw new SecretReferenceError(reference, "Workers secret binding is missing or empty");
    return value;
  }
}
