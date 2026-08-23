import { readFile } from "node:fs/promises";
import { productEnvironmentVariable } from "./environment";

export class SecretReferenceError extends Error {
  constructor(readonly reference: string, message: string) {
    super(message);
    this.name = "SecretReferenceError";
  }
}

export interface SecretResolver {
  resolve(reference: string): Promise<string>;
}

export class EnvironmentFileSecretResolver implements SecretResolver {
  async resolve(reference: string): Promise<string> {
    if (reference.startsWith("env:")) {
      const name = reference.slice(4);
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new SecretReferenceError(reference, "invalid environment variable reference");
      const value = productEnvironmentVariable(name);
      if (!value) throw new SecretReferenceError(reference, "environment variable is missing or empty");
      return value;
    }
    if (reference.startsWith("file:")) {
      const path = reference.slice(5);
      if (!path) throw new SecretReferenceError(reference, "file path is empty");
      try {
        const value = (await readFile(path, "utf8")).replace(/[\r\n]+$/, "");
        if (!value) throw new SecretReferenceError(reference, "secret file is empty");
        return value;
      } catch (error) {
        if (error instanceof SecretReferenceError) throw error;
        throw new SecretReferenceError(reference, "secret file cannot be read");
      }
    }
    throw new SecretReferenceError(reference, "only env: and file: secret references are supported");
  }
}

export async function validateSecretReferences(resolver: SecretResolver, references: string[]): Promise<void> {
  await Promise.all(references.map((reference) => resolver.resolve(reference)));
}
