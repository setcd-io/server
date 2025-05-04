export class RetryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RetryError";
  }
}

export class FatalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FatalError";
  }
}
