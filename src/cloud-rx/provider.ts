import { Observable } from "rxjs";

export abstract class Provider<T> {
  constructor(protected signal: AbortSignal) {}

  abstract init(id?: string): Observable<T>;
}
