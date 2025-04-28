import {
  AuthenticateRequest,
  AuthenticateResponse,
} from "@setcd-io/connectrpc-etcd";
import { BaseHandler } from "./base";
import Context from "../context";

export class AuthHandler extends BaseHandler {
  constructor(ctx: Context) {
    super(ctx);
  }

  async authenticate(req: AuthenticateRequest): Promise<AuthenticateResponse> {
    const { name, password } = req;
    if (!name || !password) {
      throw new Error("name and password are required");
    }
    const token = Buffer.from(`${name}:${password}`).toString("base64");
    return {
      $typeName: "etcdserverpb.AuthenticateResponse",
      header: await this.header(name),
      token,
    };
  }
}
