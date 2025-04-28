import { HandlerContext } from "@connectrpc/connect";
import Context from "../context";
import {
  MemberListRequest,
  MemberListResponse,
} from "@setcd-io/connectrpc-etcd";
import { BaseHandler } from "./base";

export class ClusterHandler extends BaseHandler {
  constructor(ctx: Context) {
    super(ctx);
  }

  async members(
    tenant: string,
    req: MemberListRequest,
    ctx: HandlerContext
  ): Promise<MemberListResponse> {
    return {
      $typeName: "etcdserverpb.MemberListResponse",
      header: await this.header(tenant),
      members: [
        {
          $typeName: "etcdserverpb.Member",
          clientURLs: [ctx.url],
          ID: 0n,
          isLearner: false,
          name: tenant,
          peerURLs: [ctx.url],
        },
      ],
    };
  }
}
