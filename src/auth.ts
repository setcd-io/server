import { FastifyReply, FastifyRequest } from "fastify";
import { PeerCertificate, TLSSocket } from "tls";
import { context } from "./context";

export const X_NAMESPACE = "x-namespace";
export const X_TENANT = "x-tenant";

type Validation = {
  tenant?: string;
};

export const authHook = async (req: FastifyRequest, res: FastifyReply) => {
  const { token } = req.headers;
  const socket = req.raw.socket as Partial<TLSSocket>;
  const cert = socket?.getCertificate?.() as Partial<PeerCertificate>;

  const namespace = context.namespace;
  const tenant = (await validate(cert, token)).tenant || namespace;

  req.headers[X_NAMESPACE] = namespace;
  req.headers[X_TENANT] = tenant;

  if (namespace) {
    res.header(X_NAMESPACE, namespace);
  }
  if (tenant) {
    res.header(X_TENANT, tenant);
  }
};

const validate = async (
  cert?: Partial<PeerCertificate>,
  token?: string | string[]
): Promise<Validation> => {
  try {
    const commonName = cert?.subject?.CN || context.namespace;
    let userName = "nobody";

    if (token && Array.isArray(token)) {
      return Promise.race(token.map((t) => validate(cert, t)));
    }

    if (token) {
      [userName] = Buffer.from(token, "base64").toString("utf8").split(":");
    }

    return { tenant: `${userName}@${commonName}` };
  } catch (e) {
    return {};
  }
};
