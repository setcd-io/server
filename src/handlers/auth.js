"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AuthHandler = void 0;
const base_1 = require("./base");
class AuthHandler extends base_1.BaseHandler {
    constructor(ctx) {
        super(ctx);
    }
    async authenticate(req) {
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
exports.AuthHandler = AuthHandler;
