"use strict";

// Loaded through NODE_OPTIONS during the offline reproducibility build. Keep
// this dependency-free: it must run before Observable Framework or npm loads.
const dns = require("node:dns");
const http = require("node:http");
const https = require("node:https");
const net = require("node:net");
const tls = require("node:tls");

function denied(target = "unknown target") {
  throw new Error(`network access is disabled for deterministic site builds: ${String(target)}`);
}

globalThis.fetch = async (target) => denied(target);
dns.lookup = (...args) => denied(args[0]);
dns.resolve = (...args) => denied(args[0]);
http.get = (...args) => denied(args[0]);
http.request = (...args) => denied(args[0]);
https.get = (...args) => denied(args[0]);
https.request = (...args) => denied(args[0]);
net.connect = (...args) => denied(args[0]);
net.createConnection = (...args) => denied(args[0]);
net.Socket.prototype.connect = function connect(...args) { return denied(args[0]); };
tls.connect = (...args) => denied(args[0]);
