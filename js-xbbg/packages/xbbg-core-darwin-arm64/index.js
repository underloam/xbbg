"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.runtimeBinaryPath = exports.binaryPath = void 0;
const node_path_1 = __importDefault(require("node:path"));
exports.binaryPath = node_path_1.default.join(__dirname, 'napi_xbbg.node');
exports.runtimeBinaryPath = node_path_1.default.join(__dirname, 'napi_xbbg_runtime.node');
