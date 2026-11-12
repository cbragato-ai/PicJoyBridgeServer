import { UPLOAD_LIMITS, UploadError } from "./uploadProtocol";

export const CONFIRMED_PROTOCOL = 2;
export const FLOW_LIMITS = {
  chunkBytes: 48 * 1024,
  frameBytes: 128 * 1024,
  maxTransfers: 32,
  globalWindowBytes: 4 * 1024 * 1024,
  requestTimeoutMs: 60_000,
  completionTimeoutMs: 610_000,
  transferTimeoutMs: 30 * 60_000,
  permissionTimeoutMs: 10 * 60_000,
} as const;

export type ManifestFile = { fileId: string; name: string; size: number; mime: string };
export type UploadRequest = {
  type: "upload-request"; protocolVersion: 2; transferId: string; requestId: number;
  operation: "start" | "chunk" | "file-complete" | "finish";
  files?: ManifestFile[]; fileId?: string; seq?: number; chunkBase64?: string; sha256?: string;
};
export type UploadAck = { type: "upload-ack"; protocolVersion: 2; transferId: string; requestId: number };
export function requireUpload(condition: unknown, message: string): asserts condition {
  if (!condition) throw new UploadError("INVALID_TRANSFER", message);
}
export function validId(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_-]{1,80}$/.test(value);
}
export function validateManifest(files: unknown): ManifestFile[] {
  requireUpload(Array.isArray(files) && files.length > 0 && files.length <= UPLOAD_LIMITS.maxFiles, "O lote deve conter entre 1 e 100 fotos.");
  const ids = new Set<string>(); let bytes = 0;
  for (const file of files) {
    requireUpload(file && validId(file.fileId) && !ids.has(file.fileId), "Identificador de foto inválido ou repetido.");
    requireUpload(typeof file.name === "string" && file.name.length > 0 && file.name.length <= 255, "Nome de foto inválido.");
    requireUpload(Number.isSafeInteger(file.size) && file.size > 0 && file.size <= UPLOAD_LIMITS.maxFileBytes, "Cada foto deve ter até 20 MiB.");
    requireUpload(typeof file.mime === "string" && file.mime.length <= 100 && file.mime.startsWith("image/"), "Tipo de foto inválido.");
    ids.add(file.fileId); bytes += file.size;
  }
  requireUpload(bytes <= UPLOAD_LIMITS.maxTotalBytes, "O lote deve ter até 2 GiB.");
  return files.map(({ fileId, name, size, mime }) => ({ fileId, name, size, mime }));
}

// Metadados e contadores apenas; o Bridge nunca guarda os blocos para retransmissão.
export class ConfirmedManifest {
  readonly files: ManifestFile[];
  private index = 0;
  private seq = 0;
  private bytes = 0;
  constructor(files: unknown) { this.files = validateManifest(files); }
  accept(request: UploadRequest) {
    if (request.operation === "finish") {
      requireUpload(this.index === this.files.length, "O lote ainda contém fotos incompletas."); return;
    }
    const file = this.files[this.index];
    requireUpload(file && request.fileId === file.fileId, "Foto fora da ordem do manifesto.");
    if (request.operation === "file-complete") {
      requireUpload(this.bytes === file.size, "Foto incompleta.");
      this.index++; this.seq = 0; this.bytes = 0; return;
    }
    const chunk = request.chunkBase64;
    requireUpload(request.operation === "chunk" && request.seq === this.seq && typeof chunk === "string" &&
      chunk.length > 0 && chunk.length <= UPLOAD_LIMITS.maxChunkChars && chunk.length % 4 === 0 &&
      /^[A-Za-z0-9+/]+={0,2}$/.test(chunk) && typeof request.sha256 === "string" && /^[a-f0-9]{64}$/.test(request.sha256), "Bloco inválido ou fora de ordem.");
    const bytes = chunk.length / 4 * 3 - (chunk.endsWith("==") ? 2 : chunk.endsWith("=") ? 1 : 0);
    requireUpload(bytes <= FLOW_LIMITS.chunkBytes && this.bytes + bytes <= file.size &&
      (!chunk.includes("=") || this.bytes + bytes === file.size), "Tamanho de bloco inconsistente.");
    this.bytes += bytes; this.seq++;
  }
}
