export const UPLOAD_PROTOCOL_VERSION = 1;

export const UPLOAD_LIMITS = {
  maxFiles: 100,
  maxFileBytes: Number.MAX_SAFE_INTEGER,
  maxTotalBytes: Number.MAX_SAFE_INTEGER,
  maxChunkChars: 64 * 1024,
} as const;

type UploadMessage = {
  type: string;
  protocolVersion?: unknown;
  fileName?: unknown;
  size?: unknown;
  totalBase64Length?: unknown;
  seq?: unknown;
  chunkBase64?: unknown;
};

type UploadFile = {
  expectedChars: number;
  receivedChars: number;
  nextSeq: number;
  complete: boolean;
};

export class UploadError extends Error {
  constructor(public readonly code: string, message: string) {
    super(message);
  }
}

// Guarda somente contadores e metadados. Nunca retém os chunks no Bridge.
export class UploadSession {
  private files = new Map<string, UploadFile>();
  private totalBytes = 0;
  private finished = false;

  accept(message: UploadMessage) {
    if (this.finished) {
      throw new UploadError("UPLOAD_FINISHED", "Este envio já foi encerrado.");
    }
    if (message.protocolVersion !== undefined && message.protocolVersion !== UPLOAD_PROTOCOL_VERSION) {
      throw new UploadError("UNSUPPORTED_PROTOCOL", "Versão de envio não suportada.");
    }
    const name = message.fileName;
    if (typeof name !== "string" || !name || name.length > 255) {
      throw new UploadError("INVALID_FILE", "Nome de arquivo inválido.");
    }

    if (message.type === "file-meta") {
      if (this.files.has(name)) {
        throw new UploadError("DUPLICATE_FILE", "Há fotos com o mesmo nome. Renomeie os arquivos e reinicie o envio.");
      }
      const size = message.size;
      if (typeof size !== "number" || !Number.isSafeInteger(size) || size <= 0) {
        throw new UploadError("INVALID_FILE", "Tamanho de arquivo inválido.");
      }
      if (size > UPLOAD_LIMITS.maxFileBytes || this.files.size >= UPLOAD_LIMITS.maxFiles ||
          this.totalBytes + size > UPLOAD_LIMITS.maxTotalBytes) {
        throw new UploadError("UPLOAD_LIMIT_EXCEEDED", "Quantidade de fotos excedida ou metadados de tamanho inválidos.");
      }
      const expectedChars = 4 * Math.ceil(size / 3);
      if (message.totalBase64Length !== expectedChars) {
        throw new UploadError("INVALID_FILE", "O tamanho declarado da foto é inconsistente.");
      }
      this.files.set(name, { expectedChars, receivedChars: 0, nextSeq: 0, complete: false });
      this.totalBytes += size;
      return;
    }

    const file = this.files.get(name);
    if (!file || file.complete) {
      throw new UploadError("INVALID_FILE", "Foto sem metadados ou já finalizada.");
    }
    if (message.type === "file-chunk") {
      const chunk = message.chunkBase64;
      if (typeof chunk !== "string" || chunk.length === 0 || chunk.length > UPLOAD_LIMITS.maxChunkChars ||
          chunk.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(chunk) ||
          message.seq !== file.nextSeq || file.receivedChars + chunk.length > file.expectedChars ||
          (chunk.includes("=") && file.receivedChars + chunk.length !== file.expectedChars)) {
        throw new UploadError("INVALID_CHUNK", "Bloco de foto inválido, fora de ordem ou acima do tamanho permitido.");
      }
      file.receivedChars += chunk.length;
      file.nextSeq++;
      return;
    }
    if (message.type === "file-complete") {
      if (file.receivedChars !== file.expectedChars) {
        throw new UploadError("INCOMPLETE_FILE", "A foto não foi recebida por inteiro. Reinicie o envio.");
      }
      file.complete = true;
      return;
    }
    throw new UploadError("INVALID_UPLOAD", "Mensagem de envio inválida.");
  }

  finish() {
    if (this.finished || !this.files.size || [...this.files.values()].some(file => !file.complete)) {
      throw new UploadError("INCOMPLETE_TRANSFER", "O lote contém fotos incompletas ou já foi encerrado.");
    }
    this.finished = true;
  }
}
