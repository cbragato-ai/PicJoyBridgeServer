import { randomBytes, createHash } from "node:crypto";
import { WebSocket } from "ws";
import { CONFIRMED_PROTOCOL, FLOW_LIMITS, ConfirmedManifest, UploadRequest, UploadAck, requireUpload, validId } from "./confirmedProtocol";

type Peer = { ws: WebSocket; role: "web" | "kiosk"; sessionId: string; id: string; ticket?: string; maxFiles?: number; canPipeline?: boolean; completed?: { ack: UploadAck; fingerprint: string } };
type Permission = { ticket: string; kiosk: WebSocket; expiresAt: number; customerId?: string; used: boolean; maxFiles: number;
  transferWindowBytes: number; maxInFlightRequests: number; revoked?: boolean };
type Transfer = { id: string; web: Peer; kiosk: Peer; manifest: ConfirmedManifest; next: number;
  pending: Map<number, { fingerprint: string; operation: string; bytes: number }>;
  recent: Map<number, { ack: UploadAck; fingerprint: string }>;
  startedAt: number; timer?: ReturnType<typeof setTimeout> };

export class ConfirmedTransfers {
  private peers = new Map<WebSocket, Peer>();
  private permissions = new Map<string, Permission>();
  private transfers = new Map<string, Transfer>();
  private peakInFlightBytes = 0;
  snapshot() {
    const transfers = [...this.transfers.values()];
    return { activeTransfers: transfers.length, pendingRequests: transfers.reduce((sum, transfer) => sum + transfer.pending.size, 0),
      inFlightBytes: transfers.reduce((sum, transfer) => sum + [...transfer.pending.values()].reduce((bytes, item) => bytes + item.bytes, 0), 0),
      peakInFlightBytes: this.peakInFlightBytes, globalWindowBytes: FLOW_LIMITS.globalWindowBytes };
  }
  constructor(private send: (ws: WebSocket, message: unknown) => boolean) {}

  capacity(ws: WebSocket) { return this.peers.get(ws)?.maxFiles ?? 0; }
  flowLimits(ws: WebSocket) {
    const permission = this.permissions.get(this.peers.get(ws)?.sessionId ?? "");
    return { ...FLOW_LIMITS, transferWindowBytes: permission?.transferWindowBytes ?? FLOW_LIMITS.frameBytes,
      maxInFlightRequests: permission?.maxInFlightRequests ?? 1 };
  }
  hasKiosk(session: string) { return [...this.peers.values()].some(p => p.role === "kiosk" && p.sessionId === session); }
  claim(session: string, ticket: unknown, customer: string): boolean {
    const permission = this.permissions.get(session);
    if (!permission || permission.revoked || permission.used || permission.expiresAt <= Date.now() || permission.ticket !== ticket ||
      !validId(customer) || (permission.customerId && permission.customerId !== customer)) return false;
    permission.customerId = customer; return true;
  }
  register(ws: WebSocket, role: "web" | "kiosk", sessionId: string, id: string, ticket?: unknown, canPipeline = false) {
    const previous = this.peers.get(ws);
    if (previous) {
      requireUpload(previous.role === role && previous.sessionId === sessionId && previous.id === id, "Registro já vinculado a outra sessão."); return;
    }
    requireUpload(validId(id), "Identificador de cliente inválido.");
    if (role === "kiosk") {
      requireUpload(!this.hasKiosk(sessionId), "Já existe um totem conectado para esta sessão.");
    } else {
      requireUpload(![...this.peers.values()].some(peer => peer.role === "web" && peer.sessionId === sessionId && peer.ws.readyState === peer.ws.OPEN),
        "Já existe um uploader conectado nesta sessão.");
      const permission = this.permissions.get(sessionId);
      requireUpload(permission && !permission.revoked && (!permission.used || !this.transfers.has(sessionId)) && permission.expiresAt > Date.now() && permission.ticket === ticket &&
        permission.customerId === id, "Permissão inválida ou consumida. Releia o QR Code no totem.");
      permission.used = true;
    }
    this.peers.set(ws, { ws, role, sessionId, id, ...(role === "kiosk" ? { canPipeline } : {}),
      ...(role === "web" ? { ticket: ticket as string, maxFiles: this.permissions.get(sessionId)!.maxFiles } : {}) });
  }
  unlock(ws: WebSocket, maxFiles: unknown = 0) {
    const peer = this.peers.get(ws);
    requireUpload(peer?.role === "kiosk", "Somente o totem pode abrir uma nova transferência.");
    this.abort(peer.sessionId, "SESSION_RESET", "O totem reiniciou a sessão.");
    for (const web of [...this.peers.values()]) {
      if (web.role !== "web" || web.sessionId !== peer.sessionId) continue;
      this.peers.delete(web.ws);
      this.send(web.ws, { type: "transfer-error", protocolVersion: 2, code: "SESSION_RESET", message: "O totem abriu uma nova sessão. Releia o QR Code." });
      web.ws.close(1008, "SESSION_RESET");
    }
    const capacity = typeof maxFiles === "number" && Number.isSafeInteger(maxFiles) && maxFiles >= 0 ? Math.min(100, maxFiles) : 0;
    const permission = { maxFiles: capacity,
      transferWindowBytes: peer.canPipeline ? FLOW_LIMITS.transferWindowBytes : FLOW_LIMITS.frameBytes,
      maxInFlightRequests: peer.canPipeline ? FLOW_LIMITS.maxInFlightRequests : 1,
      ticket: randomBytes(32).toString("hex"), kiosk: ws,
      expiresAt: Date.now() + FLOW_LIMITS.permissionTimeoutMs, used: false };
    this.permissions.set(peer.sessionId, permission);
    this.send(ws, { type: "upload-permission", protocolVersion: CONFIRMED_PROTOCOL, uploadTicket: permission.ticket,
      expiresAt: permission.expiresAt, maxFiles: capacity });
  }
  private revoke(session: string) { const permission = this.permissions.get(session); if (permission) { permission.used = true; permission.revoked = true; } }
  private remove(transfer: Transfer) {
    if (transfer.timer) clearTimeout(transfer.timer);
    this.transfers.delete(transfer.web.sessionId); this.revoke(transfer.web.sessionId);
  }
  private abort(session: string, code: string, message: string) {
    const transfer = this.transfers.get(session); if (!transfer) return;
    this.remove(transfer);
    const error = { type: "transfer-error", protocolVersion: CONFIRMED_PROTOCOL, transferId: transfer.id, code, message };
    this.send(transfer.web.ws, error); this.send(transfer.kiosk.ws, error);
    transfer.web.ws.close(1008, code);
  }
  cleanup(ws: WebSocket) {
    const peer = this.peers.get(ws); if (!peer) return;
    this.peers.delete(ws);
    this.abort(peer.sessionId, "PEER_DISCONNECTED", "A conexão foi interrompida. Reinicie o envio junto ao totem.");
    if (peer.role === "kiosk" || (this.permissions.get(peer.sessionId)?.ticket === peer.ticket && this.transfers.has(peer.sessionId))) this.revoke(peer.sessionId);
    if (peer.role === "kiosk") this.permissions.delete(peer.sessionId);
  }
  private arm(transfer: Transfer, finishing: boolean) {
    if (transfer.timer) clearTimeout(transfer.timer);
    const remaining = FLOW_LIMITS.transferTimeoutMs - (Date.now() - transfer.startedAt);
    const timeoutMs = Math.max(1, Math.min(remaining, finishing ? FLOW_LIMITS.completionTimeoutMs : FLOW_LIMITS.requestTimeoutMs));
    const first = transfer.pending.entries().next().value as [number, { operation: string }] | undefined;
    const operation = first?.[1].operation ?? "próxima solicitação";
    const requestId = first?.[0] ?? transfer.next;
    transfer.timer = setTimeout(() => this.abort(transfer.web.sessionId, "ACK_TIMEOUT",
      `O totem não confirmou ${operation} (solicitação ${requestId}) em ${Math.ceil(timeoutMs / 1000)} segundos. Reinicie junto ao totem.`), timeoutMs);
    transfer.timer.unref?.();
  }
  private error(ws: WebSocket, message: string) {
    this.send(ws, { type: "transfer-error", protocolVersion: CONFIRMED_PROTOCOL, code: "INVALID_TRANSFER", message });
    ws.close(1008, "INVALID_TRANSFER");
  }
  handle(ws: WebSocket, raw: Record<string, unknown>): boolean {
    const peer = this.peers.get(ws);
    if (raw.type === "upload-request") {
      if (peer?.role !== "web") { this.error(ws, "Conexão sem permissão de upload."); return true; }
      try {
        const request = raw as unknown as UploadRequest;
        const serialized = JSON.stringify(request);
        const requestBytes = Buffer.byteLength(serialized);
        requireUpload(requestBytes <= FLOW_LIMITS.frameBytes && request.protocolVersion === 2 &&
          validId(request.transferId) && Number.isSafeInteger(request.requestId) && request.requestId >= 0, "Mensagem de envio inválida.");
        const fingerprint = createHash("sha256").update(serialized).digest("hex");
        if (peer.completed) {
          requireUpload(peer.completed.fingerprint === fingerprint, "Esta permissão já foi consumida.");
          this.send(ws, peer.completed.ack); return true;
        }
        let transfer = this.transfers.get(peer.sessionId);
        if (!transfer) {
          const permission = this.permissions.get(peer.sessionId);
          requireUpload(permission?.used && !permission.revoked && permission.ticket === peer.ticket && permission.customerId === peer.id && permission.expiresAt > Date.now(), "A permissão expirou. Releia o QR Code.");
          requireUpload(request.operation === "start" && request.requestId === 0, "Envie o manifesto antes das fotos.");
          const kiosk = this.peers.get(permission.kiosk);
          requireUpload(kiosk?.role === "kiosk" && kiosk.ws.readyState === kiosk.ws.OPEN, "Totem desconectado ou incompatível.");
          requireUpload(this.transfers.size < FLOW_LIMITS.maxTransfers &&
            (this.transfers.size + 1) * FLOW_LIMITS.frameBytes <= FLOW_LIMITS.globalWindowBytes, "Capacidade de envios atingida. Tente novamente junto ao totem.");
          transfer = { id: request.transferId, web: peer, kiosk, manifest: new ConfirmedManifest(request.files, peer.maxFiles ?? 0),
            next: 0, pending: new Map(), recent: new Map(), startedAt: Date.now() };
          this.transfers.set(peer.sessionId, transfer);
        }
        requireUpload(transfer.web.ws === ws && transfer.id === request.transferId, "Transferência não pertence a esta conexão.");
        const recent = transfer.recent.get(request.requestId);
        if (recent) {
          requireUpload(recent.fingerprint === fingerprint, "Repetição com conteúdo diferente."); this.send(ws, recent.ack); return true;
        }
        const pending = transfer.pending.get(request.requestId);
        if (pending) {
          requireUpload(pending.fingerprint === fingerprint, "Repetição com conteúdo diferente.");
          return true; // Repetição ainda pendente não cria outra cópia na fila de saída.
        }
        requireUpload(request.requestId === transfer.next, "Sequência de requisição inválida.");
        const currentBytes = [...transfer.pending.values()].reduce((sum, item) => sum + item.bytes, 0);
        requireUpload(transfer.pending.size < FLOW_LIMITS.maxInFlightRequests && currentBytes + requestBytes <= FLOW_LIMITS.transferWindowBytes,
          "Janela de transferência excedida.");
        if (request.requestId > 0) transfer.manifest.accept(request);
        else requireUpload(request.operation === "start", "Manifesto inválido.");
        requireUpload(transfer.kiosk.ws.bufferedAmount + requestBytes <= FLOW_LIMITS.transferWindowBytes, "O orçamento de saída da sessão foi excedido.");
        transfer.pending.set(request.requestId, { fingerprint, operation: request.operation, bytes: requestBytes });
        transfer.next++;
        this.peakInFlightBytes = Math.max(this.peakInFlightBytes, this.snapshot().inFlightBytes);
        this.arm(transfer, request.operation === "finish");
        requireUpload(this.send(transfer.kiosk.ws, request), "Totem desconectado.");
      } catch (error) {
        const message = error instanceof Error ? error.message : "Envio inválido.";
        if (this.transfers.get(peer.sessionId)?.web.ws === ws) this.abort(peer.sessionId, "INVALID_TRANSFER", message);
        else {
          if (this.permissions.get(peer.sessionId)?.ticket === peer.ticket) this.revoke(peer.sessionId);
          this.error(ws, message);
        }
      }
      return true;
    }
    if (raw.type === "upload-ack") {
      const transfer = peer && this.transfers.get(peer.sessionId);
      if (!transfer || peer?.role !== "kiosk" || transfer.kiosk.ws !== ws || raw.transferId !== transfer.id || raw.protocolVersion !== 2 ||
        typeof raw.requestId !== "number" || !Number.isSafeInteger(raw.requestId) || raw.requestId < 0) return true;
      const requestId = raw.requestId;
      if (transfer.recent.has(requestId)) return true;
      const pending = transfer.pending.get(requestId);
      if (!pending) {
        this.abort(peer.sessionId, "INVALID_ACK", "Confirmação de envio fora de sequência."); return true;
      }
      const ack: UploadAck = { type: "upload-ack", protocolVersion: 2, transferId: transfer.id, requestId };
      const completed = { ack, fingerprint: pending.fingerprint }, finished = pending.operation === "finish";
      transfer.pending.delete(ack.requestId); transfer.recent.set(ack.requestId, completed);
      while (transfer.recent.size > FLOW_LIMITS.maxInFlightRequests * 2) transfer.recent.delete(transfer.recent.keys().next().value!);
      if (finished) { transfer.web.completed = completed; this.remove(transfer); }
      else this.arm(transfer, false);
      this.send(transfer.web.ws, ack); return true;
    }
    if (raw.type === "transfer-error" && peer) {
      const transfer = this.transfers.get(peer.sessionId);
      if (transfer && raw.transferId === transfer.id) this.abort(peer.sessionId, "RECEIVER_ERROR", "O recebimento falhou. Reinicie junto ao totem.");
      return true;
    }
    if (peer?.role === "web") {
      if (this.permissions.get(peer.sessionId)?.revoked || this.permissions.get(peer.sessionId)?.ticket !== peer.ticket || raw.type !== "notify" || raw.toSession !== peer.sessionId ||
        (raw.message !== "web_connected" && raw.message !== "user_data") || peer.completed) {
        this.error(ws, "Mensagem fora da transferência autorizada."); return true;
      }
    }
    return false;
  }
}
