import { WebSocket } from "ws";
import { ConfirmedTransfers } from "./confirmedTransfers";
import { CONFIRMED_PROTOCOL, FLOW_LIMITS } from "./confirmedProtocol";
import { verifyToken } from "./auth";
import { DataSource } from "./database/sqlite";
import { UPLOAD_LIMITS, UPLOAD_PROTOCOL_VERSION, UploadError, UploadSession } from "./uploadProtocol";

export type ClientInfo = {
  ws: WebSocket;
  type: "web" | "kiosk";
  sessionId?: string;
  id?: string; // client id
  upload?: UploadSession;
  uploadFailed?: boolean;
  protocolVersion?: number;
};

const sessionLocks = new Map<string, string>();
const clients = new Map<WebSocket, ClientInfo>();
const datasource = new DataSource();
const confirmed = new ConfirmedTransfers(send);
export const confirmedUploadStats = () => ({ ...confirmed.snapshot(),
  transportBufferedBytes: [...clients.values()].reduce((sum, client) => sum + (client.ws.bufferedAmount || 0), 0) });
export const hasConfirmedKiosk = (sessionId: string) => confirmed.hasKiosk(sessionId);
export const claimUploadPermission = (sessionId: string, ticket: unknown, customerId: string) => confirmed.claim(sessionId, ticket, customerId);

export function register(ws: WebSocket, rawMsg: any) {}

function send(ws: WebSocket, message: unknown): boolean {
  if (ws.readyState !== ws.OPEN) return false;
  try {
    const text = JSON.stringify(message);
    const bytes = Buffer.byteLength(text);
    const buffered = [...clients.values()].reduce((sum, client) => sum + (client.ws.bufferedAmount || 0), 0);
    if ((ws.bufferedAmount || 0) + bytes > FLOW_LIMITS.frameBytes || buffered + bytes > FLOW_LIMITS.globalWindowBytes) {
      cleanup(ws); ws.terminate(); return false;
    }
    ws.send(text, (error) => {
      if (error) {
        console.error("WS send error:", error.message);
        cleanup(ws);
        ws.terminate();
      }
    });
    return true;
  } catch (error) {
    console.error("WS send error:", error);
    cleanup(ws);
    ws.terminate();
    return false;
  }
}

function rejectUpload(ws: WebSocket, code: string, message: string) {
  const info = clients.get(ws);
  if (info?.uploadFailed) return;
  if (info) info.uploadFailed = true;
  const error = { type: "transfer-error", protocolVersion: UPLOAD_PROTOCOL_VERSION, code, message };
  send(ws, error);
  if (info?.type === "web" && info.sessionId) sendToKiosk(info.sessionId, error);
  ws.close(1008, code);
}

export function handleMessage(ws: WebSocket, message: any) {
  if (ws.readyState !== ws.OPEN || !message || typeof message !== "object") return;
  if (clients.get(ws)?.uploadFailed) return;
  if (message.type !== "register" && confirmed.handle(ws, message)) return;
  if (message.type !== "register" && message.type !== "healtcheck" && !clients.has(ws)) {
    rejectUpload(ws, "UNREGISTERED", "Registre a conexão antes do envio."); return;
  }
  switch (message.type) {
    case "healtcheck":
      {
      }
      break;
    case "unlock":
      {
        const owner = clients.get(ws);
        if (owner?.type !== "kiosk" || owner.sessionId !== message.sessionId) {
          rejectUpload(ws, "INVALID_SESSION", "Somente o totem pode liberar sua sessão."); return;
        }
        if (owner.protocolVersion === CONFIRMED_PROTOCOL) confirmed.unlock(ws, message.maxFiles);
        const sessionId: String = message.sessionId;
        console.log("Unlock Kiosk Session", sessionId);
        sessionLocks.set(sessionId.toString(), "");
        datasource.getSessionByCode(sessionId.toUpperCase(), (err, row) => {
          if (err) {
            console.error("Error retrieving session:", err);
          } else {
            console.log(`Session Row`, row);
            if (row) {
              console.log("Update database session to unlock", row.id);
              datasource.updateSession(row.id, false);
            } else {
              console.log(`Session not found for code ${sessionId}`);
            }
          }
        });
      }
      break;
    case "register":
      {
        const tokenData = verifyToken(message.token || "");
        const sessionId = tokenData?.sessionId || message.sessionId;
        if ((message.role !== "web" && message.role !== "kiosk") || typeof sessionId !== "string" || !sessionId) {
          rejectUpload(ws, "INVALID_REGISTRATION", "Registro de conexão inválido.");
          return;
        }
        if (message.protocolVersion !== undefined && message.protocolVersion !== UPLOAD_PROTOCOL_VERSION && message.protocolVersion !== CONFIRMED_PROTOCOL) {
          rejectUpload(ws, "UNSUPPORTED_PROTOCOL", "Versão de envio não suportada.");
          return;
        }
        const previous = clients.get(ws);
        if (previous && (previous.type !== message.role || previous.sessionId !== sessionId || previous.id !== message.id || previous.protocolVersion !== message.protocolVersion)) {
          rejectUpload(ws, "INVALID_REGISTRATION", "A conexão já pertence a outra sessão.");
          return;
        }
        if (message.role === "kiosk" && [...clients.values()].some(client => client.ws !== ws && client.type === "kiosk" &&
          client.sessionId === sessionId && client.ws.readyState === client.ws.OPEN)) {
          rejectUpload(ws, "DUPLICATE_KIOSK", "Já existe um totem conectado nesta sessão."); return;
        }
        try {
          if (message.protocolVersion === CONFIRMED_PROTOCOL) confirmed.register(ws, message.role, sessionId, message.id, message.uploadTicket);
          else if (message.role === "web" && confirmed.hasKiosk(sessionId)) {
            rejectUpload(ws, "UPGRADE_REQUIRED", "Este totem exige o novo protocolo de envio."); return;
          }
        } catch (error) {
          rejectUpload(ws, "UPLOAD_PERMISSION_REQUIRED", error instanceof Error ? error.message : "Permissão de envio inválida."); return;
        }
        const info: ClientInfo = {
          ws,
          type: message.role,
          sessionId,
          protocolVersion: message.protocolVersion,
          id: message.id,
          upload: previous?.upload ?? (message.role === "web" && message.protocolVersion === UPLOAD_PROTOCOL_VERSION
            ? new UploadSession() : undefined),
        };

        if (info.type === "web" && info.sessionId) {
          const currentLock = sessionLocks.get(info.sessionId);

          if (currentLock && currentLock !== info.id) {
            send(ws, {
                type: "session-locked",
                sessionId: info.sessionId,
            });

            ws.close();

            return;
          }

          sessionLocks.set(info.sessionId, info.id || "");
        }

        clients.set(ws, info);
        console.info("WS Client Registered", { role: info.type, sessionId });

        send(ws, { type: "registered", role: info.type, sessionId,
          ...([UPLOAD_PROTOCOL_VERSION, CONFIRMED_PROTOCOL].includes(message.protocolVersion)
            ? { protocolVersion: message.protocolVersion, uploadLimits: message.protocolVersion === CONFIRMED_PROTOCOL ? { ...UPLOAD_LIMITS, maxFiles: confirmed.capacity(ws) } : UPLOAD_LIMITS,
                ...(message.protocolVersion === CONFIRMED_PROTOCOL ? { flowLimits: FLOW_LIMITS, printCapacity: { maxFiles: confirmed.capacity(ws) } } : {}) } : {}),
        });

        if (info.type === "kiosk" && info.sessionId) {
          for (const c of clients.values()) {
            if (
              c.type === "web" &&
              c.sessionId === info.sessionId &&
              c.ws.readyState === c.ws.OPEN
            ) {
              send(c.ws, {
                  type: "device-status",
                  sessionId: info.sessionId,
                  status: "online",
              });
            }
          }
        }
      }
      break;

    case "notify":
      {
        // Encaminha notify para o kiosk correto
        const session =
          message.toSession || message.sessionId || message.session;
        const info = clients.get(ws);
        if (info?.type !== "web" || info.sessionId !== session) {
          rejectUpload(ws, "INVALID_SESSION", "O envio não pertence a esta sessão."); return;
        }
        if (info?.upload) {
          if (info.sessionId !== session) {
            rejectUpload(ws, "INVALID_SESSION", "O envio não pertence a esta sessão.");
            return;
          }
          if (message.message === "transfer_complete") {
            try { info.upload.finish(); }
            catch (error) {
              rejectUpload(ws, error instanceof UploadError ? error.code : "INVALID_UPLOAD",
                error instanceof Error ? error.message : "Envio inválido.");
              return;
            }
          }
        }
        if (!sendToKiosk(session, message) && info?.upload) {
          rejectUpload(ws, "KIOSK_UNAVAILABLE", "O totem está desconectado. Reinicie o envio junto ao totem.");
        }
      }
      break;

    case "file-meta":
    case "file-chunk":
    case "file-complete":
      {
        const session = message.toSession;
        const info = clients.get(ws);
        if (info?.type !== "web" || info.sessionId !== session) {
          rejectUpload(ws, "INVALID_SESSION", "O envio não pertence a esta sessão."); return;
        }
        if (info?.upload) {
          if (info.sessionId !== session) {
            rejectUpload(ws, "INVALID_SESSION", "O envio não pertence a esta sessão.");
            return;
          }
          try { info.upload.accept(message); }
          catch (error) {
            rejectUpload(ws, error instanceof UploadError ? error.code : "INVALID_UPLOAD",
              error instanceof Error ? error.message : "Envio inválido.");
            return;
          }
        }
        if (!sendToKiosk(session, message) && info?.upload) {
          rejectUpload(ws, "KIOSK_UNAVAILABLE", "O totem está desconectado. Reinicie o envio junto ao totem.");
        }
      }
      break;

    case "transfer-error": {
      const info = clients.get(ws);
      if (info?.type !== "kiosk" || !info.sessionId) return;
      for (const client of clients.values()) {
        if (client.type === "web" && client.sessionId === info.sessionId) {
          client.uploadFailed = true;
          send(client.ws, { type: "transfer-error", protocolVersion: UPLOAD_PROTOCOL_VERSION,
            code: "KIOSK_UPLOAD_ERROR", message: "O totem não conseguiu receber as fotos. Reinicie o envio junto ao totem." });
          client.ws.close(1008, "KIOSK_UPLOAD_ERROR");
        }
      }
      break;
    }

    // opcional: web -> enviar ack ou status para outros webs
    default:
      // ignore
      break;
  }
}

export function cleanup(ws: WebSocket) {
  confirmed.cleanup(ws);
  const info = clients.get(ws);
  clients.delete(ws);
  if (info?.type === "web" && info.protocolVersion !== CONFIRMED_PROTOCOL && info.sessionId &&
      ![...clients.values()].some(c => c.type === "web" && c.sessionId === info.sessionId)) {
    sessionLocks.delete(info.sessionId);
  }
}

export function sendToKiosk(sessionId: string, message: any): boolean {
  let sent = false;

  for (const client of clients.values()) {
    if (
      client.type === "kiosk" &&
      client.sessionId === sessionId &&
      client.ws.readyState === client.ws.OPEN
    ) {
      sent = send(client.ws, message) || sent;
    }
  }

  return sent;
}
