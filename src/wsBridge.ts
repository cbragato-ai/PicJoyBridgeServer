import { WebSocket } from "ws";
import { verifyToken } from "./auth";

export type ClientInfo = {
  ws: WebSocket;
  type: "web" | "kiosk";
  sessionId?: string;
  id?: string; // client id
};

const sessionLocks = new Map<string, string>();
const clients = new Set<ClientInfo>();

export function register(ws: WebSocket, rawMsg: any) {}

export function handleMessage(ws: WebSocket, message: any) {
  switch (message.type) {
    case "healtcheck":
      {
      }
      break;
    case "register":
      {
        const tokenData = verifyToken(message.token || "");
        const sessionId = tokenData?.sessionId || message.sessionId;
        const info: ClientInfo = {
          ws,
          type: message.role,
          sessionId,
          id: message.id,
        };

        if (info.type === "web" && info.sessionId) {
          const currentLock = sessionLocks.get(info.sessionId);

          if (currentLock && currentLock !== info.id) {
            ws.send(
              JSON.stringify({
                type: "session-locked",
                sessionId: info.sessionId,
              }),
            );

            ws.close();

            return;
          }

          sessionLocks.set(info.sessionId, info.id || "");
        }

        clients.add(info);
        console.info("WS Client Registered", info);

        ws.send(
          JSON.stringify({ type: "registered", role: info.type, sessionId }),
        );

        if (info.type === "kiosk" && info.sessionId) {
          for (const c of clients) {
            if (
              c.type === "web" &&
              c.sessionId === info.sessionId &&
              c.ws.readyState === c.ws.OPEN
            ) {
              c.ws.send(
                JSON.stringify({
                  type: "device-status",
                  sessionId: info.sessionId,
                  status: "online",
                }),
              );
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
        for (const c of clients) {
          if (
            c.type === "kiosk" &&
            c.sessionId === session &&
            c.ws.readyState === c.ws.OPEN
          ) {
            c.ws.send(JSON.stringify(message));
          }
        }
      }
      break;

    case "file-meta":
      {
        const session = message.toSession;
        for (const c of clients) {
          if (
            c.type === "kiosk" &&
            c.sessionId === session &&
            c.ws.readyState === c.ws.OPEN
          ) {
            c.ws.send(JSON.stringify(message));
          }
        }
      }
      break;

    case "file-chunk":
      {
        const session = message.toSession;
        for (const c of clients) {
          if (
            c.type === "kiosk" &&
            c.sessionId === session &&
            c.ws.readyState === c.ws.OPEN
          ) {
            c.ws.send(JSON.stringify(message));
          }
        }
      }
      break;

    case "file-complete":
      {
        const session = message.toSession;
        for (const c of clients) {
          if (
            c.type === "kiosk" &&
            c.sessionId === session &&
            c.ws.readyState === c.ws.OPEN
          ) {
            c.ws.send(JSON.stringify(message));
          }
        }
      }
      break;

    // opcional: web -> enviar ack ou status para outros webs
    default:
      // ignore
      break;
  }
}

export function cleanup(ws: WebSocket) {
  for (const c of Array.from(clients)) {
    if (c.ws === ws) {
      if (c.type === "web" && c.sessionId) {
        sessionLocks.delete(c.sessionId);
      }
      clients.delete(c);
    }
  }
}

export function sendToKiosk(sessionId: string, message: any): boolean {
  let sent = false;

  for (const client of clients) {
    if (
      client.type === "kiosk" &&
      client.sessionId === sessionId &&
      client.ws.readyState === client.ws.OPEN
    ) {
      client.ws.send(JSON.stringify(message));
      sent = true;
    }
  }

  return sent;
}
