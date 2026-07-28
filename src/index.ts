import express, { response } from "express";
import http from "http";
import cors from "cors";
import { WebSocketServer } from "ws";
import { handleMessage, cleanup, sendToKiosk } from "./wsBridge";
import { DataSource } from "./database/sqlite";
import { msUntilNextMidnight } from "./util/MsUntilNextMidnight";

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true,
  }),
);
app.options("*", cors()); // Permite pré-flight para todas as rotas
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const datasource = new DataSource();

// Clear expired sessions every day at midnight
console.info(
  `Scheduling daily expired session cleanup, to ${msUntilNextMidnight()}ms from now.`,
);
setTimeout(() => {
  datasource.clearExpiredSessions();
  setInterval(
    () => {
      datasource.clearExpiredSessions();
    },
    24 * 60 * 60 * 1000,
  );
}, msUntilNextMidnight());

// upgrade http protocol to WebSocket protocol
server.on("upgrade", (request, socket, head) => {
  wss.handleUpgrade(request, socket as any, head, (ws) => {
    wss.emit("connection", ws, request);
  });
});

// Quando um cliente novo se conecta
wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    try {
      let text: string;

      if (typeof raw === "string") {
        text = raw;
      } else if (raw instanceof ArrayBuffer) {
        text = new TextDecoder("utf8").decode(raw);
      } else if (Buffer.isBuffer(raw)) {
        text = raw.toString("utf8");
      } else {
        console.warn("WS recebeu tipo desconhecido:", raw);
        return;
      }

      const data = JSON.parse(text);
      handleMessage(ws, data);
    } catch (e) {
      console.error("Bridge parse error:", e);
    }
  });

  ws.on("close", () => cleanup(ws));
});

app.get("/health", (request, response) => response.json({ ok: true }));
app.get("/", (request, response) => response.json({ ok: true }));
app.get("/sessions", (request, response) => {
  const sessions = datasource.getSessions();
  response.json(sessions);
});

app.post("/session/:code", (request, response) => {
  try {
    const code = request.params.code.toUpperCase();
    const session = datasource.getSessionByCode(code, (err, row) => {
      if (err) {
        console.error("Error retrieving session:", err);
        response.status(500).json({ error: "Internal server error" });
      } else {
        if (row) {
          console.log(`Return old session for ${code}`, row);
          response.json({
            id: row.id,
            code: row.code,
            bridgeServerAddress: row.bridge_server_address,
            expiresOn: row.expires_on,
          });
        } else {
          const row = datasource.addSession(
            code,
            request.body.bridgeServerAddress,
            Date.now() + 3600 * 1000,
            (err, row) => {
              if (err) {
                console.error("Error creating session:", err);
                response.status(500).json({ error: "Internal server error" });
              } else {
                console.log(`Created new Session for ${code}`, row);
                response
                  .status(201)
                  .json({ message: "Session created", session: row });
              }
            },
          );
        }
      }
    });
  } catch (error) {
    console.error("Error handling session request:", error);
    response.status(500).json({ error: "Internal server error" });
  }
});

app.get("/session/:code/:customerId", (request, response) => {
  const code = request.params.code.toUpperCase();
  const customerId = request.params.customerId;
  console.log(code);
  datasource.getSessionByCode(code, (err, row) => {
    console.log(row);
    if (err) {
      console.error("Error retrieving session:", err);
      response.status(500).json({ error: "Internal server error" });
    } else {
      if (row) {
        if (row.in_use == 0) {
          datasource.updateSession(row.id, true, customerId);
        }
        response.json({
          id: row.id,
          code: row.code,
          bridgeServerAddress: row.bridge_server_address,
          expiresOn: row.expires_on,
          inUse: row.in_use == 1,
          customerId: row.customer_id,
        });
      } else {
        response
          .status(404)
          .json({ error: "Este kiosk pode estar desconectado da internet" });
      }
    }
  });
});

app.put("/session/:code", (request, response) => {
  const code = request.params.code.toUpperCase();
  console.log(code);
  datasource.getSessionByCode(code, (err, row) => {
    if (err) {
      console.error("Error retrieving session:", err);
      response.status(500).json({ error: "Internal server error" });
    } else {
      if (row) {
        datasource.updateSession(row.id, false);
      }
    }
  });
});

app.post("/command/:code/:command", (request, response) => {
  const code = request.params.code.toUpperCase();
  const command = request.params.command;

  datasource.getSessionByCode(code, (err, row) => {
    if (err) {
      console.error(err);
      return response.status(500).json({
        error: "Internal server error",
      });
    }

    if (!row) {
      return response.status(404).json({
        error: "Session not found",
      });
    }

    const sent = sendToKiosk(row.code, {
      type: "command",
      command,
      payload: request.body,
    });

    if (!sent) {
      return response.status(404).json({
        error: "Kiosk is not connected",
      });
    }

    response.json({
      success: true,
      command,
      session: row.code,
    });
  });
});

const port = Number(process.env.PORT || 3000);
server.listen(port, () => console.log(`Bridge running on ${port}`));
