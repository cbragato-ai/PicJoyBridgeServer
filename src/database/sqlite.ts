import sqlite3 from "sqlite3";
import path from "path";
import { v4 as uuid } from "uuid";

class DataSource {
  private dbPath = path.resolve(__dirname, "database.sqlite");
  private db: sqlite3.Database;

  constructor() {
    sqlite3.verbose();
    this.db = new sqlite3.Database(this.dbPath, (err) => {
      if (err) {
        console.error("Error connecting to the SQLite database:", err);
      } else {
        console.log("Connected to the SQLite database.");
      }
    });
    this.initialize();
  }

  private initialize() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS sessao (
            id TEXT PRIMARY KEY,                 -- UUID como string
            code TEXT NOT NULL CHECK(length(code) = 6),
            bridge_server_address TEXT NOT NULL,
            expires_on INTEGER NOT NULL           -- timestamp (Unix time)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sessao_code ON sessao(code);
        CREATE INDEX IF NOT EXISTS idx_sessao_expires ON sessao(expires_on);
        `;
    this.db.run(createTableQuery, (err) => {
      if (err) {
        console.error("Error creating table:", err);
      } else {
        console.log("Table created or already exists.");
      }
    });
  }

  addSession(
    code: string,
    bridgeServerAddress: string,
    expiresOn: number,
    callback: (err: Error | null, row?: any) => void
  ) {
    const id = uuid();
    const insertQuery = `
        INSERT INTO sessao (id, code, bridge_server_address, expires_on)
        VALUES (?, ?, ?, ?);`;
    this.db.run(
      insertQuery,
      [id, code, bridgeServerAddress, expiresOn],
      (err) => {
        if (err) {
          console.error("Error inserting session:", err);
        } else {
          console.log("Session inserted successfully.");
        }
      }
    );
    this.getSessionByCode(code, callback);
  }

  getSessionByCode(
    code: string,
    callback: (err: Error | null, row?: any) => void
  ) {
    const selectQuery = `
        SELECT * FROM sessao WHERE code = ? and expires_on > ?;`;
    this.db.get(selectQuery, [code, Date.now()], (err, row) => {
      callback(err, row);
    });
  }
}

export { DataSource };
