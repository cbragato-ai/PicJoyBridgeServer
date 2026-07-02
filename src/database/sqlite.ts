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
            in_use INTEGER NOT NULL DEFAULT 0 CHECK(in_use IN (0,1)),
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

    const createTableHealtchekingLog = `
      CREATE TABLE IF NOT EXISTS healtchecking_log (
        id TEXT PRIMARY_KEY,
        code TEXT NOT NULL CHECK(length(code) = 6),
        status_log_csv TEXT,
        last_update INTEGER NOT NULL, 
        record_active INTEGER NOT NULL DEFAULT 1 CHECK(record_active IN (0,1)),
        created_at INTEGER_NOT_NULL
      )

      -- garante apenas 1 registro ativo por code
      CREATE UNIQUE INDEX IF NOT EXISTS idx_healthchecking_active
      ON healthchecking_log(code)
      WHERE record_active = 1;

      -- index para performance em updates
      CREATE INDEX IF NOT EXISTS idx_healthchecking_code
      ON healthchecking_log(code);

      CREATE INDEX IF NOT EXISTS idx_healthchecking_active_lookup
      ON healthchecking_log(record_active);
    `;
  }

  addSession(
    code: string,
    bridgeServerAddress: string,
    expiresOn: number,
    callback?: (err: Error | null, row?: any) => void
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

  updateSession(id:string, setInUse:boolean){
    const updateQuery = "UPDATE sessao SET in_use=? WHERE id=?";
    this.db.run(updateQuery, [setInUse?1:0,id],(err)=>{
      if(err){
        console.error("Error update session:", err);
      }else{
        console.log("Session updated correctly");
      }
    })
  }


  getSessionByCode(
    code: string,
    callback?: (err: Error | null, row?: any) => void
  ) {
    const selectQuery = `
        SELECT * FROM sessao WHERE code = ? and expires_on > ?;`;
    this.db.get(selectQuery, [code, Date.now()], (err, row) => {
      callback?.(err, row);
    });
  }

  clearExpiredSessions() {
    const deleteQuery = `
        DELETE FROM sessao WHERE expires_on < ?;`;
    this.db.run(deleteQuery, [Date.now()], (err) => {
      if (err) {
        console.error("Error clearing expired sessions:", err);
      } else {
        console.log("Expired sessions cleared.");
      }
    });
  }

  getSessions() {
    const selectQuery = `SELECT * FROM sessao WHERE expires_on > ?;`;
    this.db.all(selectQuery, [Date.now()], (err, rows) => {
      if (err) {
        console.error("Error retrieving sessions:", err);
      } else {
        return rows;
      }
    });
  }

  healtcheckDeactivateActualLog(code: string) {
    const query = `UPDATE healtchecking_log SET record_active = 0, last_update=? WHERE code = ? AND record_active=1`;
    this.db.run(query, [Date.now(), code], (error) => {
      if (error) {
        console.error("Erro for deactivate log of last hour", error);
      } else {
        console.log("Deactivated log of last hour!");
      }
    });
  }

  healtcheckNewLog(code: string, csvItem: string) {
    const query = `
      INSERT INTO 
        healtchecking_log ( id, code, status_log_csv, last_update, created_at, record_active )
      VALUES 
        ( ?, ?, ?, ?, ?, 1 );
    `;
    const id = uuid();
    this.db.run(query, [id, code, csvItem, Date.now(), Date.now()], (error) => {
      if (error) {
        console.error("Erro for add new log for next hour", error);
      } else {
        console.log("Created new log for next hour!");
      }
    });
  }
}

export { DataSource };
