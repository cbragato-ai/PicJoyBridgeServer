import jwt from "jsonwebtoken";
const SECRET = process.env.JWT_SECRET || "changeme";

export function signSession(sessionId: string) {
  return jwt.sign({ sessionId }, SECRET, { expiresIn: "1h" });
}

export function verifyToken(token: string) {
  try {
    return jwt.verify(token, SECRET) as { sessionId: string };
  } catch (e) {
    return null;
  }
}
