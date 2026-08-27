import express from "express";
import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import cors from "cors";

// ✅ Leer credencial desde variable de entorno
const firebaseKey = process.env.FIREBASE_KEY;
if (!firebaseKey) {
  console.error("❌ ERROR: Falta la variable de entorno FIREBASE_KEY");
  process.exit(1);
}
const serviceAccount = JSON.parse(firebaseKey);

// ✅ Inicializa Firebase Admin
initializeApp({
  credential: cert(serviceAccount),
});
const db = getFirestore();

const app = express();
const port = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

const clients = []; // Lista de conexiones SSE activas por usuario
const adminClients = []; // Lista de conexiones SSE activas para el panel de administración

// 📡 Conexión SSE por usuario
app.get("/events/:userId", (req, res) => {
  const userId = req.params.userId;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const horaActual = new Date();
  const hora = horaActual.getHours().toString().padStart(2, "0");
  const minutos = horaActual.getMinutes().toString().padStart(2, "0");

  const client = { userId, res };
  clients.push(client);

  req.on("close", () => {
    console.log(`❌ Cliente desconectado: ${userId} a las ${hora}:${minutos}`);
    clients.splice(clients.indexOf(client), 1);
  });

  console.log(`🟢 Cliente conectado: ${userId} a las ${hora}:${minutos}`);
});

// 📡 Canal SSE para el Panel de Administración (Todos los usuarios)
app.get("/admin/events", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  adminClients.push(res);

  try {
    const snapshot = await db.collection("usuarios").get();
    const usuariosObj = {};
    snapshot.forEach(doc => {
      usuariosObj[doc.id] = doc.data();
    });
    res.write(`data: ${JSON.stringify(usuariosObj)}\n\n`);
  } catch (e) {
    console.error("Error al obtener usuarios iniciales:", e);
  }

  req.on("close", () => {
    adminClients.splice(adminClients.indexOf(res), 1);
  });
});

// 🔄 Escucha de cambios en Firestore para notificar a Clientes y Admin
db.collection("usuarios").onSnapshot(async (snapshot) => {
  snapshot.docChanges().forEach((change) => {
    const data = change.doc.data();
    const docId = change.doc.id;

    clients.forEach((client) => {
      if (client.userId === docId) {
        client.res.write(`data: ${JSON.stringify(data)}\n\n`);
      }
    });
  });

  try {
    const snapshotTotal = await db.collection("usuarios").get();
    const usuariosObj = {};
    snapshotTotal.forEach(doc => {
      usuariosObj[doc.id] = doc.data();
    });

    adminClients.forEach(adminRes => {
      adminRes.write(`data: ${JSON.stringify(usuariosObj)}\n\n`);
    });
  } catch (e) {
    console.error("Error al actualizar admin SSE:", e);
  }
});

// 📥 Crear o actualizar usuario
app.post("/usuarios", async (req, res) => {
  const { userId, data } = req.body;

  if (!userId || !data) {
    return res.status(400).json({ error: "Faltan parámetros" });
  }

  await db.collection("usuarios").doc(userId).set(data, { merge: true });
  res.status(200).json({ success: true });
});

// 🎮 Recibir comandos del admin para un usuario
app.post("/admin/command", async (req, res) => {
  const { userId, comando } = req.body;
  if (!userId) return res.status(400).json({ error: "Falta userId" });

  try {
    await db.collection("usuarios").doc(userId).set({ comando: comando || "" }, { merge: true });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "No se pudo guardar el comando" });
  }
});

// 🗑️ Eliminar usuario desde el panel de admin
app.delete("/admin/user/:userId", async (req, res) => {
  const userId = req.params.userId;
  try {
    await db.collection("usuarios").doc(userId).delete();
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "No se pudo eliminar" });
  }
});

// ❌ Eliminar todas las conexiones
app.delete("/conexiones", (req, res) => {
  clients.forEach((client) => {
    client.res.end();
  });
  const cantidad = clients.length;
  clients.length = 0;
  console.log(`🧹 ${cantidad} conexiones eliminadas`);
  res.json({ success: true, eliminados: cantidad });
});

// ❌ Eliminar conexión de un usuario específico
app.delete("/conexiones/:userId", (req, res) => {
  const userId = req.params.userId;
  const index = clients.findIndex((c) => c.userId === userId);

  if (index !== -1) {
    clients[index].res.end();
    clients.splice(index, 1);
    console.log(`🗑️ Usuario ${userId} desconectado`);
    return res.json({ success: true, eliminado: userId });
  }

  res.status(404).json({ error: "Usuario no encontrado" });
});

// 📋 Ver usuarios conectados
app.get("/conexiones", (req, res) => {
  const usuariosConectados = clients.map((client) => client.userId);
  res.json({ total: usuariosConectados.length, usuarios: usuariosConectados });
});

// 🗑️ Eliminar usuario completo (doc + conexión)
app.delete("/usuarios-completo/:userId", async (req, res) => {
  const userId = req.params.userId;

  const index = clients.findIndex((c) => c.userId === userId);
  if (index !== -1) {
    clients[index].res.end();
    clients.splice(index, 1);
    console.log(`🔌 SSE cerrada para ${userId}`);
  }

  try {
    await db.collection("usuarios").doc(userId).delete();
    console.log(`🗑️ Documento eliminado: ${userId}`);
    res.json({ success: true, eliminado: userId });
  } catch (error) {
    console.error("❌ Error al eliminar:", error);
    res.status(500).json({ error: "No se pudo eliminar el usuario" });
  }
});

// 🛰️ Ping para saber si está online
app.post("/ping", async (req, res) => {
  const { userId, timestamp } = req.body;

  if (!userId || !timestamp) {
    return res.status(400).json({ error: "Faltan datos" });
  }

  await db.collection("usuarios").doc(userId).set({ last_active: timestamp }, { merge: true });
  res.json({ success: true });
});

app.listen(port, () => {
  console.log(`🚀 Backend en tiempo real corriendo en http://localhost:${port}`);
});
