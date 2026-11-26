"use strict";

require("dotenv").config();

const express = require("express");
const session = require("express-session");
const helmet = require("helmet");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");

const messenger = require("./messenger");

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const HOST = "127.0.0.1";
const APPSTATE_PATH = path.join(
  process.env.HOME || require("os").homedir(),
  ".quasar-messenger",
  "appstate.json"
);

if (!process.env.SESSION_SECRET || !process.env.ADMIN_PASSWORD) {
  console.error("ERROR: Set SESSION_SECRET and ADMIN_PASSWORD first.");
  process.exit(1);
}

app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json({ limit: "1mb" }));

app.use(session({
  name: "quasar_dashboard",
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    secure: false,
    sameSite: "strict",
    maxAge: 8 * 60 * 60 * 1000
  }
}));

function auth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  res.status(401).json({ error: "غير مسجل الدخول" });
}

function safeEqual(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

app.post("/api/login", (req, res) => {
  const password = req.body?.password;

  if (typeof password !== "string" ||
      !safeEqual(password, process.env.ADMIN_PASSWORD)) {
    return res.status(401).json({ error: "كلمة المرور غير صحيحة" });
  }

  req.session.regenerate(err => {
    if (err) return res.status(500).json({ error: "تعذر إنشاء الجلسة" });
    req.session.authenticated = true;
    res.json({ success: true });
  });
});

app.post("/api/logout", auth, (req, res) => {
  req.session.destroy(() => {
    res.clearCookie("quasar_dashboard");
    res.json({ success: true });
  });
});

app.get("/api/status", auth, (req, res) => {
  res.json(messenger.status());
});

app.post("/api/connect", auth, async (req, res) => {
  try {
    const identifier = typeof req.body?.identifier === "string"
      ? req.body.identifier.trim()
      : typeof req.body?.email === "string"
        ? req.body.email.trim() : "";
    const password = typeof req.body?.password === "string"
      ? req.body.password : "";
    const cookie = typeof req.body?.cookie === "string"
      ? req.body.cookie.trim() : "";

    let credentials;

    if (cookie) {
      let cookieHeader = cookie;

      // Accept browser-exported JSON cookie arrays.
      if (cookie.startsWith("[")) {
        const items = JSON.parse(cookie);

        if (!Array.isArray(items) || items.length === 0) {
          throw new Error("Invalid cookie format");
        }

        const pairs = items
          .filter(item => {
            if (!item || typeof item !== "object") return false;

            const name = typeof item.name === "string" ? item.name : item.key;
            const domain = typeof item.domain === "string"
              ? item.domain.replace(/^\./, "").toLowerCase()
              : "";

            return typeof name === "string" &&
              name.length > 0 &&
              typeof item.value === "string" &&
              (domain === "facebook.com" || domain.endsWith(".facebook.com"));
          })
          .map(item => `${item.name || item.key}=${item.value}`);

        if (pairs.length === 0) {
          throw new Error("No valid Facebook cookies");
        }

        cookieHeader = pairs.join("; ");
      }

      credentials = { Cookie: cookieHeader };
    } else if (identifier && password) {
      // تمرير المعرّف كما أدخله المستخدم إلى حقل الدخول الذي تستخدمه المكتبة.
      credentials = { email: identifier, password };
    } else {
      return res.status(400).json({
        error: "أدخل البريد أو رقم الهاتف أو المعرّف مع كلمة المرور، أو Cookie."
      });
    }

    await messenger.connect(credentials);
    res.json({ success: true, status: messenger.status() });
  } catch (error) {
    const isCheckpoint =
      error?.code === "CHECKPOINT" || error?.checkpoint === true;

    const isSessionExpired =
      error?.code === "SESSION_EXPIRED";

    console.error(
      "Messenger connection failed:",
      isCheckpoint ? "CHECKPOINT" :
      isSessionExpired ? "SESSION_EXPIRED" : "CONNECTION_ERROR"
    );

    res.status(400).json({
      code: isCheckpoint ? "CHECKPOINT" :
            isSessionExpired ? "SESSION_EXPIRED" : "CONNECTION_ERROR",
      error: isCheckpoint
        ? "فيسبوك يطلب تأكيدًا أمنيًا للحساب. افتح صفحة فيسبوك الرسمية وأكمل التأكيد هناك، ثم أعد الاتصال."
        : isSessionExpired
          ? "انتهت صلاحية جلسة Messenger. أعد تسجيل الدخول."
          : "تعذر الاتصال. تحقق من بيانات الدخول أو اتصال الشبكة."
    });
  }
});

app.post("/api/start", auth, async (req, res) => {
  try {
    await messenger.start({
      message: req.body?.message,
      delay: req.body?.delay,
      selectedGroups: req.body?.selectedGroups
    });

    res.json({ success: true, status: messenger.status() });
  } catch (error) {
    res.status(400).json({ error: error.message || "تعذر التشغيل." });
  }
});

app.post("/api/stop", auth, (req, res) => {
  messenger.stop();
  res.json({ success: true, status: messenger.status() });
});

app.post("/api/disconnect", auth, async (req, res) => {
  try {
    await messenger.disconnect();
    res.json({ success: true, status: messenger.status() });
  } catch (_) {
    res.status(500).json({ error: "تعذر فصل الحساب." });
  }
});

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", service: "QUASAR-MESSENGER-LOCAL" });
});

app.use(express.static(path.join(__dirname, "../public")));

app.get("*path", (req, res) => {
  res.sendFile(path.join(__dirname, "../public/index.html"));
});

app.listen(PORT, HOST, () => {
  console.log(`QUASAR-MESSENGER local dashboard: http://${HOST}:${PORT}`);
});
