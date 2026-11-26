"use strict";

const { loginAsync } = require("@dongdev/fca-unofficial");

const START_MESSAGES = [
  "تم بدء صيانة المحرك.. 🌐",
  "جاري قراءة مستوى الاتصال.🤖",
  "الشبكة جيدة و الرد قابل بدء تشغيل المحرك كوازار.📡"
];

const state = {
  ctx: null,
  api: null,
  mqtt: null,
  userID: null,
  connected: false,
  enabled: false,
  message: "",
  delay: 5,
  groups: [],
  selectedGroups: new Set(),
  activity: [],
  timers: new Map(),
  processedMessages: new Set(),
  sending: Promise.resolve()
};

function logActivity(message) {
  state.activity.push({
    message: String(message).slice(0, 300),
    time: new Date().toISOString()
  });

  if (state.activity.length > 100) {
    state.activity.splice(0, state.activity.length - 100);
  }
}

function safeError(error) {
  let message = "تعذر تنفيذ العملية";

  if (typeof error === "string") {
    message = error;
  } else if (error && typeof error === "object") {
    message =
      error.message ||
      error.error?.message ||
      error.error ||
      error.reason ||
      error.code ||
      error.type ||
      (() => {
        try {
          return JSON.stringify(error);
        } catch {
          return "تعذر قراءة تفاصيل الخطأ";
        }
      })();
  }

  return String(message).slice(0, 300);
}

function clearPendingReplies() {
  for (const timer of state.timers.values()) {
    clearTimeout(timer);
  }

  state.timers.clear();
}

function enqueueSend(text, threadID) {
  state.sending = state.sending.catch(() => {}).then(async () => {
    if (!state.api || !state.connected) {
      throw new Error("الحساب غير متصل");
    }

    const characters = Array.from(String(text));
    const chunkSize = 1800;

    for (let i = 0; i < characters.length; i += chunkSize) {
      const part = characters.slice(i, i + chunkSize).join("");
      await state.api.sendMessage(part, threadID);

      if (i + chunkSize < characters.length) {
        await new Promise(resolve => setTimeout(resolve, 700));
      }
    }
  });

  return state.sending;
}

function waitForMqttReady(mqtt, timeoutMs = 45000) {
  return new Promise((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      clearTimeout(timer);
      mqtt.removeListener("ready", onReady);
      mqtt.removeListener("error", onError);
      mqtt.removeListener("stop_listen", onStop);
    };

    const finish = (error) => {
      if (settled) return;
      settled = true;
      cleanup();

      if (error) reject(error);
      else resolve();
    };

    const onReady = () => finish();

    const onError = error => {
      // توافق مع المكتبة التي تمرر أحداث الحالة عبر وسيط error.
      if (error && typeof error === "object" && error.type === "ready" && error.error == null) {
        finish();
        return;
      }

      if (error && typeof error === "object" && error.type === "stop_listen") {
        finish(new Error(
          "توقف مستمع Messenger: " + safeError(error.error || error)
        ));
        return;
      }

      finish(new Error(
        "خطأ في اتصال MQTT: " + safeError(error)
      ));
    };

    const onStop = event => {
      finish(new Error(
        "توقف مستمع Messenger: " + safeError(event && event.error ? event.error : event)
      ));
    };

    const timer = setTimeout(() => {
      finish(new Error(
        "انتهت مهلة انتظار جاهزية MQTT (45 ثانية)"
      ));
    }, timeoutMs);

    mqtt.on("ready", onReady);
    mqtt.on("error", onError);
    mqtt.on("stop_listen", onStop);
  });
}

async function connect(credentials) {
  if (state.connected) {
    throw new Error("الحساب متصل بالفعل");
  }

  if (!credentials || typeof credentials !== "object") {
    throw new Error("بيانات الربط غير صالحة");
  }

  const ctx = await loginAsync(credentials, {
    listenEvents: true,
    selfListen: false,
    emitReady: true
  });

  if (!ctx || !ctx.api) {
    throw new Error("لم تُنشأ جلسة Messenger");
  }

  state.ctx = ctx;
  state.api = ctx.api;
  state.userID = String(ctx.userID || "");
  state.connected = true;

  try {
    const threads = await state.api.getThreadList(100, null, ["INBOX"]);

    state.groups = (Array.isArray(threads) ? threads : [])
      .filter(thread => thread && thread.isGroup && thread.threadID)
      .map(thread => ({
        id: String(thread.threadID),
        name: String(thread.name || "قروب بدون اسم")
      }));

    logActivity("تم الاتصال وتحميل القروبات.");
    return state.groups;
  } catch (error) {
    logActivity(
      "تم الاتصال، لكن تعذر تحميل القروبات: " + safeError(error)
    );
    throw error;
  }
}

function attachListener() {
  if (!state.api) {
    throw new Error("الحساب غير متصل");
  }

  if (state.mqtt) {
    return state.mqtt;
  }

  const mqtt = state.api.listenMqtt();

  if (!mqtt || typeof mqtt.on !== "function") {
    throw new Error("تعذر إنشاء مستمع رسائل Messenger");
  }

  state.mqtt = mqtt;

  const handleMessage = event => {
    if (!event || !["message", "message_reply"].includes(event.type)) return;
    if (!state.enabled || !state.connected) return;
    if (!event.threadID || !event.senderID || !event.messageID) return;
    if (String(event.senderID) === state.userID) return;

    const threadID = String(event.threadID);
    const messageID = String(event.messageID);

    if (!state.selectedGroups.has(threadID)) return;
    if (typeof event.body !== "string" || !event.body.trim()) return;

    // منع تكرار الرسالة إذا أصدرتها المكتبة بأكثر من نوع حدث.
    if (state.processedMessages.has(messageID)) return;

    state.processedMessages.add(messageID);

    if (state.processedMessages.size > 2000) {
      const oldest = state.processedMessages.values().next().value;
      state.processedMessages.delete(oldest);
    }

    // مؤقت مستقل لكل رسالة، وليس لكل قروب.
    const timer = setTimeout(async () => {
      state.timers.delete(messageID);

      if (!state.enabled || !state.connected) return;
      if (!state.selectedGroups.has(threadID)) return;

      try {
        await enqueueSend(state.message, threadID);
        logActivity("تم الرد على رسالة في قروب: " + threadID);
      } catch (error) {
        logActivity("تعذر إرسال الرد: " + safeError(error));
      }
    }, state.delay * 1000);

    state.timers.set(messageID, timer);
  };

  mqtt.on("message", handleMessage);
  mqtt.on("message_reply", handleMessage);

  mqtt.on("error", error => {
    // حدث ready يصل من بعض إصدارات المكتبة داخل حدث error.
    if (error && typeof error === "object" && error.type === "ready" && error.error == null) {
      return;
    }

    if (error && typeof error === "object" && error.type === "stop_listen") {
      logActivity("توقف مستمع الرسائل: " + safeError(error.error || error));
      return;
    }

    logActivity("خطأ في اتصال الرسائل اللحظية: " + safeError(error));
  });

  mqtt.on("stop_listen", event => {
    logActivity("توقف مستمع الرسائل: " + safeError(event));
  });

  return mqtt;
}

async function start(options = {}) {
  if (!state.connected || !state.api) {
    throw new Error("اربط الحساب أولاً");
  }

  if (state.enabled) {
    throw new Error("الرد التلقائي يعمل بالفعل");
  }

  const message = options.message;
  const delay = Number(options.delay);
  const selectedGroups = options.selectedGroups;

  if (typeof message !== "string" || !message.trim() || message.length > 100000) {
    throw new Error("اكتب رسالة صحيحة لا تتجاوز 100000 حرف");
  }

  if (!Number.isInteger(delay) || delay < 5 || delay > 3600) {
    throw new Error("التأخير يجب أن يكون من 5 إلى 3600 ثانية");
  }

  if (!Array.isArray(selectedGroups) || selectedGroups.length === 0) {
    throw new Error("اختر قروبًا واحدًا على الأقل");
  }

  const available = new Set(state.groups.map(group => group.id));
  const chosen = [...new Set(selectedGroups.map(String))];

  if (chosen.some(id => !available.has(id))) {
    throw new Error("توجد قروبات غير صالحة في الاختيار");
  }

  state.message = message;
  state.delay = delay;
  state.selectedGroups = new Set(chosen);

  try {
    // تشغيل مستمع MQTT أولًا.
    const mqtt = attachListener();

    logActivity("جاري انتظار جاهزية اتصال MQTT...");

    // لا نرسل أي رسالة قبل حدث ready الحقيقي.
    await waitForMqttReady(mqtt);

    logActivity("اتصال MQTT جاهز.");

    state.enabled = true;

    // رسائل البداية بعد التأكد من جاهزية الاتصال.
    for (const groupID of chosen) {
      for (const text of START_MESSAGES) {
        await enqueueSend(text, groupID);
        await new Promise(resolve => setTimeout(resolve, 1500));
      }
    }

    logActivity("تم تشغيل الرد التلقائي للقروبات المختارة.");
  } catch (error) {
    state.enabled = false;
    clearPendingReplies();

    if (state.mqtt && state.api && state.api.stopListeningAsync) {
      try {
        await state.api.stopListeningAsync();
      } catch (_) {}
    }

    state.mqtt = null;
    throw error;
  }
}

function stop() {
  state.enabled = false;
  clearPendingReplies();

  logActivity("تم إيقاف الرد التلقائي.");
}

async function disconnect() {
  stop();

  if (state.mqtt && state.api && state.api.stopListeningAsync) {
    try {
      await state.api.stopListeningAsync();
    } catch (error) {
      logActivity("تعذر إيقاف مستمع MQTT: " + safeError(error));
    }
  }

  state.mqtt = null;
  state.api = null;
  state.ctx = null;
  state.userID = null;
  state.connected = false;
  state.groups = [];
  state.selectedGroups.clear();
  state.processedMessages.clear();

  logActivity("تم فصل الحساب.");
}

function status() {
  return {
    connected: state.connected,
    enabled: state.enabled,
    message: state.message,
    delay: state.delay,
    groups: state.groups,
    selectedGroups: [...state.selectedGroups],
    activity: state.activity.slice(-30).reverse()
  };
}

module.exports = {
  connect,
  start,
  stop,
  disconnect,
  status
};
