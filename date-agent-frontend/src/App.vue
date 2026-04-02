<template>
  <div class="chat-page">
    <header class="site-header">
      <div class="utility-bar">
        <span class="utility-label">DATE AGENT</span>
        <span class="utility-divider"></span>
        <span class="utility-text">数据查询控制台</span>
      </div>

      <div class="masthead">
        <div class="seal">DA</div>

        <div class="masthead-copy">
          <p class="eyebrow">INTELLIGENT DATA SERVICE SYSTEM</p>
          <h1>数据分析问答</h1>
          <p class="lead">输入业务问题后，系统会返回执行进度和查询结果。</p>
        </div>
      </div>
    </header>

    <main class="board-shell">
      <section class="board-heading">
        <div>
          <p class="section-kicker">Query</p>
          <h2>问答记录</h2>
        </div>
        <p class="section-note">支持查看执行阶段、错误信息和表格结果。</p>
      </section>

      <section ref="messagesEl" class="messages">
        <div v-if="!messages.length" class="empty-state">
          <div class="empty-intro">
            <p class="section-kicker">使用说明</p>
            <h3>输入自然语言问题即可开始查询</h3>
            <p>
              可以直接描述统计口径、时间范围或筛选条件，例如“查询近 30 天销售额变化”。
            </p>
          </div>

          <div class="empty-grid">
            <article class="empty-card">
              <span class="card-index">01</span>
              <strong>输入问题</strong>
              <p>使用自然语言描述你要查询的业务数据。</p>
            </article>
            <article class="empty-card">
              <span class="card-index">02</span>
              <strong>查看进度</strong>
              <p>系统会实时返回当前执行阶段。</p>
            </article>
            <article class="empty-card">
              <span class="card-index">03</span>
              <strong>查看结果</strong>
              <p>查询完成后会展示结果表格或错误信息。</p>
            </article>
          </div>
        </div>

        <article
          v-for="(msg, index) in messages"
          :key="index"
          :class="['message-row', msg.role, `type-${msg.type}`]"
        >
          <div class="message-marker">
            {{ msg.role === "assistant" ? "DA" : "Q" }}
          </div>

          <div class="message-body">
            <div class="message-meta">
              <span class="speaker">
                {{ msg.role === "assistant" ? "Data Agent" : "用户提问" }}
              </span>
              <span class="meta-divider"></span>
              <span class="message-tag">
                {{
                  msg.type === "steps"
                    ? "执行进度"
                    : msg.type === "table"
                      ? "查询结果"
                      : msg.type === "error"
                        ? "错误信息"
                        : "文本消息"
                }}
              </span>
            </div>

            <div class="bubble">
              <div v-if="msg.type === 'text'" class="bubble-text">
                {{ msg.content }}
              </div>

              <div v-else-if="msg.type === 'steps'" class="steps">
                <div v-for="(step, sIdx) in msg.steps" :key="sIdx" class="step">
                  <span class="dot" :class="step.status"></span>
                  <span class="step-text">{{ step.text }}</span>
                </div>
              </div>

              <div v-else-if="msg.type === 'table'" class="table-wrap">
                <table class="result-table">
                  <thead>
                    <tr>
                      <th v-for="col in msg.columns" :key="col">
                        {{ col }}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(row, rIdx) in msg.rows" :key="rIdx">
                      <td v-for="col in msg.columns" :key="col">
                        {{ row[col] }}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div v-else-if="msg.type === 'error'" class="error-text">
                {{ msg.content }}
              </div>
            </div>
          </div>
        </article>

        <div class="messages-bottom-spacer"></div>
      </section>

      <footer class="input-shell">
        <div class="input-frame" :class="{ loading }">
          <div class="input-main">
            <label class="input-label" for="question-input">输入问题</label>
            <input
              id="question-input"
              v-model="question"
              @keyup.enter="sendQuestion"
              placeholder="例如：查询近 30 天各地区销售额变化"
            />
          </div>

          <div class="input-actions">
            <span class="input-hint">
              {{ loading ? "正在接收结果" : "按 Enter 快速发送" }}
            </span>
            <button @click="sendQuestion" :disabled="loading">
              {{ loading ? "处理中..." : "发送查询" }}
            </button>
          </div>
        </div>
      </footer>
    </main>
  </div>
</template>

<script setup>
import { nextTick, ref } from "vue";

const API_URL = "/api/query";

const question = ref("");
const loading = ref(false);
const messages = ref([]);
const messagesEl = ref(null);

function scrollToBottom() {
  const el = messagesEl.value;
  if (!el) return;
  el.scrollTop = el.scrollHeight;
}

async function sendQuestion() {
  if (!question.value || loading.value) return;

  const q = question.value;
  question.value = "";
  loading.value = true;

  messages.value.push({ role: "user", type: "text", content: q });

  const stepIndex =
    messages.value.push({
      role: "assistant",
      type: "steps",
      steps: [],
    }) - 1;

  await nextTick();
  scrollToBottom();

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: q }),
    });

    if (!response.body) throw new Error("服务端未返回可读取的数据流");

    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const events = buffer.split("\n\n");
      buffer = events.pop() ?? "";

      for (const evt of events) {
        const line = evt.trim();
        if (!line.startsWith("data:")) continue;

        let data;
        try {
          data = JSON.parse(line.replace(/^data:\s*/, ""));
        } catch {
          continue;
        }

        const steps = messages.value[stepIndex].steps;

        if (data.stage) {
          const last = steps.at(-1);
          if (last && last.status === "running") last.status = "success";
          steps.push({ text: data.stage, status: "running" });
        } else if (data.error) {
          const last = steps.at(-1);
          if (last) last.status = "error";
          messages.value.push({
            role: "assistant",
            type: "error",
            content: data.error,
          });
        } else if (Array.isArray(data.result)) {
          const last = steps.at(-1);
          if (last) last.status = "success";
          messages.value.push({
            role: "assistant",
            type: "table",
            columns: Object.keys(data.result[0] || {}),
            rows: data.result,
          });
        }

        await nextTick();
        scrollToBottom();
      }
    }
  } catch (e) {
    messages.value.push({
      role: "assistant",
      type: "error",
      content: e?.message || "请求过程中发生异常",
    });
  } finally {
    loading.value = false;
    await nextTick();
    scrollToBottom();
  }
}
</script>

<style scoped>
.chat-page {
  min-height: 100vh;
  background:
    linear-gradient(180deg, var(--navy-900) 0 352px, transparent 352px),
    var(--paper);
}

.site-header {
  max-width: 1320px;
  margin: 0 auto;
  padding: 0 28px;
}

.utility-bar {
  display: flex;
  align-items: center;
  gap: 14px;
  min-height: 56px;
  color: rgba(255, 255, 255, 0.92);
  font-size: 12px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.utility-label {
  font-weight: 700;
}

.utility-divider {
  width: 1px;
  height: 16px;
  background: rgba(255, 255, 255, 0.36);
}

.masthead {
  display: grid;
  grid-template-columns: 112px minmax(0, 1fr);
  gap: 28px;
  align-items: start;
  padding: 34px 0 40px;
  color: #fff;
}

.seal {
  display: grid;
  place-items: center;
  width: 96px;
  height: 96px;
  border: 2px solid rgba(255, 255, 255, 0.82);
  border-radius: 50%;
  font-family: var(--font-serif);
  font-size: 28px;
  letter-spacing: 0.12em;
}

.eyebrow {
  margin: 0 0 12px;
  color: rgba(255, 255, 255, 0.82);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.16em;
  text-transform: uppercase;
}

.masthead-copy h1,
.board-heading h2,
.empty-intro h3 {
  font-family: var(--font-serif);
  font-weight: 700;
  letter-spacing: -0.02em;
}

.masthead-copy h1 {
  margin: 0;
  font-size: clamp(2.5rem, 4vw, 4.4rem);
  line-height: 1.02;
}

.lead {
  max-width: 760px;
  margin: 18px 0 0;
  color: rgba(255, 255, 255, 0.92);
  font-size: 17px;
  line-height: 1.7;
}

.board-shell {
  max-width: 1320px;
  margin: 0 auto;
  padding: 0 28px 28px;
}

.board-heading {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  align-items: end;
  padding: 28px 32px 24px;
  background: #fff;
  border: 1px solid var(--line);
  border-bottom: none;
}

.section-kicker {
  margin: 0 0 10px;
  color: var(--navy-700);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.board-heading h2 {
  margin: 0;
  color: var(--navy-900);
  font-size: clamp(2rem, 2.6vw, 2.8rem);
  line-height: 1.05;
}

.section-note {
  max-width: 520px;
  margin: 0;
  color: var(--ink);
  line-height: 1.7;
  text-align: right;
}

.messages {
  height: calc(100vh - 578px);
  overflow-y: auto;
  padding: 28px 32px;
  background: #fff;
  border: 1px solid var(--line);
}

.messages::-webkit-scrollbar {
  width: 10px;
}

.messages::-webkit-scrollbar-thumb {
  background: #b8c2cf;
}

.empty-state {
  padding: 4px 0 10px;
}

.empty-intro {
  max-width: 760px;
  padding-bottom: 24px;
  border-bottom: 1px solid var(--line);
}

.empty-intro h3 {
  margin: 0 0 14px;
  color: var(--navy-900);
  font-size: clamp(1.7rem, 2.1vw, 2.3rem);
  line-height: 1.15;
}

.empty-intro p:last-child {
  margin: 0;
  color: var(--ink);
  line-height: 1.8;
}

.empty-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
  margin-top: 26px;
}

.empty-card {
  padding: 22px 22px 20px;
  background: #f7f5f0;
  border: 1px solid var(--line);
}

.card-index {
  display: inline-block;
  padding-bottom: 10px;
  border-bottom: 2px solid var(--navy-700);
  color: var(--navy-700);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.14em;
}

.empty-card strong {
  display: block;
  margin: 18px 0 8px;
  color: var(--navy-900);
  font-size: 1.05rem;
}

.empty-card p {
  margin: 0;
  color: var(--ink);
  line-height: 1.7;
}

.message-row {
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr);
  gap: 18px;
  padding: 20px 0;
  border-top: 1px solid var(--line);
}

.message-marker {
  display: grid;
  place-items: center;
  width: 42px;
  height: 42px;
  border: 1px solid var(--navy-300);
  border-radius: 50%;
  color: var(--navy-800);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.12em;
}

.message-row.user .message-marker {
  background: var(--navy-900);
  border-color: var(--navy-900);
  color: #fff;
}

.message-body {
  min-width: 0;
}

.message-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
  color: var(--ink);
  font-size: 13px;
}

.speaker {
  color: var(--navy-900);
  font-weight: 700;
}

.meta-divider {
  width: 20px;
  height: 1px;
  background: var(--line-strong);
}

.message-tag {
  color: var(--ink-soft);
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.bubble {
  padding: 18px 20px;
  border-left: 4px solid var(--navy-700);
  background: #f8f6f1;
}

.message-row.user .bubble {
  background: #eef2f6;
  border-left-color: var(--red);
}

.bubble-text,
.step-text,
.error-text {
  color: var(--ink);
  line-height: 1.8;
  word-break: break-word;
}

.steps {
  display: grid;
  gap: 12px;
}

.step {
  display: flex;
  align-items: start;
  gap: 12px;
}

.dot {
  width: 10px;
  height: 10px;
  margin-top: 10px;
  border-radius: 50%;
  background: var(--navy-500);
  flex: 0 0 auto;
}

.dot.running {
  background: var(--gold);
}

.dot.success {
  background: #2b6d53;
}

.dot.error {
  background: var(--red);
}

.table-wrap {
  overflow-x: auto;
}

.result-table {
  width: max-content;
  min-width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}

.result-table th,
.result-table td {
  padding: 12px 14px;
  text-align: left;
  white-space: nowrap;
  border: 1px solid #d1d7e0;
  color: var(--ink);
}

.result-table th {
  background: #e6ebf2;
  color: var(--navy-900);
  font-weight: 700;
}

.result-table tbody tr:nth-child(even) td {
  background: #fafbfd;
}

.error-text {
  color: #8b1e32;
  font-weight: 700;
}

.input-shell {
  background: #fff;
  border: 1px solid var(--line);
  border-top: none;
  padding: 22px 32px 30px;
}

.input-frame {
  display: flex;
  align-items: end;
  gap: 20px;
  padding-top: 18px;
  border-top: 2px solid var(--navy-900);
}

.input-frame.loading {
  border-top-color: var(--gold);
}

.input-main {
  flex: 1;
  min-width: 0;
}

.input-label {
  display: block;
  margin-bottom: 10px;
  color: var(--navy-900);
  font-size: 13px;
  font-weight: 700;
}

.input-main input {
  width: 100%;
  min-height: 54px;
  padding: 0 16px;
  border: 1px solid var(--line-strong);
  border-radius: 0;
  background: #fff;
  color: var(--ink);
}

.input-main input:focus {
  outline: none;
  border-color: var(--navy-700);
}

.input-actions {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 10px;
}

.input-hint {
  color: var(--ink);
  font-size: 12px;
}

.input-actions button {
  min-width: 132px;
  min-height: 54px;
  padding: 0 22px;
  border: 1px solid var(--navy-900);
  border-radius: 0;
  background: var(--navy-900);
  color: #fff;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.input-actions button:hover:not(:disabled) {
  background: var(--navy-800);
}

.input-actions button:disabled {
  opacity: 0.72;
  cursor: not-allowed;
}

.messages-bottom-spacer {
  height: 8px;
}

@media (max-width: 1080px) {
  .masthead {
    grid-template-columns: 96px minmax(0, 1fr);
  }

  .board-heading {
    flex-direction: column;
    align-items: start;
  }

  .section-note {
    max-width: none;
    text-align: left;
  }
}

@media (max-width: 820px) {
  .site-header,
  .board-shell {
    padding-left: 16px;
    padding-right: 16px;
  }

  .chat-page {
    background:
      linear-gradient(180deg, var(--navy-900) 0 424px, transparent 424px),
      var(--paper);
  }

  .masthead {
    grid-template-columns: 1fr;
    gap: 18px;
    padding-top: 24px;
  }

  .seal {
    width: 74px;
    height: 74px;
    font-size: 22px;
  }

  .masthead-copy h1 {
    font-size: clamp(2.1rem, 11vw, 3rem);
  }

  .empty-grid {
    grid-template-columns: 1fr;
  }

  .messages {
    height: calc(100vh - 712px);
    padding: 20px 18px;
  }

  .board-heading,
  .input-shell {
    padding-left: 18px;
    padding-right: 18px;
  }

  .input-frame {
    flex-direction: column;
    align-items: stretch;
  }

  .input-actions {
    align-items: stretch;
  }

  .input-actions button {
    width: 100%;
  }
}

@media (max-width: 560px) {
  .utility-bar {
    gap: 10px;
    font-size: 11px;
  }

  .messages {
    height: calc(100vh - 772px);
  }

  .message-row {
    grid-template-columns: 1fr;
    gap: 10px;
  }

  .message-marker {
    width: 36px;
    height: 36px;
  }
}
</style>
