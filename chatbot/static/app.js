/**
 * SoundFlow AI Chatbot - Frontend Application
 *
 * SSE client connecting to /chat/stream, with typing effect,
 * tool call indicators, and sessionStorage conversation persistence.
 */

// ===================== State =====================
const state = {
    conversationId: sessionStorage.getItem("sf_conversation_id") || null,
    userId: "1", // Default user ID
    isStreaming: false,
};

// ===================== DOM Elements =====================
const messagesContainer = document.getElementById("chat-messages");
const welcomeContainer = document.getElementById("welcome-container");
const messageInput = document.getElementById("message-input");
const sendBtn = document.getElementById("send-btn");
const newChatBtn = document.getElementById("new-chat-btn");

// ===================== Allowed Tool Names (whitelist) =====================
const TOOL_LABELS = Object.freeze({
    recommend_songs: "🎧 Đang tìm gợi ý bài hát...",
    recommend_personalized: "🎧 Đang tìm gợi ý cho bạn...",
    recommend_by_mood: "🎭 Đang phân tích cảm xúc...",
    get_user_stats: "📊 Đang lấy thống kê nghe nhạc...",
    get_user_listening_history: "🕒 Đang lấy lịch sử nghe nhạc...",
    change_subscription: "💳 Đang xử lý subscription...",
    search_songs: "🔍 Đang tìm kiếm bài hát...",
    search_artists: "🔍 Đang tìm kiếm nghệ sĩ...",
    search_songs_by_artist: "🔍 Đang tìm bài hát của nghệ sĩ...",
    get_playlist: "📋 Đang lấy danh sách playlist...",
    create_playlist: "📝 Đang tạo playlist...",
    delete_playlist: "🗑️ Đang xóa playlist...",
    update_playlist: "📝 Đang cập nhật playlist...",
    remove_song_from_playlist: "🗑️ Đang xóa bài hát khỏi playlist...",
});

function normalizeDisplayText(text) {
    if (!text) return "";

    let out = String(text);

    // Convert escaped newlines from JSON-like payloads.
    out = out.replace(/\\n/g, "\n");

    // Convert markdown-ish emphasis markers to plain text for this UI.
    out = out.replace(/\*\*(.*?)\*\*/g, "$1");

    // Split common one-line list patterns.
    out = out.replace(/\s+(?=\d+\.\s)/g, "\n\n");
    out = out.replace(/\s+\*\s+/g, "\n- ");
    out = out.replace(/:\s*(?=(\d+\.\s|-\s))/g, ":\n\n");

    // Normalize real bullet lines only (do not split song - artist patterns).
    out = out.replace(/(^|\n)\s*-\s+/g, "$1- ");

    // Keep CTA prompt on a separate paragraph.
    out = out.replace(/\s+(?=(Bạn muốn|Ban muon|Bạn cần|Bạn có muốn)\b)/g, "\n\n");

    // Clean up spacing.
    out = out.replace(/\n{3,}/g, "\n\n").trim();
    return out;
}

function finalizeListFormatting(text) {
    if (!text) return "";

    let out = String(text);
    const numberMatches = out.match(/\d+\.\s/g) || [];

    // Force multiline when we detect compact numbered lists.
    if (numberMatches.length >= 2) {
        const parts = out.split(/(?=\d+\.\s)/g).map((p) => p.trim()).filter(Boolean);
        if (parts.length >= 2) {
            const formatted = parts.map((part) => {
                let item = part;

                // Keep song and artist on one line when model outputs "song - artist".
                item = item.replace(/\n\s*-\s*/g, " - ");

                // Move metric blocks to indented new lines.
                item = item.replace(/\s+(▶️|👤|💎|⏰|🎤|🎵|📅)/g, "\n   $1");

                // Remove trailing separators like " |" to reduce visual noise.
                item = item.replace(/\s*\|\s*$/gm, "");
                return item.trim();
            });
            out = formatted.join("\n\n");
        }
    }

    // Put closing prompts on a separate paragraph.
    out = out.replace(/\s+(?=(Bạn có muốn|Hay bạn muốn|Bạn muốn)\b)/g, "\n\n");
    return out;
}

// ===================== Event Listeners =====================
sendBtn.addEventListener("click", () => sendMessage());

messageInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});

messageInput.addEventListener("input", () => {
    // Auto-resize textarea
    messageInput.style.height = "auto";
    messageInput.style.height = Math.min(messageInput.scrollHeight, 150) + "px";
    // Toggle send button
    sendBtn.disabled = !messageInput.value.trim() || state.isStreaming;
});

newChatBtn.addEventListener("click", () => {
    // Clear all children safely
    while (messagesContainer.firstChild) {
        messagesContainer.removeChild(messagesContainer.firstChild);
    }
    state.conversationId = null;
    sessionStorage.removeItem("sf_conversation_id");
    messagesContainer.appendChild(createWelcome());
    messageInput.focus();
});

// Suggestion chips
document.querySelectorAll(".chip").forEach((chip) => {
    chip.addEventListener("click", () => {
        const message = chip.dataset.message;
        if (message) {
            messageInput.value = message;
            sendMessage();
        }
    });
});

// ===================== Core Functions =====================

function sendMessage() {
    const text = messageInput.value.trim();
    if (!text || state.isStreaming) return;

    // Hide welcome
    const welcome = document.getElementById("welcome-container");
    if (welcome) {
        welcome.remove();
    }

    // Add user message bubble
    addMessage("user", text);

    // Clear input
    messageInput.value = "";
    messageInput.style.height = "auto";
    sendBtn.disabled = true;

    // Start streaming
    streamResponse(text);
}

/**
 * Safely set text content in a message bubble.
 * For user messages: uses textContent (no HTML).
 * For assistant messages: uses sanitized HTML for markdown formatting.
 */
function addMessage(role, content) {
    const msg = document.createElement("div");
    msg.className = `message ${role}`;

    const avatar = document.createElement("div");
    avatar.className = "message-avatar";
    avatar.textContent = role === "user" ? "👤" : "🎵";

    const contentDiv = document.createElement("div");
    contentDiv.className = "message-content";

    if (role === "user") {
        // User messages: plain text only — no HTML injection possible
        contentDiv.textContent = content;
    } else {
        // Assistant messages: sanitize then apply safe formatting
        setSanitizedContent(contentDiv, content);
    }

    msg.appendChild(avatar);
    msg.appendChild(contentDiv);
    messagesContainer.appendChild(msg);
    scrollToBottom();

    return contentDiv;
}

/**
 * Safely render formatted text into an element using textContent only.
 * No innerHTML is used — fully XSS-safe.
 */
function setSanitizedContent(element, text) {
    const normalized = normalizeDisplayText(text);

    if (!normalized) {
        element.textContent = "";
        return;
    }

    // Clear existing nodes safely.
    while (element.firstChild) {
        element.removeChild(element.firstChild);
    }

    // Preserve line breaks without using innerHTML.
    const lines = String(normalized).split("\n");
    lines.forEach((line, idx) => {
        element.appendChild(document.createTextNode(line));
        if (idx < lines.length - 1) {
            element.appendChild(document.createElement("br"));
        }
    });
}

function addTypingIndicator() {
    const msg = document.createElement("div");
    msg.className = "message assistant";
    msg.id = "typing-indicator";

    const avatar = document.createElement("div");
    avatar.className = "message-avatar";
    avatar.textContent = "🎵";

    const typing = document.createElement("div");
    typing.className = "typing-indicator";

    // Build typing dots using safe DOM API
    for (let i = 0; i < 3; i++) {
        const dot = document.createElement("div");
        dot.className = "typing-dot";
        typing.appendChild(dot);
    }

    msg.appendChild(avatar);
    msg.appendChild(typing);
    messagesContainer.appendChild(msg);
    scrollToBottom();
}

function removeTypingIndicator() {
    const indicator = document.getElementById("typing-indicator");
    if (indicator) indicator.remove();
}

/**
 * Creates a tool indicator using safe DOM APIs (no innerHTML).
 * Tool name is validated against a whitelist.
 */
function addToolIndicator(toolName) {
    const toolDiv = document.createElement("div");
    toolDiv.className = "tool-indicator";
    toolDiv.id = `tool-${Date.now()}`;

    const spinner = document.createElement("div");
    spinner.className = "spinner";

    const label = document.createElement("span");
    // Use whitelisted label or sanitize the tool name
    const safeLabel = TOOL_LABELS[toolName];
    if (safeLabel) {
        label.textContent = safeLabel;
    } else {
        // Sanitize unknown tool names — textContent prevents XSS
        label.textContent = `🔧 Đang gọi ${toolName}...`;
    }

    toolDiv.appendChild(spinner);
    toolDiv.appendChild(label);
    messagesContainer.appendChild(toolDiv);
    scrollToBottom();
    return toolDiv;
}

// ===================== SSE Streaming =====================

async function streamResponse(message) {
    state.isStreaming = true;
    addTypingIndicator();

    const params = new URLSearchParams({
        message: message,
        user_id: state.userId,
    });
    if (state.conversationId) {
        params.set("conversation_id", state.conversationId);
    }

    const url = `/chat/stream?${params.toString()}`;
    let assistantContentDiv = null;
    let collectedText = "";
    let currentToolDiv = null;

    try {
        const response = await fetch(url, {
            headers: { Accept: "text/event-stream" },
        });

        if (!response.ok) {
            throw new Error(`Server error: ${response.status}`);
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";

            for (const line of lines) {
                if (!line.startsWith("data:")) continue;
                const dataStr = line.slice(5).trim();
                if (!dataStr || dataStr === "[DONE]") continue;

                try {
                    const event = JSON.parse(dataStr);
                    handleSSEEvent(event);
                } catch {
                    // Skip malformed JSON
                }
            }
        }
    } catch (error) {
        removeTypingIndicator();
        if (!assistantContentDiv && !collectedText) {
            // Use textContent for error to prevent XSS from error.message
            const errorDiv = addMessage("assistant", "");
            errorDiv.textContent = `❌ Lỗi kết nối: ${error.message}`;
        }
    } finally {
        state.isStreaming = false;
        sendBtn.disabled = !messageInput.value.trim();
    }

    // --- Inner event handler ---
    function handleSSEEvent(event) {
        switch (event.type) {
            case "token":
                removeTypingIndicator();
                if (!assistantContentDiv) {
                    assistantContentDiv = addMessage("assistant", "");
                }
                // Complete any pending tool indicator
                if (currentToolDiv) {
                    currentToolDiv.classList.add("complete");
                    currentToolDiv = null;
                }
                collectedText += event.content;
                // Use safe sanitized rendering
                setSanitizedContent(assistantContentDiv, collectedText);
                scrollToBottom();
                break;

            case "tool_call":
                removeTypingIndicator();
                currentToolDiv = addToolIndicator(event.name);
                break;

            case "tool_result":
                if (currentToolDiv) {
                    currentToolDiv.classList.add("complete");
                    currentToolDiv = null;
                }
                break;

            case "done":
                removeTypingIndicator();
                if (assistantContentDiv && collectedText) {
                    setSanitizedContent(
                        assistantContentDiv,
                        finalizeListFormatting(collectedText)
                    );
                }
                if (event.conversation_id) {
                    state.conversationId = event.conversation_id;
                    sessionStorage.setItem(
                        "sf_conversation_id",
                        event.conversation_id
                    );
                }
                break;

            case "error":
                removeTypingIndicator();
                if (!assistantContentDiv) {
                    // Safely display error message via textContent
                    const errorDiv = addMessage("assistant", "");
                    errorDiv.textContent = `❌ ${event.message}`;
                }
                break;
        }
    }
}

// ===================== Utilities =====================

function scrollToBottom() {
    requestAnimationFrame(() => {
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    });
}

function createWelcome() {
    const div = document.createElement("div");
    div.className = "welcome-container";
    div.id = "welcome-container";

    const icon = document.createElement("div");
    icon.className = "welcome-icon";
    icon.textContent = "🎶";

    const title = document.createElement("h2");
    title.className = "welcome-title";
    title.textContent = "Chào mừng bạn đến SoundFlow!";

    const subtitle = document.createElement("p");
    subtitle.className = "welcome-subtitle";
    subtitle.textContent =
        "Tôi có thể giúp bạn tìm bài hát, tạo playlist, xem thống kê nghe nhạc, và nhiều hơn nữa.";

    const chips = document.createElement("div");
    chips.className = "suggestion-chips";

    const chipData = [
        { emoji: "🎧", label: "Gợi ý bài hát", msg: "Gợi ý bài hát hay cho tôi" },
        { emoji: "📊", label: "Thống kê nghe nhạc", msg: "Xem thống kê nghe nhạc của tôi" },
        { emoji: "📝", label: "Tạo playlist", msg: "Tạo playlist nhạc chill cho tôi" },
        { emoji: "🔥", label: "Bài hát hot", msg: "Top bài hát đang hot tại Việt Nam" },
    ];

    chipData.forEach(({ emoji, label, msg }) => {
        const btn = document.createElement("button");
        btn.className = "chip";
        btn.textContent = `${emoji} ${label}`;
        btn.addEventListener("click", () => {
            messageInput.value = msg;
            sendMessage();
        });
        chips.appendChild(btn);
    });

    div.appendChild(icon);
    div.appendChild(title);
    div.appendChild(subtitle);
    div.appendChild(chips);
    return div;
}
