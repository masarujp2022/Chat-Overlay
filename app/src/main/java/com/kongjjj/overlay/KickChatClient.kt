package com.kongjjj.overlay

import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

data class KickChannelInfo(
    val chatroomId: String,
    val chatroomChannelId: String,
    val userId: String?,
    val viewersCount: Int = 0,
    val title: String? = null,
    val category: String? = null
)

class KickChatClient {
    private val tag = "KickChatClient"
    private val http = OkHttpClient()
    private val scope = CoroutineScope(Dispatchers.IO)
    private var webSocket: WebSocket? = null
    private var connectionJob: Job? = null

    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    val messages: StateFlow<List<ChatMessage>> = _messages

    private val _newMessages = MutableSharedFlow<ChatMessage>()
    val newMessages: SharedFlow<ChatMessage> = _newMessages

    private val _connected = MutableStateFlow(false)
    val connected: StateFlow<Boolean> = _connected

    private var currentChannel: String? = null
    private var chatroomId: String? = null
    private var chatroomChannelId: String? = null
    var userId: String? = null
        private set

    var onReconnect: (() -> Unit)? = null

    fun connect(channel: String) {
        val normalized = channel.trim().lowercase().removePrefix("/")
        if (normalized.isBlank()) {
            disconnect()
            return
        }
        if (normalized == currentChannel && _connected.value) return

        disconnectInternal()
        currentChannel = normalized
        _messages.value = emptyList()

        connectionJob = scope.launch {
            while (isActive) {
                try {
                    val info = resolveChannelInfo(normalized)
                    if (info != null) {
                        chatroomId = info.chatroomId
                        chatroomChannelId = info.chatroomChannelId
                        userId = info.userId
                        openWebSocket(info.chatroomId, info.chatroomChannelId)
                        break
                    } else {
                        Log.e(tag, "Failed to resolve Kick channel info for $normalized")
                    }
                } catch (e: Exception) {
                    Log.e(tag, "Error connecting Kick chat for $normalized", e)
                }

                if (isActive) {
                    delay(10.seconds)
                }
            }
        }
    }

    suspend fun resolveChannelInfo(slug: String): KickChannelInfo? = withContext(Dispatchers.IO) {
        if (slug.isBlank()) return@withContext null
        try {
            val request = Request.Builder()
                .url("https://kick.com/api/v1/channels/${slug.lowercase().trim()}")
                .addHeader(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                )
                .addHeader("Accept", "application/json")
                .build()

            http.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return@use null
                val body = response.body?.string() ?: return@use null
                if (body.isBlank()) return@use null

                val root = JSONObject(body)
                val userId = root.optLong("user_id", 0L).takeIf { it > 0 }?.toString()
                    ?: root.optString("user_id").takeIf { it.isNotBlank() }
                val chatroomObj = root.optJSONObject("chatroom")
                val chatroomId = chatroomObj?.optLong("id", 0L)?.takeIf { it > 0 }?.toString()
                    ?: chatroomObj?.optString("id")?.takeIf { it.isNotBlank() }
                val chatroomChannelId = chatroomObj?.optLong("channel_id", 0L)?.takeIf { it > 0 }?.toString()
                    ?: chatroomObj?.optString("channel_id")?.takeIf { it.isNotBlank() }

                if (chatroomId == null || chatroomChannelId == null) return@use null

                val livestreamObj = root.optJSONObject("livestream")
                var viewers = 0
                var title: String? = null
                var category: String? = null

                if (livestreamObj != null && livestreamObj.optBoolean("is_live", false)) {
                    viewers = livestreamObj.optInt("viewer_count", 0)
                    title = livestreamObj.optString("session_title").ifBlank { null }
                    val categoriesArr = livestreamObj.optJSONArray("categories")
                    if (categoriesArr != null && categoriesArr.length() > 0) {
                        category = categoriesArr.getJSONObject(0).optString("name").ifBlank { null }
                    } else {
                        val catObj = livestreamObj.optJSONObject("category")
                        category = catObj?.optString("name")?.ifBlank { null }
                    }
                }

                KickChannelInfo(
                    chatroomId = chatroomId,
                    chatroomChannelId = chatroomChannelId,
                    userId = userId,
                    viewersCount = viewers,
                    title = title,
                    category = category
                )
            }
        } catch (e: Exception) {
            Log.e(tag, "Error fetching Kick channel API for $slug", e)
            null
        }
    }

    private fun openWebSocket(roomId: String, channelId: String) {
        val url = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679?protocol=7&client=js&version=8.4.0-picker&flash=false"
        val req = Request.Builder().url(url).build()

        webSocket = http.newWebSocket(req, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                _connected.value = true
                listOf(
                    "chatrooms.$roomId.v2",
                    "chatroom_$roomId",
                    "chatrooms.$roomId",
                    "channel_$channelId"
                ).forEach { ch ->
                    ws.send("""{"event":"pusher:subscribe","data":{"auth":"","channel":"$ch"}}""")
                }
            }

            override fun onMessage(ws: WebSocket, text: String) {
                handleFrame(ws, text)
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                _connected.value = false
                scheduleReconnect()
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                _connected.value = false
                scheduleReconnect()
            }
        })
    }

    private fun handleFrame(ws: WebSocket, raw: String) {
        try {
            val root = JSONObject(raw)
            val event = root.optString("event")

            if (event == "pusher:ping") {
                ws.send("""{"event":"pusher:pong","data":{}}""")
                return
            }

            if (event == "App\\Events\\ChatMessageEvent") {
                val dataStr = root.optString("data")
                if (dataStr.isNotBlank()) {
                    val dataObj = JSONObject(dataStr)
                    val msgId = dataObj.optString("id")
                    val content = dataObj.optString("content")
                    val sender = dataObj.optJSONObject("sender")
                    val username = sender?.optString("username") ?: "Unknown"
                    val identity = sender?.optJSONObject("identity")
                    val colorHex = identity?.optString("color")?.takeIf { it.isNotBlank() }

                    val badgesArr = identity?.optJSONArray("badges")
                    val badgeTags = mutableListOf<String>()
                    if (badgesArr != null) {
                        for (i in 0 until badgesArr.length()) {
                            val b = badgesArr.getJSONObject(i)
                            val type = b.optString("type")
                            val active = b.optBoolean("active", true)
                            if (active) {
                                val badgeObj = b.optJSONObject("badge")
                                val imageUrl = badgeObj?.optString("url") ?: badgeObj?.optString("image")
                                if (!imageUrl.isNullOrBlank()) {
                                    badgeTags.add(imageUrl)
                                } else if (type.isNotBlank()) {
                                    badgeTags.add("kick-$type")
                                }
                            }
                        }
                    }

                    val msg = ChatMessage(
                        id = msgId.ifBlank { UUID.randomUUID().toString() },
                        username = username,
                        message = content,
                        color = colorHex,
                        badgeTags = badgeTags,
                        timestamp = System.currentTimeMillis(),
                        platform = "kick"
                    )
                    _messages.value = (_messages.value + msg).takeLast(MAX_CHAT_MESSAGES)
                    scope.launch { _newMessages.emit(msg) }
                }
            } else if (event == "App\\Events\\SubscriptionEvent" || event == "App\\Events\\GiftedSubscriptionsEvent") {
                val dataStr = root.optString("data")
                if (dataStr.isNotBlank()) {
                    val dataObj = JSONObject(dataStr)
                    val username = dataObj.optString("username", dataObj.optString("gifter_username", "KickUser"))
                    val months = dataObj.optInt("months", 1)
                    val giftCount = dataObj.optInt("gift_count", 1)
                    val noticeText = if (event == "App\\Events\\GiftedSubscriptionsEvent") {
                        "✦【$username 贈送了 $giftCount 個 Kick 訂閱】"
                    } else {
                        "✦【$username 訂閱了 Kick 頻道（$months 個月）】"
                    }
                    val msg = ChatMessage(
                        id = UUID.randomUUID().toString(),
                        username = username,
                        message = noticeText,
                        rawSystemMessage = noticeText,
                        timestamp = System.currentTimeMillis(),
                        platform = "kick",
                        isAnnouncement = true
                    )
                    _messages.value = (_messages.value + msg).takeLast(MAX_CHAT_MESSAGES)
                    scope.launch { _newMessages.emit(msg) }
                }
            }
        } catch (e: Exception) {
            Log.e(tag, "Error parsing Kick Pusher frame", e)
        }
    }

    private fun scheduleReconnect() {
        connectionJob?.cancel()
        connectionJob = scope.launch {
            delay(5.seconds)
            currentChannel?.let {
                onReconnect?.invoke()
                connect(it)
            }
        }
    }

    fun disconnect() {
        connectionJob?.cancel()
        connectionJob = null
        disconnectInternal()
        currentChannel = null
        chatroomId = null
        chatroomChannelId = null
        userId = null
    }

    private fun disconnectInternal() {
        webSocket?.close(1000, null)
        webSocket = null
        _connected.value = false
    }

    fun clearMessages() {
        _messages.value = emptyList()
        currentChannel = null
        chatroomId = null
        chatroomChannelId = null
        userId = null
    }
}
