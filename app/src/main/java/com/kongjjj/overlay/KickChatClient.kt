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
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
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
    val category: String? = null,
    val pusherWsUrl: String? = null
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
                        
                        val wsUrl = info.pusherWsUrl
                            ?: fetchDynamicPusherUrl(info.chatroomChannelId)
                            ?: "wss://ws-us2.pusher.com/app/34bf7a0ff419a2a775b9?protocol=7&client=js&version=8.4.0-picker&flash=false"
                        
                        openWebSocket(wsUrl, info.chatroomId, info.chatroomChannelId)
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
            Log.d(tag, "Resolving Kick channel info for slug: $slug")
            val request = Request.Builder()
                .url("https://kick.com/api/v1/channels/${slug.lowercase().trim()}")
                .addHeader(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                )
                .addHeader("Accept", "application/json")
                .build()

            http.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.e(tag, "Kick channel API failed with HTTP code: ${response.code}")
                    return@use null
                }
                val body = response.body.string()
                if (body.isBlank()) return@use null

                val root = JSONObject(body)
                val userId = root.opt("user_id")?.toString()?.takeIf { it != "null" && it.isNotBlank() }
                val chatroomObj = root.optJSONObject("chatroom")
                val chatroomId = chatroomObj?.opt("id")?.toString()?.takeIf { it != "null" && it.isNotBlank() }
                val chatroomChannelId = chatroomObj?.opt("channel_id")?.toString()?.takeIf { it != "null" && it.isNotBlank() }

                if (chatroomId == null || chatroomChannelId == null) {
                    Log.e(tag, "Could not extract chatroomId or chatroomChannelId for $slug")
                    return@use null
                }

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

                Log.d(tag, "Resolved Kick $slug -> chatroomId=$chatroomId, chatroomChannelId=$chatroomChannelId, userId=$userId, viewers=$viewers")

                val dynamicWsUrl = fetchDynamicPusherUrl(chatroomChannelId)

                KickChannelInfo(
                    chatroomId = chatroomId,
                    chatroomChannelId = chatroomChannelId,
                    userId = userId,
                    viewersCount = viewers,
                    title = title,
                    category = category,
                    pusherWsUrl = dynamicWsUrl
                )
            }
        } catch (e: Exception) {
            Log.e(tag, "Error fetching Kick channel API for $slug", e)
            null
        }
    }

    private fun fetchDynamicPusherUrl(channelId: String): String? {
        return try {
            val body = """{"client":{"id":"${UUID.randomUUID()}","type":"web"},"capabilities":{"accepted_providers":[{"provider":"pusher"}]}}"""
                .toRequestBody("application/json".toMediaType())
            val req = Request.Builder()
                .url("https://web.kick.com/api/v1/realtime/channels/$channelId/chat/connection")
                .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
                .header("Accept", "application/json")
                .header("Referer", "https://kick.com/")
                .post(body)
                .build()

            http.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return null
                val raw = resp.body.string()
                val root = JSONObject(raw)
                val dataObj = root.optJSONObject("data")
                val conns = dataObj?.optJSONArray("connections")
                if (conns != null && conns.length() > 0) {
                    val conn = conns.getJSONObject(0)
                    val creds = conn.optJSONObject("credentials")
                    val appKey = creds?.optString("app_key")
                    val cluster = creds?.optString("cluster", "us2")
                    if (!appKey.isNullOrBlank()) {
                        Log.d(tag, "Fetched dynamic Pusher credentials: key=$appKey, cluster=$cluster")
                        return "wss://ws-$cluster.pusher.com/app/$appKey?protocol=7&client=js&version=8.4.0-picker&flash=false"
                    }
                }
                null
            }
        } catch (e: Exception) {
            Log.e(tag, "Failed to fetch dynamic Pusher credentials", e)
            null
        }
    }

    private fun openWebSocket(wsUrl: String, roomId: String, channelId: String) {
        Log.d(tag, "Connecting to Kick Pusher WS: $wsUrl")
        val req = Request.Builder().url(wsUrl).build()

        fun sendSubscriptions(ws: WebSocket) {
            listOf(
                "chatrooms.$roomId.v2",
                "chatroom_$roomId",
                "chatrooms.$roomId",
                "channel_$channelId"
            ).forEach { ch ->
                ws.send("""{"event":"pusher:subscribe","data":{"auth":"","channel":"$ch"}}""")
            }
        }

        webSocket = http.newWebSocket(req, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                Log.d(tag, "Kick Pusher WebSocket connected")
                _connected.value = true
                sendSubscriptions(webSocket)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                handleFrame(webSocket, text, roomId, channelId)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                Log.d(tag, "Kick WebSocket closed: $code / $reason")
                _connected.value = false
                scheduleReconnect()
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                Log.e(tag, "Kick WebSocket failure", t)
                _connected.value = false
                scheduleReconnect()
            }
        })
    }

    private fun handleFrame(ws: WebSocket, raw: String, roomId: String, channelId: String) {
        try {
            val root = JSONObject(raw)
            val event = root.optString("event")

            if (event == "pusher:ping") {
                ws.send("""{"event":"pusher:pong","data":{}}""")
                return
            }

            if (event == "pusher:connection_established") {
                listOf(
                    "chatrooms.$roomId.v2",
                    "chatroom_$roomId",
                    "chatrooms.$roomId",
                    "channel_$channelId"
                ).forEach { ch ->
                    ws.send("""{"event":"pusher:subscribe","data":{"auth":"","channel":"$ch"}}""")
                }
                return
            }

            if (event.contains("ChatMessage")) {
                val dataObj: JSONObject? = when {
                    root.optJSONObject("data") != null -> root.optJSONObject("data")
                    root.optString("data").isNotBlank() -> runCatching { JSONObject(root.getString("data")) }.getOrNull()
                    else -> null
                }

                if (dataObj != null) {
                    val msgId = dataObj.optString("id").ifBlank { UUID.randomUUID().toString() }
                    val content = dataObj.optString("content").ifBlank { dataObj.optString("message") }
                    val sender = dataObj.optJSONObject("sender")
                    val username = sender?.optString("username")?.ifBlank { null }
                        ?: sender?.optString("slug")
                        ?: "Unknown"
                    val identity = sender?.optJSONObject("identity")
                    val colorHex = identity?.optString("color")?.takeIf { it.isNotBlank() }

                    val badgeTags = mutableListOf<String>()

                    // Check badges_v2 first (contains direct image_url)
                    val badgesV2Arr = identity?.optJSONArray("badges_v2")
                    if (badgesV2Arr != null && badgesV2Arr.length() > 0) {
                        for (i in 0 until badgesV2Arr.length()) {
                            val b = badgesV2Arr.optJSONObject(i) ?: continue
                            val imgUrl = b.optString("image_url")
                            if (imgUrl.isNotBlank()) {
                                badgeTags.add(imgUrl)
                            }
                        }
                    }

                    // Fallback to badges array if badges_v2 is missing or empty
                    if (badgeTags.isEmpty()) {
                        val badgesArr = identity?.optJSONArray("badges")
                        if (badgesArr != null) {
                            for (i in 0 until badgesArr.length()) {
                                val b = badgesArr.optJSONObject(i) ?: continue
                                val type = b.optString("type")
                                val active = b.optBoolean("active", true)
                                if (active) {
                                    val badgeObj = b.optJSONObject("badge")
                                    val imageUrl = badgeObj?.optString("url")?.takeIf { it.isNotBlank() }
                                        ?: badgeObj?.optString("image")?.takeIf { it.isNotBlank() }
                                    if (imageUrl != null) {
                                        badgeTags.add(imageUrl)
                                    } else if (type.isNotBlank()) {
                                        badgeTags.add("kick-$type")
                                    }
                                }
                            }
                        }
                    }

                    val msg = ChatMessage(
                        id = msgId,
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
            } else if (event.contains("Subscription") || event.contains("GiftedSubscriptions")) {
                val dataObj: JSONObject? = when {
                    root.optJSONObject("data") != null -> root.optJSONObject("data")
                    root.optString("data").isNotBlank() -> runCatching { JSONObject(root.getString("data")) }.getOrNull()
                    else -> null
                }

                if (dataObj != null) {
                    val username = dataObj.optString("username", dataObj.optString("gifter_username", "KickUser"))
                    val months = dataObj.optInt("months", 1)
                    val giftCount = dataObj.optInt("gift_count", 1)
                    val noticeText = if (event.contains("GiftedSubscriptions")) {
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
