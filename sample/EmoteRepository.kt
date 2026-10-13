package com.slachy.studiobridge

import com.google.gson.JsonArray
import com.google.gson.JsonParser
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request

class EmoteRepository {

    // Single shared client with a browser-like UA so APIs don't reject us
    private val http = OkHttpClient.Builder()
        .addInterceptor { chain ->
            chain.proceed(
                chain.request().newBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124.0 Mobile Safari/537.36")
                    .header("Accept", "application/json")
                    .build()
            )
        }
        .build()

    private val _thirdPartyEmotes = MutableStateFlow<Map<String, String>>(emptyMap())
    val thirdPartyEmotes: StateFlow<Map<String, String>> = _thirdPartyEmotes

    private val _twitchBadges = MutableStateFlow<Map<String, String>>(emptyMap())
    val twitchBadges: StateFlow<Map<String, String>> = _twitchBadges

    private val _loadReport = MutableStateFlow("loading…")
    val loadReport: StateFlow<String> = _loadReport

    /**
     * Loads emote providers and global badges sequentially on the IO dispatcher.
     * Clears existing third-party emotes first so toggling a provider off takes effect.
     * loadReport is updated with per-source counts, "off", or exception class names.
     */
    suspend fun loadAll(
        enable7tv: Boolean = true,
        enableBttv: Boolean = true,
        enableFfz: Boolean = true
    ) = withContext(Dispatchers.IO) {
        _thirdPartyEmotes.value = emptyMap()

        fun loadProvider(enabled: Boolean, fetch: () -> Map<String, String>): String {
            if (!enabled) return "off"
            return runCatching { fetch() }
                .onSuccess { map -> _thirdPartyEmotes.update { current -> current + map } }
                .fold({ it.size.toString() }, { it.javaClass.simpleName })
        }

        val s7tv  = loadProvider(enable7tv,  ::fetch7TV)
        val sBttv = loadProvider(enableBttv, ::fetchBTTV)
        val sFfz  = loadProvider(enableFfz,  ::fetchFFZ)

        val sBdg = runCatching { fetchTwitchBadgesGlobalIvr() }
            .onSuccess { badges -> _twitchBadges.value = badges }
            .fold({ it.size.toString() }, { it.javaClass.simpleName })

        _loadReport.value = "7tv:$s7tv bttv:$sBttv ffz:$sFfz bdg:$sBdg"
    }

    /**
     * Fetches channel-specific emotes and badges for a joined channel and merges them.
     */
    suspend fun loadChannelEmotes(
        channelId: String,
        channelName: String,
        enable7tv: Boolean = true,
        enableBttv: Boolean = true,
        enableFfz: Boolean = true
    ) = withContext(Dispatchers.IO) {
        fun mergeProvider(enabled: Boolean, fetch: () -> Map<String, String>): String {
            if (!enabled) return "off"
            return runCatching { fetch() }
                .onSuccess { map -> _thirdPartyEmotes.update { current -> current + map } }
                .fold({ it.size.toString() }, { it.javaClass.simpleName })
        }

        val s7tv  = mergeProvider(enable7tv)  { fetch7TVChannel(channelId) }
        val sBttv = mergeProvider(enableBttv) { fetchBTTVChannel(channelId) }
        val sFfz  = mergeProvider(enableFfz)  { fetchFFZChannel(channelName) }

        // Load channel badges from IVR
        runCatching { fetchTwitchBadgesChannelIvr(channelId) }
            .onSuccess { badges -> _twitchBadges.update { current -> current + badges } }

        val prev = _loadReport.value.substringBefore(" | ch:")
        _loadReport.value = "$prev | ch: 7tv:$s7tv bttv:$sBttv ffz:$sFfz"
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun get(url: String): String? = try {
        val req = Request.Builder().url(url).build()
        http.newCall(req).execute().use { resp ->
            if (resp.isSuccessful) resp.body?.string() else null
        }
    } catch (_: Exception) { null }

    // ── 7TV global emotes ─────────────────────────────────────────────────────
    // GET https://7tv.io/v3/emote-sets/global
    // Response: { emotes: [{ name, data: { animated, host: { url, files:[{name}] } } }] }
    // Prefer 1x.gif for animated emotes (our GifDecoder handles it);
    // fall back to 1x.webp for static emotes.

    private fun fetch7TV(): Map<String, String> {
        val body = get("https://7tv.io/v3/emote-sets/global") ?: return emptyMap()
        val root = JsonParser.parseString(body).takeIf { it.isJsonObject }?.asJsonObject
            ?: return emptyMap()
        val arr  = root.getAsJsonArray("emotes") ?: return emptyMap()
        return parse7tvEmoteArray(arr)
    }

    // ── BetterTTV global emotes ───────────────────────────────────────────────
    // GET https://api.betterttv.net/3/cached/emotes/global
    // Response: [{ id, code, imageType }]
    // Image URL: https://cdn.betterttv.net/emote/{id}/1x

    private fun fetchBTTV(): Map<String, String> {
        val body = get("https://api.betterttv.net/3/cached/emotes/global") ?: return emptyMap()
        val arr  = JsonParser.parseString(body).takeIf { it.isJsonArray }?.asJsonArray
            ?: return emptyMap()
        val map  = mutableMapOf<String, String>()
        arr.forEach { el ->
            runCatching {
                val obj  = el.asJsonObject
                val id   = obj.get("id").asString
                val code = obj.get("code").asString
                map[code] = "https://cdn.betterttv.net/emote/$id/1x"
            }
        }
        return map
    }

    // ── FrankerFaceZ global emotes ────────────────────────────────────────────
    // GET https://api.frankerfacez.com/v1/set/global
    // Response: { sets: { "3": { emoticons: [{ name, urls: { "1": "//cdn..." } }] } } }
    // Image URL: https:{urls["1"]}

    private fun fetchFFZ(): Map<String, String> {
        val body = get("https://api.frankerfacez.com/v1/set/global") ?: return emptyMap()
        val root = JsonParser.parseString(body).takeIf { it.isJsonObject }?.asJsonObject
            ?: return emptyMap()
        val sets = root.getAsJsonObject("sets") ?: return emptyMap()
        val map  = mutableMapOf<String, String>()
        sets.entrySet().forEach { (_, setEl) ->
            runCatching {
                val emoticons = setEl.asJsonObject.getAsJsonArray("emoticons") ?: return@runCatching
                emoticons.forEach { emEl ->
                    runCatching {
                        val emObj = emEl.asJsonObject
                        val name  = emObj.get("name").asString
                        val url1  = emObj.getAsJsonObject("urls").get("1")?.asString
                            ?: return@runCatching
                        map[name] = "https:$url1"
                    }
                }
            }
        }
        return map
    }

    // ── 7TV channel emotes ────────────────────────────────────────────────────
    // GET https://7tv.io/v3/users/twitch/{twitch_user_id}
    // Response: { emote_set: { emotes: [{ name, data: { host: { url, files } } }] } }

    private fun fetch7TVChannel(channelId: String): Map<String, String> {
        val body = get("https://7tv.io/v3/users/twitch/$channelId") ?: return emptyMap()
        val root = JsonParser.parseString(body).takeIf { it.isJsonObject }?.asJsonObject
            ?: return emptyMap()
        val arr  = root.getAsJsonObject("emote_set")?.getAsJsonArray("emotes") ?: return emptyMap()
        return parse7tvEmoteArray(arr)
    }

    // Shared parser for both global and channel 7TV emote arrays.
    // Prefers 1x.gif for animated emotes; falls back to 1x.webp for static ones.
    private fun parse7tvEmoteArray(arr: JsonArray): Map<String, String> {
        val map = mutableMapOf<String, String>()
        arr.forEach { el ->
            runCatching {
                val obj     = el.asJsonObject
                val name    = obj.get("name").asString
                val data    = obj.getAsJsonObject("data")
                val hostObj = data.getAsJsonObject("host")
                val hostUrl = hostObj.get("url").asString
                val files   = hostObj.getAsJsonArray("files")
                val hasGif  = files?.any { it.asJsonObject.get("name")?.asString == "1x.gif" } == true
                val ext     = if (hasGif) "1x.gif" else "1x.webp"
                map[name]   = "https:$hostUrl/$ext"
            }
        }
        return map
    }

    // ── BTTV channel emotes ───────────────────────────────────────────────────
    // GET https://api.betterttv.net/3/cached/users/twitch/{twitch_user_id}
    // Response: { channelEmotes: [{id, code}], sharedEmotes: [{id, code}] }

    private fun fetchBTTVChannel(channelId: String): Map<String, String> {
        val body = get("https://api.betterttv.net/3/cached/users/twitch/$channelId") ?: return emptyMap()
        val root = JsonParser.parseString(body).takeIf { it.isJsonObject }?.asJsonObject
            ?: return emptyMap()
        val map  = mutableMapOf<String, String>()
        listOf("channelEmotes", "sharedEmotes").forEach { key ->
            root.getAsJsonArray(key)?.forEach { el ->
                runCatching {
                    val obj  = el.asJsonObject
                    val id   = obj.get("id").asString
                    val code = obj.get("code").asString
                    map[code] = "https://cdn.betterttv.net/emote/$id/1x"
                }
            }
        }
        return map
    }

    // ── FFZ channel emotes ────────────────────────────────────────────────────
    // GET https://api.frankerfacez.com/v1/room/{channel_name}  (login name, no ID needed)
    // Response: { sets: { "N": { emoticons: [{ name, urls: { "1": "//cdn..." } }] } } }

    private fun fetchFFZChannel(channelName: String): Map<String, String> {
        val body = get("https://api.frankerfacez.com/v1/room/$channelName") ?: return emptyMap()
        val root = JsonParser.parseString(body).takeIf { it.isJsonObject }?.asJsonObject
            ?: return emptyMap()
        val sets = root.getAsJsonObject("sets") ?: return emptyMap()
        val map  = mutableMapOf<String, String>()
        sets.entrySet().forEach { (_, setEl) ->
            runCatching {
                val emoticons = setEl.asJsonObject.getAsJsonArray("emoticons") ?: return@runCatching
                emoticons.forEach { emEl ->
                    runCatching {
                        val emObj = emEl.asJsonObject
                        val name  = emObj.get("name").asString
                        val url1  = emObj.getAsJsonObject("urls").get("1")?.asString
                            ?: return@runCatching
                        map[name] = "https:$url1"
                    }
                }
            }
        }
        return map
    }

    // ── Twitch global & channel badges (via IVR) ─────────────────────────────
    // Primary: fetch from api.ivr.fi which groups global and channel sets nicely.
    // Flat map key: "set_id/version_id" e.g. "subscriber/0"

    private fun fetchTwitchBadgesGlobalIvr(): Map<String, String> {
        val body = get("https://api.ivr.fi/v2/twitch/badges/global") ?: return fetchTwitchBadgesLegacy()
        return parseIvrBadgeSets(body)
    }

    private fun fetchTwitchBadgesChannelIvr(channelId: String): Map<String, String> {
        val body = get("https://api.ivr.fi/v2/twitch/badges/channel?id=$channelId") ?: return emptyMap()
        return parseIvrBadgeSets(body)
    }

    private fun parseIvrBadgeSets(body: String): Map<String, String> {
        val map = mutableMapOf<String, String>()
        runCatching {
            val json = JsonParser.parseString(body)
            val badgeSets = if (json.isJsonArray) json.asJsonArray else {
                val arr = JsonArray()
                if (json.isJsonObject) arr.add(json)
                arr
            }

            badgeSets.forEach { setEl ->
                val setObj = setEl.asJsonObject
                val setId = setObj.get("set_id")?.asString ?: return@forEach
                val versions = setObj.getAsJsonArray("versions") ?: return@forEach
                versions.forEach { verEl ->
                    val verObj = verEl.asJsonObject
                    val verId = verObj.get("id")?.asString ?: return@forEach
                    val url = verObj.get("image_url_2x")?.asString 
                           ?: verObj.get("image_url_1x")?.asString ?: return@forEach
                    map["$setId/$verId"] = url
                }
            }
        }
        return map
    }

    private fun fetchTwitchBadgesLegacy(): Map<String, String> {
        val body = get("https://badges.twitch.tv/v1/badges/global/display?language=en")
        if (body != null) {
            runCatching {
                val root     = JsonParser.parseString(body).asJsonObject
                val sets     = root.getAsJsonObject("badge_sets")
                val map      = mutableMapOf<String, String>()
                sets.entrySet().forEach { (setName, setEl) ->
                    setEl.asJsonObject.getAsJsonObject("versions")
                        .entrySet().forEach { (version, verEl) ->
                            val url = verEl.asJsonObject.get("image_url_1x")?.asString
                            if (url != null) map["$setName/$version"] = url
                        }
                }
                if (map.isNotEmpty()) return map
            }
        }
        // Fallback to hardcoded UUIDs if all endpoints fail
        fun url(uuid: String) = "https://static-cdn.jtvnw.net/badges/v1/$uuid/1"
        return mapOf(
            "admin/1"       to url("9ef7e029-4cdf-4d4d-a0d5-e2b3fb2583fe"),
            "broadcaster/1" to url("5527c58c-fb7d-422d-b71b-f309dcb85cc1"),
            "global_mod/1"  to url("9384c9a7-f2f2-4f74-b6e5-7f78898a43fc"),
            "moderator/1"   to url("3267646d-33f0-4b17-b3df-f923a41db1d0"),
            "partner/1"     to url("d12a2e27-16f6-41d0-ab77-b780518f00a3"),
            "premium/1"     to url("bbbe0db0-a598-423e-86d0-f9fb98ca1933"),
            "staff/1"       to url("d97c37be-57b3-4f3d-b8db-6dcb904f84c8"),
            "subscriber/0"  to url("5d9f2208-5dd8-11e7-8513-2ff4adfae661"),
            "turbo/1"       to url("bd444ec6-8f34-4bf9-91f4-af1e3428d80f"),
            "vip/1"         to url("b817aba4-fad8-49e2-b88a-7cc744dfa6ec"),
        )
    }
}

// ── Top-level parsing function (pure, callable from composables) ──────────────

fun parseMessageSegments(
    message:          String,
    emotesTag:        String?,
    thirdPartyEmotes: Map<String, String>,
    youtubeEmotes:    Map<String, String> = emptyMap(),
    bits:             Int = 0
): List<MessageSegment> {
    
    // Step 1 — resolve Twitch native emote positions from the IRC tag
    // Tag format: "emoteId:start-end,start-end/emoteId:start-end"
    val twitchRanges = mutableListOf<Triple<Int, Int, String>>()  // start, end (inclusive), url
    emotesTag?.split("/")?.forEach { entry ->
        if (entry.isBlank()) return@forEach
        val colonIdx = entry.indexOf(':')
        if (colonIdx < 0) return@forEach
        val emoteId = entry.substring(0, colonIdx)
        val url = "https://static-cdn.jtvnw.net/emoticons/v2/$emoteId/default/dark/1.0"
        entry.substring(colonIdx + 1).split(",").forEach { range ->
            val dashIdx = range.indexOf('-')
            if (dashIdx < 0) return@forEach
            val start = range.substring(0, dashIdx).toIntOrNull() ?: return@forEach
            val end   = range.substring(dashIdx + 1).toIntOrNull() ?: return@forEach
            if (start <= end && end < message.length) twitchRanges.add(Triple(start, end, url))
        }
    }
    twitchRanges.sortBy { it.first }

    // Step 2 — split message into text gaps and Twitch emotes
    val stage1 = mutableListOf<MessageSegment>()
    var cursor = 0
    for ((start, end, url) in twitchRanges) {
        if (start > cursor) {
            val text = message.substring(cursor, start)
            if (text.isNotEmpty()) stage1.add(MessageSegment.TextPart(text))
        }
        val name = message.substring(start, end + 1)
        stage1.add(MessageSegment.EmotePart(name, url))
        cursor = end + 1
    }
    if (cursor < message.length) {
        val tail = message.substring(cursor)
        if (tail.isNotEmpty()) stage1.add(MessageSegment.TextPart(tail))
    }

    // Step 3 — YouTube Emotes (No strict boundaries)
    val stageYoutube = if (youtubeEmotes.isNotEmpty()) {
        stage1.flatMap { seg ->
            if (seg is MessageSegment.TextPart) scanWords(seg.text, youtubeEmotes, useStrictBoundaries = false)
            else listOf(seg)
        }
    } else stage1

    // Step 4 — Third-party Emotes (Strict boundaries)
    val stageThirdParty = if (thirdPartyEmotes.isNotEmpty()) {
        stageYoutube.flatMap { seg ->
            if (seg is MessageSegment.TextPart) scanWords(seg.text, thirdPartyEmotes, useStrictBoundaries = true)
            else listOf(seg)
        }
    } else stageYoutube

    // Step 5 — scan remaining TextParts for Bits (if applicable)
    val stageBits = if (bits > 0) {
        stageThirdParty.flatMap { seg ->
            if (seg is MessageSegment.TextPart) scanForBits(seg.text)
            else listOf(seg)
        }
    } else stageThirdParty

    // Step 6 — scan remaining TextParts for URLs
    return stageBits.flatMap { seg ->
        if (seg is MessageSegment.TextPart) scanForLinks(seg.text)
        else listOf(seg)
    }
}

private fun scanForBits(text: String): List<MessageSegment> {
    // Basic Twitch Cheer regex: matches "cheer", "bday", "kappa", etc. followed by digits
    // Twitch bits prefixes are many, but "cheer" is the standard. 
    // We'll focus on "cheer" as requested, but a more general one is common.
    val bitsRegex = Regex("(?<=^|\\s)(cheer)(\\d+)(?=\$|\\s)", RegexOption.IGNORE_CASE)
    val result = mutableListOf<MessageSegment>()
    var cursor = 0
    
    bitsRegex.findAll(text).forEach { match ->
        if (match.range.first > cursor) {
            result.add(MessageSegment.TextPart(text.substring(cursor, match.range.first)))
        }
        
        val name = match.groupValues[1]   // "cheer"
        val amountStr = match.groupValues[2] // "100"
        val amount = amountStr.toIntOrNull() ?: 0
        
        // Determine bit tier URL (Twitch CDN)
        val tier = when {
            amount >= 10000 -> "4"
            amount >= 5000  -> "3.5" // Not exactly, but Twitch has specific tiers
            amount >= 1000  -> "3"
            amount >= 100   -> "2"
            else            -> "1"
        }
        // Using common Twitch bits animation URLs
        // Note: Twitch has many styles, but "purple" or standard "cheer" is requested.
        val url = when {
            amount >= 10000 -> "https://static-cdn.jtvnw.net/bits/dark/animated/red/$tier"
            amount >= 5000  -> "https://static-cdn.jtvnw.net/bits/dark/animated/blue/$tier"
            amount >= 1000  -> "https://static-cdn.jtvnw.net/bits/dark/animated/green/$tier"
            amount >= 100   -> "https://static-cdn.jtvnw.net/bits/dark/animated/purple/$tier"
            else            -> "https://static-cdn.jtvnw.net/bits/dark/animated/gray/$tier"
        }
        
        result.add(MessageSegment.BitsPart(name, amountStr, url))
        cursor = match.range.last + 1
    }
    
    if (cursor < text.length) {
        result.add(MessageSegment.TextPart(text.substring(cursor)))
    }
    return result
}

private fun scanWords(text: String, emotes: Map<String, String>, useStrictBoundaries: Boolean): List<MessageSegment> {
    if (emotes.isEmpty()) return listOf(MessageSegment.TextPart(text))

    // Create a regex to find any of the emote keys, escaped for safety.
    // Sort keys by length descending to match longer strings first (e.g., ":heart_eyes:" before ":heart:")
    val escapedKeys = emotes.keys.filter { it.length >= 2 }.map { Regex.escape(it) }.sortedByDescending { it.length }
    if (escapedKeys.isEmpty()) return listOf(MessageSegment.TextPart(text))
    
    // Use lookbehind and lookahead to ensure emotes only match on whitespace boundaries if requested
    val pattern = if (useStrictBoundaries) {
        escapedKeys.joinToString("|", prefix = "(?<=^|\\s)(", postfix = ")(?=\$|\\s)").toRegex()
    } else {
        escapedKeys.joinToString("|", prefix = "(", postfix = ")").toRegex()
    }
    val result = mutableListOf<MessageSegment>()
    var cursor = 0

    pattern.findAll(text).forEach { match ->
        if (match.range.first > cursor) {
            result.add(MessageSegment.TextPart(text.substring(cursor, match.range.first)))
        }
        val word = match.value
        val url = emotes[word]!!
        result.add(MessageSegment.EmotePart(word, url))
        cursor = match.range.last + 1
    }

    if (cursor < text.length) {
        result.add(MessageSegment.TextPart(text.substring(cursor)))
    }
    return result
}

private val urlRegex = Regex(
    "((?:https?://|www\\.)[\\w\\d.\\-?&#=+%!:/~@_()$*,\\[\\];]+|(?:^|[\\s])(?:[\\w\\d\\-]+\\.)+(?:com|tv|net|org|io|me|gov|edu|tw|hk|jp|live)(?:/[\\w\\d.\\-?&#=+%!:/~@_()$*,\\[\\];]*)?)",
    RegexOption.IGNORE_CASE
)

private fun scanForLinks(text: String): List<MessageSegment> {
    val result = mutableListOf<MessageSegment>()
    var cursor = 0
    val matches = urlRegex.findAll(text)

    for (match in matches) {
        val matchedUrl = match.value.trim()
        val start = match.range.first + (match.value.length - match.value.trimStart().length)
        val end = start + matchedUrl.length

        if (start > cursor) {
            result.add(MessageSegment.TextPart(text.substring(cursor, start)))
        }

        val fullUrl = if (matchedUrl.startsWith("http", ignoreCase = true)) matchedUrl 
                     else "https://$matchedUrl"
        result.add(MessageSegment.LinkPart(matchedUrl, fullUrl))
        cursor = end
    }

    if (cursor < text.length) {
        result.add(MessageSegment.TextPart(text.substring(cursor)))
    }

    return result
}
