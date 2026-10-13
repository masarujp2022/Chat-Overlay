package com.kongjjj.overlay

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test

class TwitchChatClientTest {

    @Test
    fun testUserNoticeRemovesTmiTwitchTv() {
        val client = TwitchChatClient()
        val rawIrcLine = "@badges=subscriber/1;color=#FF0000;display-name=TestUser;login=testuser;msg-id=sub;system-msg=TestUser\\s(tmi.twitch.tv)\\ssubscribed\\sat\\sTier\\s1. :tmi.twitch.tv USERNOTICE #channel :Great stream!"

        val msg = client.parseTwitchIrcLine(rawIrcLine)
        assertNotNull(msg)
        assertEquals("TestUser", msg!!.username)
        assertEquals("testuser", msg.login)
        assertEquals("TestUser subscribed at Tier 1.", msg.rawSystemMessage)
    }
}
