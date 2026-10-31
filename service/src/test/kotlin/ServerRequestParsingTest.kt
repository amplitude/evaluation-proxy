import com.amplitude.getFlagKeys
import com.amplitude.getTracksAssignment
import com.amplitude.getTracksExposure
import io.ktor.http.Headers
import io.ktor.util.encodeBase64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ServerRequestParsingTest {
    @Test
    fun `flag keys parsed from flag_keys query parameter`() {
        assertEquals(setOf("a", "b"), getFlagKeys(null, "a,b"))
        assertEquals(setOf("a", "b"), getFlagKeys(null, " a , b ,"))
        assertEquals(emptySet(), getFlagKeys(null, null))
    }

    @Test
    fun `flag keys parsed from header take precedence over query`() {
        val header = """["x","y"]""".encodeBase64()
        assertEquals(setOf("x", "y"), getFlagKeys(header, "a,b"))
    }

    @Test
    fun `assignment tracking defaults to track and honors no-track`() {
        assertTrue(Headers.Empty.getTracksAssignment())
        assertTrue(Headers.build { set("X-Amp-Exp-Track", "track") }.getTracksAssignment())
        assertTrue(Headers.build { set("X-Amp-Exp-Track", "bogus") }.getTracksAssignment())
        assertFalse(Headers.build { set("X-Amp-Exp-Track", "no-track") }.getTracksAssignment())
        assertFalse(Headers.build { set("X-Amp-Exp-Track", "No-Track") }.getTracksAssignment())
    }

    @Test
    fun `exposure tracking defaults to no-track and honors track`() {
        assertFalse(Headers.Empty.getTracksExposure())
        assertFalse(Headers.build { set("X-Amp-Exp-Exposure-Track", "no-track") }.getTracksExposure())
        assertTrue(Headers.build { set("X-Amp-Exp-Exposure-Track", "track") }.getTracksExposure())
    }
}
