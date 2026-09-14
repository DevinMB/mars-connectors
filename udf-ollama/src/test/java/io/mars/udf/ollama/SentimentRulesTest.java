package io.mars.udf.ollama;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The regex table decides how messages are labelled whenever the model is
 * unavailable, and its ordering decides the answer whenever two patterns both
 * match. A subtle port error here would relabel messages rather than fail.
 */
class SentimentRulesTest {

    @ParameterizedTest
    @CsvSource({
            "I'm so horny,                      horny",
            "feeling thirsty today,             horny",
            "hitting the vape,                  party_substance",
            "got absolutely drunk,              party_substance",
            "craving mcgriddles,                food_craving",
            "taco bell run?,                    food_craving",
            "want some pizza,                   food_craving",
            "how do I fix this,                 question_help",
            "is anyone around,                  question_help",
            "lmao that meme,                    banter_meme",
            "amen to that,                      banter_meme",
            "the weather is quite pleasant,     NONE",
    })
    @DisplayName("each example lands in the bucket the deployed rules give it")
    void classification(String text, String expected) {
        String got = SentimentRules.regexClassifyFirst(text);
        if ("NONE".equals(expected)) {
            assertNull(got, () -> "expected no match for: " + text);
        } else {
            assertEquals(expected, got, () -> "for: " + text);
        }
    }

    @Test
    @DisplayName("ordering decides when two patterns both match")
    void orderingMatters() {
        // "eat" is in food_craving and "eat out" is in horny; horny is checked
        // first, so it wins. Reordering the table would change this answer.
        assertEquals("horny", SentimentRules.regexClassifyFirst("want to eat out later"));

        // "high" is party_substance and "how" is question_help; party comes first.
        assertEquals("party_substance", SentimentRules.regexClassifyFirst("how high are you"));
    }

    @Test
    @DisplayName("a trailing question mark is enough for question_help")
    void trailingQuestionMark() {
        assertEquals("question_help", SentimentRules.regexClassifyFirst("you around?"));
        // ...but only at the end, which is what the pattern anchors.
        assertNotEquals("question_help", SentimentRules.regexClassifyFirst("wait? no never mind"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    @DisplayName("empty text matches nothing")
    void emptyText(String text) {
        assertNull(SentimentRules.regexClassifyFirst(text.isEmpty() ? text : "   "));
    }

    @Test
    void nullTextIsSafe() {
        assertNull(SentimentRules.regexClassifyFirst(null));
    }

    @Test
    @DisplayName("fallback confidences match the Python exactly")
    void fallbackConfidences() {
        SentimentRules.Sentiment horny = SentimentRules.regexFallback("so horny");
        assertEquals("horny", horny.bucket);
        assertEquals(0.85, horny.score);
        assertEquals("regex:horny", horny.reason);
        assertEquals("regex", horny.model);

        SentimentRules.Sentiment other = SentimentRules.regexFallback("want pizza");
        assertEquals(0.7, other.score);

        SentimentRules.Sentiment none = SentimentRules.regexFallback("nothing notable here");
        assertEquals("neutral_other", none.bucket);
        assertEquals(0.55, none.score);
        assertEquals("regex:none", none.reason);
    }

    @Test
    @DisplayName("case is ignored")
    void caseInsensitive() {
        assertEquals("horny", SentimentRules.regexClassifyFirst("HORNY"));
        assertEquals("banter_meme", SentimentRules.regexClassifyFirst("LMAO"));
    }

    @Test
    @DisplayName("the emoji alternatives are dead code, and faithfully so")
    void emojiNeverMatches() {
        // The horny pattern lists an aubergine and a smiling-devil emoji, but both
        // sit inside \b(...)\b, and \b requires a word-character boundary that an
        // emoji does not provide. Neither alternative can ever fire.
        //
        // Verified against the deployed Python, which returns None for all three of
        // these. Reproduced rather than corrected: making them match would
        // reclassify messages that have always scored another way, which is a
        // product decision, not a port detail.
        assertNull(SentimentRules.regexClassifyFirst("🍆"));
        assertNull(SentimentRules.regexClassifyFirst("😈"));
        assertNull(SentimentRules.regexClassifyFirst("so 🍆 today"));
    }

    @Test
    @DisplayName("the bucket list and prompt are intact")
    void promptAndBuckets() {
        assertEquals(6, SentimentRules.BUCKETS.size());
        assertEquals("horny", SentimentRules.BUCKETS.get(0));
        assertEquals("neutral_other", SentimentRules.BUCKETS.get(5));
        assertTrue(SentimentRules.SYSTEM_PROMPT.contains("JSON-only classifier"));
        for (String b : SentimentRules.BUCKETS) {
            assertTrue(SentimentRules.SYSTEM_PROMPT.contains(b), b + " missing from prompt");
        }
    }
}
