package io.mars.udf.ollama;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The classification rules, ported from {@code DevinMB/whisper-sentiment} v1.0.1
 * as deployed.
 *
 * <p>Two details here are load-bearing rather than incidental:
 *
 * <ul>
 *   <li>The regex table is <b>ordered</b>. The first pattern that matches wins, so
 *       reordering it silently reclassifies messages.
 *   <li>The horny pattern <b>overrides the model</b>. If the regex says horny and
 *       the model said something else, the model is overruled. That is a
 *       deliberate product decision in the original, not a bug to tidy away.
 * </ul>
 */
final class SentimentRules implements Serializable {

    private static final long serialVersionUID = 1L;

    static final List<String> BUCKETS = List.of(
            "horny",
            "party_substance",
            "food_craving",
            "question_help",
            "banter_meme",
            "neutral_other");

    /** Ordered: first match wins. */
    static final List<Rule> ORDERED_REGEX = List.of(
            new Rule("horny", Pattern.compile(
                    "\\b(horny|thirsty|arous(e|ed)|sext|nsfw|turned on|moist|nut(ting)?|pussy|"
                    + "fuck|eat out|suck|ride|bang|69|slut|nude|booty|🍆|😈)\\b",
                    Pattern.CASE_INSENSITIVE)),
            new Rule("party_substance", Pattern.compile(
                    "\\b(weed|pen\\b|vape|joint|blunt|baja\\s*blast(ed)?|drunk|shots?|tequila|stoned|high)\\b",
                    Pattern.CASE_INSENSITIVE)),
            new Rule("food_craving", Pattern.compile(
                    "\\b(mcgriddles?|mcdonald'?s|taco\\s*bell|pizza|snack|coffee|caffeine|"
                    + "nectar of the gods|hungry|craving|eat|drink)\\b",
                    Pattern.CASE_INSENSITIVE)),
            new Rule("question_help", Pattern.compile(
                    "\\?\\s*$|^(how|what|why|who|where|when|can|does|do|is|are)\\b",
                    Pattern.CASE_INSENSITIVE)),
            new Rule("banter_meme", Pattern.compile(
                    "\\b(amen|lol|lmao|rofl|bit|meme|never forget|i('m| am) jesus|sacrifices were made)\\b",
                    Pattern.CASE_INSENSITIVE)));

    static final String SYSTEM_PROMPT =
            "You are a precise JSON-only classifier for short, informal chat messages. "
            + "Classify the message into exactly one of these buckets: "
            + String.join(", ", BUCKETS) + ". "
            + "Return strict JSON only with this shape: "
            + "{\"bucket\":\"<one_of_buckets>\",\"score\":<0..1>,\"reason\":\"<short>\"}. "
            + "Do not include any extra text besides the JSON.";

    private SentimentRules() {}

    /** Port of {@code regex_classify_first}: first matching bucket, or null. */
    static String regexClassifyFirst(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        for (Rule rule : ORDERED_REGEX) {
            if (rule.pattern.matcher(text).find()) {
                return rule.bucket;
            }
        }
        return null;
    }

    /** Port of {@code classify_with_regex}, including the confidence values. */
    static Sentiment regexFallback(String text) {
        String bucket = regexClassifyFirst(text == null ? "" : text);
        if (bucket != null) {
            double conf = "horny".equals(bucket) ? 0.85 : 0.7;
            return new Sentiment(bucket, conf, "regex:" + bucket, "regex", conf);
        }
        return new Sentiment("neutral_other", 0.55, "regex:none", "regex", 0.55);
    }

    static final class Rule implements Serializable {
        private static final long serialVersionUID = 1L;
        final String bucket;
        final Pattern pattern;

        Rule(String bucket, Pattern pattern) {
            this.bucket = bucket;
            this.pattern = pattern;
        }
    }

    static final class Sentiment implements Serializable {
        private static final long serialVersionUID = 1L;
        final String bucket;
        final double score;
        final String reason;
        final String model;
        final double confidence;

        Sentiment(String bucket, double score, String reason, String model, double confidence) {
            this.bucket = bucket;
            this.score = score;
            this.reason = reason;
            this.model = model;
            this.confidence = confidence;
        }
    }
}
