import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.acemq.amqp.api.*;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.test.InMemoryTransport;
import org.acemq.amqp.transport.*;

/**
 * Writes the envelope wire contract to JSON by publishing through the real library
 * and then pulling the message back at the transport level, which is the only place
 * the engine's own x-acemq-* headers are visible: the consumer API strips them and
 * materialises them onto the Envelope, by design.
 *
 * Generated rather than transcribed. A port that hand-copies these from prose
 * acquires a difference nobody notices until two languages disagree in production.
 */
public final class FixtureGen {
    public static void main(String[] args) throws Exception {
        StringBuilder out = new StringBuilder();
        out.append("{\n  \"generatedBy\": \"acemq-java-amqp FixtureGen\",\n");
        out.append("  \"contract\": \"the headers an AceMQ publish puts on the wire\",\n");
        out.append("  \"cases\": [\n");

        try (AceMq mq = AceMq.connect("memory://fixtures", Telemetry.NONE);
             TransportConnection raw = new InMemoryTransport()
                     .connect(ConnectionConfig.url("memory://fixtures").build())) {

            mq.declareExchange("fx", "topic");
            mq.declareQueue("fx.q", QueueType.CLASSIC, Map.of());
            mq.bind("fx.q", "fx", "fx.*");

            mq.publisher("fx", "fx.plain", String.class).send("{\"id\":\"o-1\"}");
            emit(out, "minimal", raw, true);

            Envelope rich = Envelope.of("order.placed")
                    .id("11111111-2222-3333-4444-555555555555")
                    .version(3)
                    .correlationId("corr-1")
                    .causationId("cause-1")
                    .origin("orders@host-7")
                    .firstSeen(Instant.parse("2026-01-02T03:04:05.678Z"))
                    .header("x-tenant", "acme")
                    .build();
            mq.publisher("fx", "fx.rich", String.class).send("{\"id\":\"o-2\"}", rich);
            emit(out, "populated", raw, false);
        }

        out.append("\n  ]\n}\n");
        Files.writeString(Path.of(args[0]), out.toString());
        System.out.println("wrote " + args[0]);
    }

    private static void emit(StringBuilder out, String name, TransportConnection raw, boolean first) {
        InboundDelivery m = raw.receive("fx.q", java.time.Duration.ofSeconds(5))
                .orElseThrow(() -> new IllegalStateException("nothing on the queue for " + name))
                .delivery();
        if (!first) out.append(",\n");
        out.append("    {\n      \"case\": ").append(q(name)).append(",\n");
        out.append("      \"routingKey\": ").append(q(m.routingKey())).append(",\n");
        out.append("      \"messageId\": ").append(q(m.messageId())).append(",\n");
        out.append("      \"body\": ").append(q(new String(m.body(), java.nio.charset.StandardCharsets.UTF_8))).append(",\n");
        out.append("      \"contentType\": ").append(q(m.contentType())).append(",\n");
        out.append("      \"headers\": {\n");
        List<String> keys = new ArrayList<>(m.headers().keySet());
        Collections.sort(keys);
        for (int i = 0; i < keys.size(); i++) {
            Object v = m.headers().get(keys.get(i));
            String rendered = (v instanceof Number || v instanceof Boolean) ? String.valueOf(v) : q(String.valueOf(v));
            out.append("        ").append(q(keys.get(i))).append(": ").append(rendered)
               .append("  ").append(typeNote(v))
               .append(i < keys.size() - 1 ? ",\n" : "\n");
        }
        out.append("      }\n    }");
    }

    private static String typeNote(Object v) {
        return "";
    }

    private static String q(String s) {
        return s == null ? "null" : "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
