# Fixture generator

Writes `tests/AceMq.Amqp.Tests/fixtures/envelope-fixtures.json` by publishing through
`acemq-java-amqp` and pulling the message back at the transport level, which is the
only place the engine's own `x-acemq-*` headers are still visible — the consumer API
strips them and materialises them onto the envelope, by design.

The fixtures are **generated, never written**. Two implementations agreeing with the
same prose is not interoperability; agreeing with the same bytes is.

## Running it

```bash
JAVA=~/.sdkman/candidates/java/21.0.2-tem
R=../acemq-amqp-libraries/acemq-java-amqp          # adjust to taste

(cd "$R" && mvn -q -pl acemq-amqp-test -am -DskipTests \
    dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt)

CP="/tmp/fixgen-classes:$(cat /tmp/cp.txt)"
for m in acemq-amqp-api acemq-amqp-core acemq-amqp-test \
         acemq-transport-spi acemq-security-api acemq-amqp-codec-json; do
  CP="$CP:$R/$m/target/classes"
done

mkdir -p /tmp/fixgen-classes
"$JAVA/bin/javac" -cp "$CP" -d /tmp/fixgen-classes FixtureGen.java
"$JAVA/bin/java"  -cp "$CP" FixtureGen \
    ../../tests/AceMq.Amqp.Tests/fixtures/envelope-fixtures.json
```

## Where this should end up

Here for now because the .NET port is the only consumer. It belongs in
`acemq-java-amqp`'s own test suite, so that CI regenerates the fixtures on every
change and fails when they differ from what is committed. That turns "the port
drifted" from something discovered by a customer into a red build on the commit
that caused it — which is the whole reason the fixtures exist.
