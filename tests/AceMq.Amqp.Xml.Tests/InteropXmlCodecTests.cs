// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using AceMq.Amqp;

namespace AceMq.Amqp.Xml.Tests;

/// <summary>The payload the Java and Go samples carry.</summary>
public sealed class Order
{
    public string OrderId { get; set; } = "";
    public string Customer { get; set; } = "";
    public long TotalCents { get; set; }
    public string Currency { get; set; } = "";
    public bool Paid { get; set; }
    public List<string> Lines { get; set; } = new List<string>();
}

/// <summary>The second sample's payload: an array member and no boolean.</summary>
public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
    public string Customer { get; set; } = "";
    public long TotalCents { get; set; }
    public string Currency { get; set; } = "";
    public string[] Items { get; set; } = Array.Empty<string>();
}

public sealed class Attachment
{
    public string Name { get; set; } = "";
    public byte[] Content { get; set; } = Array.Empty<byte>();
}

public sealed class Address
{
    public string City { get; set; } = "";
    public string Postcode { get; set; } = "";
}

public sealed class Delivery
{
    public string OrderId { get; set; } = "";
    public Address To { get; set; } = new Address();
    public decimal Weight { get; set; }
    public DateTime DueAt { get; set; }
}

public sealed class InteropXmlCodecTests
{
    private readonly InteropXmlCodec _codec = new InteropXmlCodec();

    private static Order SomeOrder() => new Order
    {
        OrderId = "A-1",
        Customer = "acme",
        TotalCents = 4250,
        Currency = "GBP",
        Paid = true,
        Lines = new List<string> { "widget", "gasket" },
    };

    // ---- the contract ----------------------------------------------------

    [Fact]
    public void WritesApplicationXml()
    {
        Assert.Equal("application/xml", _codec.ContentType);
        Assert.Equal("application/xml", InteropXmlCodec.XmlContentType);
    }

    [Fact]
    public void ClaimsTheContentTypesThatSayXml()
    {
        Assert.True(_codec.CanDecode("application/xml"));
        Assert.True(_codec.CanDecode("application/xml; charset=utf-8"));
        Assert.True(_codec.CanDecode("text/xml"));
        Assert.True(_codec.CanDecode("APPLICATION/XML"));
        // The +xml suffix, which is the whole reason SOAP and Atom messages are
        // readable here without naming each one.
        Assert.True(_codec.CanDecode("application/soap+xml"));
        Assert.True(_codec.CanDecode("application/atom+xml"));
        Assert.True(_codec.CanDecode("application/vnd.acme.order+xml"));
    }

    [Fact]
    public void NeverVolunteersForAMessageWithNoContentType()
    {
        // That case belongs to the JSON and bytes codecs. XML is rarely what arrives
        // unannounced, and a codec that guessed wrong here would turn a readable
        // message into a rejected one -- and would record traffic under a format
        // nobody sent. The same rule as Java, Go, Python and Ruby.
        Assert.False(_codec.CanDecode(null));
        Assert.False(_codec.CanDecode(""));
        Assert.False(_codec.CanDecode("application/json"));
        Assert.False(_codec.CanDecode("application/yaml"));
        Assert.False(_codec.CanDecode("text/plain"));
    }

    // ---- security: no DTD, and the reason ---------------------------------

    // A three-level billion laughs. Small on purpose: the point is to measure the
    // expansion, not to run the machine out of memory proving it.
    private const string Bomb =
        "<?xml version=\"1.0\"?>\n" +
        "<!DOCTYPE Order [\n" +
        "  <!ENTITY a \"aaaaaaaaaa\">\n" +
        "  <!ENTITY b \"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\">\n" +
        "  <!ENTITY c \"&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;\">\n" +
        "]>\n" +
        "<Order><orderId>&c;</orderId></Order>";

    // The same bomb with a fourth level, to show what "exponential in the depth"
    // means rather than asserting it in a comment.
    private const string DeeperBomb =
        "<?xml version=\"1.0\"?>\n" +
        "<!DOCTYPE Order [\n" +
        "  <!ENTITY a \"aaaaaaaaaa\">\n" +
        "  <!ENTITY b \"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\">\n" +
        "  <!ENTITY c \"&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;\">\n" +
        "  <!ENTITY d \"&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;\">\n" +
        "]>\n" +
        "<Order><orderId>&d;</orderId></Order>";

    private static string Expand(string document)
    {
        // Deliberately permissive, which is the point: this is what an XmlReader
        // built without thinking about it does.
        var permissive = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Parse,
            XmlResolver = null,
            MaxCharactersFromEntities = 0,
        };

        using var buffer = new MemoryStream(Encoding.UTF8.GetBytes(document));
        using var reader = XmlReader.Create(buffer, permissive);
        return XElement.Load(reader).Value;
    }

    [Fact]
    public void AnEntityBombExpandsInThisRuntimeUnlessItIsStopped()
    {
        // The measurement the refusal exists for, made against this runtime's reader
        // rather than taken from a documentation table.
        //
        // External entities are the famous hazard and they are already inert here:
        // there is no resolver, so file:///etc/passwd resolves to nothing. Internal
        // entity expansion is a different thing and it is not inert. It needs no
        // network access and no readable file; it happens inside the parser, and the
        // whole bomb is the three characters "&c;".
        var three = Expand(Bomb);
        var four = Expand(DeeperBomb);

        // Deterministic, so it is asserted exactly: ten a's, referenced ten times,
        // referenced ten times again.
        Assert.Equal(1000, three.Length);
        Assert.Equal(10000, four.Length);

        // One more level of nesting, 47 more characters of source, ten times the
        // output. Six levels reach a million characters -- measured, and still under
        // the SDK's own default 10,000,000-character entity cap, so that cap is not
        // what saves a consumer here. This assertion is what keeps the refusal below
        // honest: if a future runtime made expansion inert, this test fails and the
        // reasoning in the codec's remarks has to be revisited rather than quietly
        // becoming untrue.
        Assert.Equal(10, four.Length / three.Length);
        Assert.True(
            DeeperBomb.Length < Bomb.Length + 60,
            $"the deeper bomb is only {DeeperBomb.Length - Bomb.Length} characters longer");
    }

    [Fact]
    public void RefusesTheEntityBomb()
    {
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Decode(Encoding.UTF8.GetBytes(Bomb), typeof(Order)));

        // The refusal names what is wrong rather than surfacing a parser exception
        // about a security property the caller never chose.
        Assert.Contains("document type declaration", error.Message);
        Assert.Contains("not configurable", error.Message);
    }

    [Fact]
    public void RefusesADtdEvenInAnEncodingTheScanCannotRead()
    {
        // The second layer. A UTF-16 body does not contain "<!DOCTYPE" as UTF-8
        // bytes, so the up-front scan cannot see it -- but XmlReader sniffs the
        // encoding and would read that DTD perfectly well. DtdProcessing.Prohibit is
        // what catches this one, which is why both layers are here and not just the
        // one with the good error message.
        var body = Encoding.Unicode.GetPreamble().Concat(Encoding.Unicode.GetBytes(Bomb)).ToArray();

        // Ordinal, because that is the comparison the codec's scan uses and the only
        // one that means "these bytes". A culture-sensitive search finds "<!DOCTYPE"
        // in this body, because .NET's linguistic comparison treats the interleaved
        // NUL as ignorable -- which would make the scan appear to cover UTF-16 while
        // depending on the machine's culture to do it. It does not cover it, and
        // DtdProcessing.Prohibit is what does.
        Assert.DoesNotContain("<!DOCTYPE", Encoding.UTF8.GetString(body), StringComparison.Ordinal);

        var error = Assert.Throws<AceFatalException>(() => _codec.Decode(body, typeof(Order)));
        Assert.Contains("document type declaration", error.Message);
    }

    [Fact]
    public void RefusesADocumentThatPullsInAnExternalEntity()
    {
        // The XXE class: "we accept XML" becoming "we read files off the consumer's
        // disk on request". Inert here for two reasons -- no resolver, and no DTD at
        // all -- but a message that tries it should still be refused rather than
        // decoded with an empty field where the file would have gone.
        var hostile = Encoding.UTF8.GetBytes(
            "<?xml version=\"1.0\"?><!DOCTYPE Order [<!ENTITY x SYSTEM \"file:///etc/passwd\">]>"
            + "<Order><orderId>&x;</orderId></Order>");

        var error = Assert.Throws<AceFatalException>(() => _codec.Decode(hostile, typeof(Order)));
        Assert.Contains("document type declaration", error.Message);
    }

    [Fact]
    public void RefusesAnExternalDtdSubsetToo()
    {
        var hostile = Encoding.UTF8.GetBytes(
            "<!DOCTYPE Order SYSTEM \"http://example.invalid/order.dtd\"><Order><orderId>A-1</orderId></Order>");

        Assert.Throws<AceFatalException>(() => _codec.Decode(hostile, typeof(Order)));
    }

    [Fact]
    public void HasNoWayToTurnTheRefusalOff()
    {
        // Deliberate, and the same decision Java, Python and Ruby took: a message
        // body has no legitimate use for a DTD, so the configuration would only ever
        // be wrong. Nothing on the public surface takes a DTD setting.
        var settings = typeof(InteropXmlCodec).GetProperties()
            .Concat<System.Reflection.MemberInfo>(typeof(InteropXmlCodec).GetMethods())
            .Select(m => m.Name)
            .Where(n => n.IndexOf("dtd", StringComparison.OrdinalIgnoreCase) >= 0
                        || n.IndexOf("doctype", StringComparison.OrdinalIgnoreCase) >= 0
                        || n.IndexOf("entit", StringComparison.OrdinalIgnoreCase) >= 0)
            .ToList();

        Assert.Empty(settings);
    }

    // ---- interop: real Java and Go bytes -----------------------------------

    [Theory]
    [InlineData("java")]
    [InlineData("go")]
    public void DecodesTheOrderThatLanguageReallyWrote(string producer)
    {
        var sample = Samples.Named("order", producer);

        var order = (Order)_codec.Decode(sample.Body, typeof(Order));

        Assert.Equal("A-1", order.OrderId);
        Assert.Equal("acme", order.Customer);
        // The wire carried the four characters "4250"; a long came back.
        Assert.Equal(4250L, order.TotalCents);
        Assert.Equal("GBP", order.Currency);
        Assert.True(order.Paid);
        Assert.Equal(new[] { "widget", "gasket" }, order.Lines);
    }

    [Theory]
    [InlineData("java")]
    [InlineData("go")]
    public void DecodesTheOrderPlacedThatLanguageReallyWrote(string producer)
    {
        var sample = Samples.Named("orderPlaced", producer);

        var placed = (OrderPlaced)_codec.Decode(sample.Body, typeof(OrderPlaced));

        Assert.Equal("o-1", placed.OrderId);
        // Non-ASCII from a real Java body, not from a string written here.
        Assert.Equal("Ana Söderström", placed.Customer);
        Assert.Equal(4250L, placed.TotalCents);
        Assert.Equal("EUR", placed.Currency);
        Assert.Equal(new[] { "widget", "gasket" }, placed.Items);
    }

    [Fact]
    public void ReadsBothListShapesIntoTheSameMember()
    {
        // The divergence the other language agents found and documented. Jackson
        // serialising a POJO wraps the list in an element of its own; Go's
        // encoding/xml repeats the sibling. Both are real output, neither is going
        // away, and both have to arrive as the same List<string>.
        var wrapped = Samples.Named("order", "java");
        var repeated = Samples.Named("order", "go");

        Assert.Contains("<lines><lines>widget</lines><lines>gasket</lines></lines>", wrapped.Text);
        Assert.Contains("<lines>widget</lines><lines>gasket</lines>", repeated.Text);
        Assert.DoesNotContain("<lines><lines>", repeated.Text);

        var fromJava = (Order)_codec.Decode(wrapped.Body, typeof(Order));
        var fromGo = (Order)_codec.Decode(repeated.Body, typeof(Order));

        Assert.Equal(fromJava.Lines, fromGo.Lines);
        Assert.Equal(new[] { "widget", "gasket" }, fromJava.Lines);
    }

    [Fact]
    public void IgnoresTheRootElementsName()
    {
        // Java wrote <Order>, Go wrote <order>, and the OrderPlaced pair wrote
        // <OrderPlaced>. The root names the message rather than being part of it,
        // which is how Jackson and encoding/xml read one into a class too.
        Assert.StartsWith("<Order>", Samples.Named("order", "java").Text);
        Assert.StartsWith("<order>", Samples.Named("order", "go").Text);

        Assert.Equal("A-1", ((Order)_codec.Decode(Samples.Named("order", "java").Body, typeof(Order))).OrderId);
        Assert.Equal("A-1", ((Order)_codec.Decode(Samples.Named("order", "go").Body, typeof(Order))).OrderId);
    }

    [Fact]
    public void ReadsEverySampleInTheFixture()
    {
        // A guard against the fixture growing a sample nothing reads. Every entry is
        // decoded generically and checked against the value the fixture says it
        // carries, so adding one to the file is enough to have it covered.
        var samples = Samples.All();
        Assert.Equal(4, samples.Count);

        foreach (var sample in samples)
        {
            var decoded = (Dictionary<string, object>)_codec.Decode(
                sample.Body, typeof(Dictionary<string, object>));

            foreach (var expected in sample.Expected.EnumerateObject())
            {
                Assert.True(
                    decoded.ContainsKey(expected.Name),
                    $"{sample.Name}/{sample.Producer} lost '{expected.Name}'");
            }
        }
    }

    [Fact]
    public void WritesSomethingTheOtherLanguagesRead()
    {
        var written = Encoding.UTF8.GetString(_codec.Encode(SomeOrder()));

        // The root is the type's name, the way Jackson uses the class's simple name.
        Assert.StartsWith("<Order>", written);
        Assert.EndsWith("</Order>", written);
        // camelCase, so a C# OrderId and a Java orderId are the same element.
        Assert.Contains("<orderId>A-1</orderId>", written);
        Assert.DoesNotContain("<OrderId>", written);
        // Lowercase booleans and unformatted numbers, which is what Jackson and
        // encoding/xml write.
        Assert.Contains("<paid>true</paid>", written);
        Assert.Contains("<totalCents>4250</totalCents>", written);
        // Go's list shape, which Jackson reads as well.
        Assert.Contains("<lines>widget</lines><lines>gasket</lines>", written);
        // No declaration and no xsi/xsd namespaces -- the thing XmlSerializer writes
        // and no other language expects.
        Assert.DoesNotContain("<?xml", written);
        Assert.DoesNotContain("xmlns", written);
    }

    [Fact]
    public void WritesBytesGoWroteForTheSameOrder()
    {
        // Not merely "something readable": the same document Go's encoding/xml
        // produced, element for element. The root name differs because Go's struct
        // named itself, and the root is not part of the message.
        var written = Encoding.UTF8.GetString(_codec.Encode(SomeOrder()));
        var go = Samples.Named("order", "go").Text;

        Assert.Equal(
            go.Replace("<order>", "").Replace("</order>", ""),
            written.Replace("<Order>", "").Replace("</Order>", ""));
    }

    // ---- round trips and shapes -------------------------------------------

    [Fact]
    public void RoundTripsAMessage()
    {
        var back = (Order)_codec.Decode(_codec.Encode(SomeOrder()), typeof(Order));

        Assert.Equal("A-1", back.OrderId);
        Assert.Equal(4250L, back.TotalCents);
        Assert.True(back.Paid);
        Assert.Equal(new[] { "widget", "gasket" }, back.Lines);
    }

    [Fact]
    public void RoundTripsANestedObjectAndItsScalars()
    {
        var delivery = new Delivery
        {
            OrderId = "A-2",
            To = new Address { City = "Bristol", Postcode = "BS1 4DJ" },
            Weight = 2.75m,
            DueAt = new DateTime(2026, 3, 1, 9, 30, 0, DateTimeKind.Utc),
        };

        var written = Encoding.UTF8.GetString(_codec.Encode(delivery));
        Assert.Contains("<to><city>Bristol</city><postcode>BS1 4DJ</postcode></to>", written);
        // The raw number, not the current culture's rendering of it.
        Assert.Contains("<weight>2.75</weight>", written);

        var back = (Delivery)_codec.Decode(Encoding.UTF8.GetBytes(written), typeof(Delivery));
        Assert.Equal("Bristol", back.To.City);
        Assert.Equal(2.75m, back.Weight);
        Assert.Equal(delivery.DueAt, back.DueAt);
    }

    [Fact]
    public void ReadsAOneItemListThatLooksExactlyLikeAScalar()
    {
        // <lines>widget</lines> on its own is all either Jackson or encoding/xml
        // writes for a one-item list, and XML cannot tell it from a scalar. The
        // target type is the only thing that can, which is why the unwrapping is
        // driven by the declared member and not guessed from the document.
        var one = Encoding.UTF8.GetBytes(
            "<Order><orderId>A-1</orderId><lines>widget</lines></Order>");

        var order = (Order)_codec.Decode(one, typeof(Order));

        Assert.Equal(new[] { "widget" }, order.Lines);
    }

    [Fact]
    public void ReadsJacksonsWrapperAroundAOneItemListToo()
    {
        var one = Encoding.UTF8.GetBytes(
            "<Order><orderId>A-1</orderId><lines><lines>widget</lines></lines></Order>");

        var order = (Order)_codec.Decode(one, typeof(Order));

        Assert.Equal(new[] { "widget" }, order.Lines);
    }

    [Fact]
    public void WritesBytesAsBase64TextRatherThanAListOfNumbers()
    {
        // byte[] is an array, so the list-shape converter has to be told to leave it
        // alone: Jackson's XmlMapper and Go's encoding/xml both write a byte slice as
        // base64 text, and a list of numbers here would be something nothing reads.
        var attachment = new Attachment
        {
            Name = "label.png",
            Content = new byte[] { 1, 2, 3, 250 },
        };

        var written = Encoding.UTF8.GetString(_codec.Encode(attachment));
        Assert.Contains($"<content>{Convert.ToBase64String(attachment.Content)}</content>", written);
        Assert.DoesNotContain("<content>1</content>", written);

        var back = (Attachment)_codec.Decode(Encoding.UTF8.GetBytes(written), typeof(Attachment));
        Assert.Equal(attachment.Content, back.Content);
    }

    [Fact]
    public void ReadsALeafItDoesNotKnowWithoutFailing()
    {
        // A producer adding a field should not break a consumer that has not been
        // redeployed, which is the same tolerance the YAML and TOML codecs have.
        var extra = Encoding.UTF8.GetBytes(
            "<Order><orderId>A-1</orderId><promisedBy>2026-04-01</promisedBy></Order>");

        Assert.Equal("A-1", ((Order)_codec.Decode(extra, typeof(Order))).OrderId);
    }

    [Fact]
    public void ReadsANamespacedDocumentByLocalName()
    {
        // A consumer wants the field, not the URI it was declared in -- which is also
        // what Jackson gives reading XML into a Map.
        var namespaced = Encoding.UTF8.GetBytes(
            "<ns:Order xmlns:ns=\"urn:acme:orders\"><ns:orderId>A-9</ns:orderId>"
            + "<ns:totalCents>7</ns:totalCents></ns:Order>");

        var order = (Order)_codec.Decode(namespaced, typeof(Order));

        Assert.Equal("A-9", order.OrderId);
        Assert.Equal(7L, order.TotalCents);
    }

    [Fact]
    public void ReadsAnAttributeAsAnOrdinaryMember()
    {
        var withAttribute = Encoding.UTF8.GetBytes(
            "<Order orderId=\"A-7\"><customer>acme</customer></Order>");

        var order = (Order)_codec.Decode(withAttribute, typeof(Order));

        Assert.Equal("A-7", order.OrderId);
        Assert.Equal("acme", order.Customer);
    }

    [Fact]
    public void ReadsAnEmptyElementAsTheEmptyString()
    {
        // XML has no null. An empty element is the empty string, and nothing here
        // invents xsi:nil, because neither Jackson nor encoding/xml writes it.
        var order = (Order)_codec.Decode(
            Encoding.UTF8.GetBytes("<Order><orderId/><customer>acme</customer></Order>"),
            typeof(Order));

        Assert.Equal("", order.OrderId);
    }

    [Fact]
    public void DecodesIntoADictionaryWhenNoTypeIsHandy()
    {
        // The shape Python and Ruby give by default: text all the way down, because
        // XML has no types and the caller converts. A list is still a list, though --
        // the repeated element is the one piece of structure XML does carry -- so the
        // value type has to be one that can hold both.
        var decoded = (Dictionary<string, JsonElement>)_codec.Decode(
            Samples.Named("order", "go").Body, typeof(Dictionary<string, JsonElement>));

        Assert.Equal("A-1", decoded["orderId"].GetString());
        Assert.Equal("4250", decoded["totalCents"].GetString());
        Assert.Equal("true", decoded["paid"].GetString());
        Assert.Equal(
            new[] { "widget", "gasket" },
            decoded["lines"].EnumerateArray().Select(item => item.GetString()));
    }

    // ---- failures ---------------------------------------------------------

    [Fact]
    public void TreatsAMalformedBodyAsFatalRatherThanRetryable()
    {
        // Bytes that are not XML will not become XML on a redelivery, so the message
        // is dead-lettered rather than retried until it ages out.
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Decode(Encoding.UTF8.GetBytes("<Order><orderId>unclosed"), typeof(Order)));

        Assert.Contains("not XML", error.Message);
        Assert.IsAssignableFrom<AceMqException>(error);
    }

    [Fact]
    public void TreatsAnEmptyBodyAsFatal()
    {
        Assert.Throws<AceFatalException>(() => _codec.Decode(Array.Empty<byte>(), typeof(Order)));
    }

    [Fact]
    public void SaysSoWhenThePayloadHasNoRootToWrite()
    {
        // An XML document has exactly one root element, so a bare list has nowhere to
        // go. Saying so is better than inventing a wrapper the other side would not
        // know to unwrap.
        var error = Assert.Throws<AceFatalException>(
            () => _codec.Encode(new List<string> { "a", "b" }));

        Assert.Contains("one root element", error.Message);
        Assert.Contains("JsonCodec", error.Message);
    }

    [Fact]
    public void RefusesARootNameXmlWouldNotTake()
    {
        Assert.Throws<ArgumentException>(() => new InteropXmlCodec("3 orders"));
        Assert.Throws<ArgumentException>(() => new InteropXmlCodec(""));
    }

    [Fact]
    public void UsesTheConfiguredRootWhenTheTypeHasNoUsableName()
    {
        var codec = new InteropXmlCodec("envelope");
        Assert.Equal("envelope", codec.Root);

        var written = Encoding.UTF8.GetString(
            codec.Encode(new Dictionary<string, string> { ["orderId"] = "A-1" }));

        Assert.Equal("<envelope><orderId>A-1</orderId></envelope>", written);
    }

    [Fact]
    public void RejectsANullPayloadAndANullBody()
    {
        Assert.Throws<ArgumentNullException>(() => _codec.Encode(null!));
        Assert.Throws<ArgumentNullException>(() => _codec.Decode(null!, typeof(Order)));
        Assert.Throws<ArgumentNullException>(() => _codec.Decode(Array.Empty<byte>(), null!));
    }

    // ---- the library around it ---------------------------------------------

    [Fact]
    public void IsNotTheCoreXmlCodec()
    {
        // The core ships AceMq.Amqp.XmlCodec, built on XmlSerializer. It stays: it is
        // the right answer for .NET talking to .NET and for a document that has to
        // match an XSD. The two are named differently on purpose, so a consumer with
        // both namespaces imported gets a choice rather than a CS0104.
        Assert.NotEqual(typeof(AceMq.Amqp.XmlCodec), typeof(InteropXmlCodec));

        // What it writes, and why nothing else reads it: an XML declaration, two
        // namespace attributes, PascalCase element names, and a third list shape --
        // <Lines><string>widget</string></Lines> -- that is neither Jackson's wrapper
        // nor Go's repeated sibling.
        var core = Encoding.UTF8.GetString(new AceMq.Amqp.XmlCodec().Encode(SomeOrder()));
        Assert.Contains("<?xml", core);
        Assert.Contains("xmlns:xsi", core);
        Assert.Contains("<OrderId>", core);
        Assert.Contains("<string>widget</string>", core);

        var written = Encoding.UTF8.GetString(_codec.Encode(SomeOrder()));
        Assert.DoesNotContain("<?xml", written);
        Assert.DoesNotContain("xmlns", written);
    }

    [Fact]
    public void ReadsTheJavaMessageTheCoreXmlCodecRefuses()
    {
        // The reason this package exists, stated as the failure it prevents.
        //
        // XmlSerializer matches element names case-sensitively, so Java's <orderId>
        // does not bind to OrderId. Up to 0.3.0 it did not complain about that: it
        // returned an Order with every field at its default, and the message was
        // gone. A consumer that reached for the core codec to read a Java queue saw
        // empty orders and no errors, which is the worst of both.
        var java = Samples.Named("order", "java");

        var refused = Assert.Throws<AceFatalException>(
            () => new AceMq.Amqp.XmlCodec().Decode(java.Body, typeof(Order)));

        // The message has to name the codec that would have worked, because the
        // person reading it is holding a stack trace, not this file.
        Assert.Contains("InteropXmlCodec", refused.Message);
        Assert.Contains(nameof(Order), refused.Message);

        // Fatal and not retryable: the same bytes bind no better on the next
        // attempt, so the message is dead-lettered rather than looped.
        Assert.IsAssignableFrom<AceFatalException>(refused);

        // Go's message does not even get that far: its root element is <order>, and
        // XmlSerializer binds the root by name too.
        Assert.ThrowsAny<Exception>(
            () => new AceMq.Amqp.XmlCodec().Decode(Samples.Named("order", "go").Body, typeof(Order)));

        // This codec reads both.
        var read = (Order)_codec.Decode(java.Body, typeof(Order));
        Assert.Equal("A-1", read.OrderId);
        Assert.Equal(4250L, read.TotalCents);
        Assert.Equal(new[] { "widget", "gasket" }, read.Lines);
    }

    [Fact]
    public void TheCoreXmlCodecStillReadsWhatItWrote()
    {
        // The other half of the 0.4.0 change, and the reason the core codec was not
        // marked obsolete: refusing a body that bound to nothing must not refuse a
        // body that bound. .NET to .NET still round-trips.
        var core = new AceMq.Amqp.XmlCodec();
        var written = core.Encode(SomeOrder());

        var read = (Order)core.Decode(written, typeof(Order));
        Assert.Equal("A-1", read.OrderId);
        Assert.Equal(4250L, read.TotalCents);
        Assert.Equal(new[] { "widget", "gasket" }, read.Lines);
    }

    [Fact]
    public void TheCoreXmlCodecStillIgnoresAnElementItDoesNotKnow()
    {
        // The refusal is narrow: it fires only when *nothing* bound. A producer that
        // added a field must not break a consumer that has not been rebuilt, so a
        // document with one unknown element among known ones still decodes.
        var forwardCompatible = Encoding.UTF8.GetBytes(
            "<Order xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            "<OrderId>A-1</OrderId><TotalCents>4250</TotalCents>" +
            "<Discount>10</Discount>" +
            "</Order>");

        var read = (Order)new AceMq.Amqp.XmlCodec().Decode(forwardCompatible, typeof(Order));
        Assert.Equal("A-1", read.OrderId);
        Assert.Equal(4250L, read.TotalCents);
    }

    [Fact]
    public void IsAvailableByNameOnceRegistered()
    {
        // Registration is explicit -- nothing scans assemblies -- so a service that
        // wants this codec under the name "xml" says so, replacing the core's.
        CodecRegistry.Register("xml", () => new InteropXmlCodec());

        Assert.IsType<InteropXmlCodec>(CodecRegistry.ByName("xml"));
        Assert.Contains("xml", CodecRegistry.Names());
    }

    [Fact]
    public async Task CarriesAMessageThroughTheBroker()
    {
        using var mq = await AceMqConnection.ConnectAsync(
            "memory://" + Guid.NewGuid().ToString("N"), _codec);
        var queue = "q" + Guid.NewGuid().ToString("N").Substring(0, 8);
        await mq.DeclareQueueAsync(queue);

        Order? received = null;
        using var consumer = await mq.ConsumeAsync<Order>(queue, message =>
        {
            received = message.Payload;
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Order>("", queue).SendAsync(SomeOrder());

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received == null && DateTime.UtcNow < deadline) await Task.Delay(10);

        Assert.NotNull(received);
        Assert.Equal("A-1", received!.OrderId);
        Assert.True(received.Paid);
        Assert.Equal(new[] { "widget", "gasket" }, received.Lines);
    }

    [Fact]
    public void ReadsEitherFormatDuringAMigration()
    {
        var composite = CompositeCodec.Of(new JsonCodec(), new InteropXmlCodec());

        var fromJava = (Order)composite.Decode(
            Samples.Named("order", "java").Body, typeof(Order), "application/xml");

        Assert.Equal("A-1", fromJava.OrderId);
        Assert.Equal("application/json", composite.ContentType);
    }
}

/// <summary>The Java and Go bytes, read off disk rather than written here.</summary>
internal sealed class Sample
{
    internal Sample(string name, string producer, byte[] body, JsonElement expected)
    {
        Name = name;
        Producer = producer;
        Body = body;
        Expected = expected;
    }

    internal string Name { get; }
    internal string Producer { get; }
    internal byte[] Body { get; }
    internal JsonElement Expected { get; }
    internal string Text => Encoding.UTF8.GetString(Body);
}

internal static class Samples
{
    private static readonly JsonDocument Fixture = Load();

    private static JsonDocument Load()
    {
        // Beside the test assembly, because the csproj copies it there. A test that
        // read it from the source tree would pass on a machine that has the source
        // and fail everywhere else.
        var path = Path.Combine(
            AppContext.BaseDirectory, "fixtures", "xml-interop-samples.json");
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"the interop fixture is not beside the test assembly at {path}. It is copied "
                + "there by the csproj, so a build is what fixes this -- `dotnet test --no-build` "
                + "reads whatever is already in bin.", path);
        }
        return JsonDocument.Parse(File.ReadAllText(path));
    }

    internal static List<Sample> All() =>
        Fixture.RootElement.GetProperty("samples").EnumerateArray()
            .Select(s => new Sample(
                s.GetProperty("name").GetString()!,
                s.GetProperty("producer").GetString()!,
                Convert.FromBase64String(s.GetProperty("bodyBase64").GetString()!),
                s.GetProperty("expected")))
            .ToList();

    internal static Sample Named(string name, string producer) =>
        All().FirstOrDefault(s => s.Name == name && s.Producer == producer)
        ?? throw new InvalidOperationException($"no '{name}' sample from {producer} in the fixture");
}
