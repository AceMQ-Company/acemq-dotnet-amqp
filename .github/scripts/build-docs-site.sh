#!/usr/bin/env bash
# Render docs/*.md into site/, and fold in the Javadoc.
#
#   bash .github/scripts/build-docs-site.sh
#
# Lives here rather than in the shared scripts folder because the docs workflow
# runs from a checkout of this repository alone and cannot reach anything
# outside it. Runs locally too, for previewing before pushing.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"
OUT="site"

command -v pandoc >/dev/null || { echo "pandoc is required" >&2; exit 1; }

# The option was renamed: --highlight-style in older pandoc, --syntax-highlighting
# in newer, and each rejects or deprecates the other. Ubuntu's package and a
# current Homebrew install sit on opposite sides of that change, so the flag is
# chosen rather than assumed -- hardcoding either one breaks the build somewhere.
if pandoc --help 2>&1 | grep -q -- '--syntax-highlighting'; then
  HIGHLIGHT=(--syntax-highlighting=tango)
else
  HIGHLIGHT=(--highlight-style=tango)
fi

rm -rf "$OUT"
mkdir -p "$OUT"

# Images as well as stylesheets. Copying only *.css is how a logo ends up
# referenced by every page and served by none.
if compgen -G "docs/assets/*" > /dev/null; then
  mkdir -p "$OUT/assets"
  cp docs/assets/* "$OUT/assets/"
fi

cat > "$OUT/style.css" <<'CSS'
:root {
  color-scheme: light dark;
  --fg:#1a1a1a; --bg:#fff; --muted:#5f5f5f; --line:#e4e4e4;
  --accent:#b4451f; --code-bg:#f7f7f5; --nav-bg:#fbfbfa;
}
@media (prefers-color-scheme: dark) {
  :root { --fg:#e8e8e8; --bg:#161616; --muted:#9c9c9c; --line:#2d2d2d;
          --accent:#ff8a5c; --code-bg:#1e1e1e; --nav-bg:#1b1b1b; }
}
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--fg);
       font:16px/1.7 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
nav.top { background:var(--nav-bg); border-bottom:1px solid var(--line);
          padding:.85rem 1.25rem; display:flex; gap:1.15rem; flex-wrap:wrap; align-items:baseline;
          position:sticky; top:0; z-index:10; }
nav.top .brand { display:inline-flex; align-items:center; gap:.5rem; font-weight:700;
                 letter-spacing:-.01em; margin-right:.5rem; }
nav.top .brand img { height:22px; width:auto; display:block; }
/* The mark is black on transparent, so it disappears against a dark page.
   Inverting is enough for a two-tone logo and avoids shipping a second file. */
@media (prefers-color-scheme: dark) { nav.top .brand img { filter: invert(1) brightness(1.15); } }
nav.top a.enterprise { color:var(--fg); opacity:.78; }
nav.top a.enterprise:hover { opacity:1; color:var(--accent); }
nav.top a { color:var(--fg); text-decoration:none; font-size:.9rem; opacity:.78; }
nav.top a:hover { opacity:1; color:var(--accent); }
/* The right-hand group: the three destinations somebody arrives looking for,
   rather than the page-by-page guide. Tutorials carries the auto margin, so the
   group stays together however many guide pages are added to its left. */
nav.top a.tutorials { margin-left:auto; color:var(--accent); opacity:1; font-weight:600; }
nav.top a.api { color:var(--accent); opacity:1; font-weight:600; }
main { max-width:47rem; margin:0 auto; padding:2.5rem 1.25rem 5rem; }
h1 { font-size:2rem; letter-spacing:-.025em; margin:0 0 1.5rem; }
h2 { font-size:1.3rem; letter-spacing:-.015em; margin:2.75rem 0 .85rem;
     padding-top:.4rem; border-top:1px solid var(--line); }
h3 { font-size:1.05rem; margin:1.75rem 0 .6rem; }
p, li { color:var(--fg); }
a { color:var(--accent); }
pre { background:var(--code-bg); border:1px solid var(--line); border-radius:8px;
      padding:1rem 1.15rem; overflow-x:auto; font-size:.855rem; line-height:1.55; }
code { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:.9em; }
p code, li code, td code { background:var(--code-bg); border:1px solid var(--line);
      border-radius:4px; padding:.08em .35em; color:var(--accent); }
pre code { background:none; border:none; padding:0; color:inherit; }
table { border-collapse:collapse; width:100%; font-size:.92rem; display:block; overflow-x:auto; }
th,td { text-align:left; padding:.55rem .8rem; border-bottom:1px solid var(--line); vertical-align:top; }
th { color:var(--muted); font-weight:600; }
blockquote { border-left:3px solid var(--line); margin:1.25rem 0; padding:.2rem 0 .2rem 1.15rem; color:var(--muted); }
footer { max-width:47rem; margin:0 auto; padding:1.5rem 1.25rem 4rem;
         border-top:1px solid var(--line); color:var(--muted); font-size:.85rem; }
CSS

NAV='<nav class="top">
  <span class="brand"><img src="assets/acemq.png" alt="AceMQ"> for .NET</span>
  <a href="index.html">Overview</a>
  <a href="getting-started.html">Getting started</a>
  <a href="publishing.html">Publishing</a>
  <a href="consuming.html">Consuming</a>
  <a href="topology.html">Topology</a>
  <a href="testing.html">Testing</a>
  <a href="csharp.html">C#</a>
  <a href="vbnet.html">VB.NET</a>
  <a href="envelope.html">The envelope</a>
  <a class="api" href="https://acemq.org/">JVM libraries</a>
  <a class="enterprise" href="https://acemq.com">Enterprise support</a>
</nav>
<main>'

FOOT='</main>
<footer>
  <a href="https://github.com/AceMQ-Company/acemq-dotnet-amqp">AceMQ for .NET</a> &mdash;
  Apache-2.0, and provided without warranty &mdash; see the
  <a href="https://acemq.org/acemq-java-amqp/licence.html">licence</a>.
  <a href="https://acemq.com">Enterprise support</a>.
  The JVM libraries are at <a href="https://acemq.org/">acemq.org</a>.
  Pre-1.0 and in development: not published to a package feed yet.
  RabbitMQ and .NET are trademarks of their respective owners. This project is not
  affiliated with either.
</footer>'

printf '%s' "$NAV" > "$OUT/.nav.html"
printf '%s' "$FOOT" > "$OUT/.foot.html"

for f in docs/*.md; do
  base="$(basename "${f%.md}")"
  # The first heading is the page title. pagetitle rather than title, because
  # pandoc's template renders a title block from "title" and would print the
  # H1 that the markdown already contains.
  title="$(head -n1 "$f" | sed 's/^#\{1,6\} *//')"
  pandoc "$f" \
    --from=gfm --to=html5 --standalone \
    --metadata pagetitle="$title — AceMQ for .NET" \
    "${HIGHLIGHT[@]}" \
    --css=style.css \
    --include-before-body="$OUT/.nav.html" \
    --include-after-body="$OUT/.foot.html" \
    --output "$OUT/$base.html"
  # Links between pages are written as .md so they work when the same files are
  # read on GitHub; only the rendered copy is rewritten. The anchor is kept: a
  # link to another page's section is written .md#section and must survive.
  perl -pi -e 's{href="([^":#]*)\.md(#[^"]*)?"}{href="$1.html$2"}g' "$OUT/$base.html"
  echo "  rendered $base.html"
done

rm -f "$OUT/.nav.html" "$OUT/.foot.html"

# No API reference yet. DocFX would generate one from the XML comments, and it is
# worth adding when there is an API broad enough to browse -- today it is two
# types. A nav entry pointing at an empty page is worse than no entry.

# Jekyll would otherwise skip javadoc's underscore-prefixed resources.
touch "$OUT/.nojekyll"

echo "Site written to $REPO_ROOT/$OUT/index.html"
