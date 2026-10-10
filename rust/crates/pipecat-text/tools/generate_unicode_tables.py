"""Developer-only Unicode category table generator; Cargo never invokes Python."""
import unicodedata as u
from pathlib import Path
out = ['//! Unicode category tables used by the source-oracle interpreter.\n', f'pub(crate) const UNICODE_VERSION: &str = "{u.unidata_version}";\n']
for name, accepts in [('LETTER', lambda c: c.startswith('L')), ('NUMBER', lambda c: c.startswith('N')), ('PUNCTUATION', lambda c: c.startswith('P')), ('NONSPACING_MARK', lambda c: c == 'Mn')]:
    ranges = []
    for point in range(0x110000):
        if not accepts(u.category(chr(point))): continue
        if ranges and ranges[-1][1] + 1 == point: ranges[-1][1] = point
        else: ranges.append([point, point])
    out.append(f'const {name}: &[(u32, u32)] = &[\n')
    out.extend(f'    (0x{a:x}, 0x{b:x}),\n' for a, b in ranges)
    out.append('];\n')
out.append('''fn contains(ranges: &[(u32, u32)], c: char) -> bool {
    let code = c as u32;
    let index = ranges.partition_point(|(_, end)| *end < code);
    ranges.get(index).is_some_and(|(start, _)| *start <= code)
}
pub(crate) fn is_letter(c: char) -> bool { contains(LETTER, c) }
pub(crate) fn is_number(c: char) -> bool { contains(NUMBER, c) }
pub(crate) fn is_punctuation(c: char) -> bool { contains(PUNCTUATION, c) }
pub(crate) fn is_nonspacing_mark(c: char) -> bool { contains(NONSPACING_MARK, c) }
''')
path = Path(__file__).resolve().parents[1] / 'src/unicode_categories.rs'
path.write_text(''.join(out))
print(f'Wrote {path} with Unicode {u.unidata_version}')
