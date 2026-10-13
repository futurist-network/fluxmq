import { describe, expect, it } from 'vitest';
import { decodePayload } from './payload';
import { contentTypeOf, detectFormat, parseCsv, parseForm, prettyXml, tokenizeXml } from './payloadFormat';

const fmt = (s: string, ct?: string) => detectFormat(decodePayload(s), ct);

describe('payload formats', () => {
  it('sniffs JSON, XML, HTML, form data, CSV and text', () => {
    expect(fmt('{"a":1}')).toBe('json');
    expect(fmt('<?xml version="1.0"?><order id="1"><sku>A</sku></order>')).toBe('xml');
    expect(fmt('<order><sku>A</sku></order>')).toBe('xml');
    expect(fmt('<soap:Envelope xmlns:soap="x"><soap:Body/></soap:Envelope>')).toBe('xml');
    expect(fmt('<!DOCTYPE html><html><body>hi</body></html>')).toBe('html');
    expect(fmt('order_id=8842&amount=12.5&country=DE')).toBe('form');
    expect(fmt('id,sku,qty\n1,A,2\n2,B,1')).toBe('csv');
    expect(fmt('Order 8842 shipped')).toBe('text');
    expect(fmt('a < b and c > d')).toBe('text');
    expect(detectFormat(decodePayload({ encoding: 'base64', data: btoa(String.fromCharCode(0, 1, 2, 3, 255)) }))).toBe('binary');
  });

  it('a known content-type header wins over sniffing', () => {
    expect(fmt('a=1', 'application/x-www-form-urlencoded')).toBe('form');
    expect(fmt('<x/>', 'text/plain; charset=utf-8')).toBe('text');
    expect(fmt('{"a":1}', 'application/vnd.api+json')).toBe('json');
    expect(fmt('not json', 'application/json')).toBe('text');
    expect(contentTypeOf({ 'Content-Type': 'text/xml' })).toBe('text/xml');
  });

  it('tokenizes markup without losing a character', () => {
    const src = '<?xml version="1.0"?><!-- c --><a x="1" y=\'2\'><b>t &amp; u</b><c/><![CDATA[<raw>]]></a>';
    const toks = tokenizeXml(src);
    expect(toks.map((t) => t.text).join('')).toBe(src);
    expect(toks.filter((t) => t.kind === 'attr').map((t) => t.text)).toEqual(['x', 'y']);
    expect(toks.find((t) => t.kind === 'comment')?.text).toBe('<!-- c -->');
  });

  it('indents one-line XML, keeps text elements on one line', () => {
    expect(prettyXml('<?xml version="1.0"?><order id="7"><sku>A</sku><qty>2</qty><gift/><note></note></order>')).toBe(
      ['<?xml version="1.0"?>', '<order id="7">', '  <sku>A</sku>', '  <qty>2</qty>', '  <gift/>', '  <note></note>', '</order>'].join('\n'),
    );
    expect(prettyXml('<a><b></a>')).toBe('<a><b></a>'); // unbalanced → unchanged
    expect(prettyXml('<p>hi<br>there</p>', true)).toContain('<br>');
  });

  it('parses form fields and CSV with quotes', () => {
    expect(parseForm('a=1&name=J%C3%BCrgen+M&empty=&flag')).toEqual([
      ['a', '1'],
      ['name', 'Jürgen M'],
      ['empty', ''],
      ['flag', ''],
    ]);
    expect(parseCsv('id;name\n1;"a;b"\n2;"say ""hi"""')).toEqual([
      ['id', 'name'],
      ['1', 'a;b'],
      ['2', 'say "hi"'],
    ]);
  });
});
